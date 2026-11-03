"""SpatialBench Q1-Q12: geofunctions on Spark vs DuckDB spatial, same Parquet, warm medians.

usage: python bench.py <spark|duckdb> <jar> <data_dir> <label> [runs] [queries=Q9,Q10] [spark.conf=value ...]
Prints one JSON line per query: {"engine","label","query","runs":[...],"median","result"}.
"""
import json, os, statistics, sys, time

engine, jar, data, label = sys.argv[1:5]
runs = int(sys.argv[5]) if len(sys.argv) > 5 else 5
extra = dict(a.split("=", 1) for a in sys.argv[6:])
only = set(extra.pop("queries").split(",")) if "queries" in extra else None
# Register only the tables that were downloaded; a query on a missing table fails on its own.
TABLES = [t for t in ("trip", "zone", "customer", "building") if os.path.isdir(os.path.join(data, t))]

from common import BOX, BOX6, CENTER, summarize

CELL = 10.0  # Q4 QR cell in degrees: best of 0.5-45 at SF1 and SF10 for a broadcast QR join
Q6_CELL = 1.0  # Q6 QR cell in degrees: 0.5-2 all within 3% at SF1
Q8_CELL = 0.05  # Q8 QR cell in degrees: best of 0.005-0.2 at SF1 and SF10
Q9_CELL = 0.05  # Q9 QR cell in degrees: best of 0.005-0.05 at SF1
Q10_CELL = 0.5  # Q10 QR cell in degrees: 0.5-2 within 15% at SF1
Q11_CELL = 0.5  # Q11 uses the Q10 plan for both ends of each trip

GF = {
    "scan": "SELECT count(*) FROM trip WHERE length(t_pickuploc) > 0",
    "Q1": f"""
SELECT t.t_tripkey, ST_X(t.t_pickuploc, 0) AS pickup_lon, ST_Y(t.t_pickuploc, 0) AS pickup_lat, t.t_pickuptime,
       ST_DISTANCE(t.t_pickuploc, ST_FROMTEXT('{CENTER}')) AS distance_to_center
FROM trip t
WHERE ST_DWITHIN(t.t_pickuploc, ST_FROMTEXT('{CENTER}'), 0.45)
ORDER BY distance_to_center ASC, t.t_tripkey ASC LIMIT 100""",
    "Q2": """
SELECT COUNT(*) FROM trip t
WHERE ST_INTERSECTS(t.t_pickuploc,
  (SELECT z.z_boundary FROM zone z WHERE z.z_name = 'Coconino County' LIMIT 1), '-1')""",
    "Q3": f"""
SELECT date_trunc('month', t.t_pickuptime) AS pickup_month, count(t.t_tripkey) AS total_trips,
       avg(t.t_distance) AS avg_distance, avg(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
       avg(t.t_fare) AS avg_fare
FROM trip t
WHERE ST_DWITHIN(t.t_pickuploc, ST_FROMTEXT('{BOX}'), 0.045)
GROUP BY pickup_month ORDER BY pickup_month""",
    # QR join: explode both sides into CELL cells, equi-join on the cell with the 1000 points
    # broadcast, keep each pair once (QR_INTERSECT), then the exact test.
    "Q4": f"""
WITH top AS (SELECT t_pickuploc AS g FROM trip ORDER BY t_tip DESC, t_tripkey ASC LIMIT 1000),
tq AS (SELECT g, explode(QR_ENVP(g, {CELL}, 0.0)) AS qr FROM top),
zq AS (SELECT z_zonekey, z_name, z_boundary, explode(QR_ENVP(z_boundary, {CELL}, 0.0)) AS qr FROM zone)
SELECT /*+ BROADCAST(tq) */ zq.z_zonekey, zq.z_name, count(*) AS trip_count
FROM tq JOIN zq ON tq.qr.qr = zq.qr.qr
WHERE QR_INTERSECT(tq.qr, zq.qr, {CELL}) AND ST_WITHIN(tq.g, zq.z_boundary)
GROUP BY zq.z_zonekey, zq.z_name ORDER BY trip_count DESC, zq.z_zonekey ASC""",
    # Same result as the one-statement GROUP BY ... HAVING, about 15% faster: count first, then
    # collect dropoffs only for the qualifying customer-months (broadcast semi-join).
    "Q5": """
WITH t AS (SELECT t_custkey, date_trunc('month', t_pickuptime) AS pickup_month, t_dropoffloc FROM trip),
k AS (SELECT t_custkey, pickup_month FROM t GROUP BY t_custkey, pickup_month HAVING count(*) > 5),
g AS (SELECT /*+ BROADCAST(k) */ t.t_custkey, t.pickup_month,
             ST_AREA(ST_CONVEXHULL(ST_MULTIPOINT(collect_list(t.t_dropoffloc)))) AS monthly_travel_hull_area,
             count(*) AS dropoff_count
      FROM t LEFT SEMI JOIN k ON t.t_custkey = k.t_custkey AND t.pickup_month = k.pickup_month
      GROUP BY t.t_custkey, t.pickup_month)
SELECT c.c_custkey, c.c_name AS customer_name, g.pickup_month, g.monthly_travel_hull_area, g.dropoff_count
FROM g JOIN customer c ON g.t_custkey = c.c_custkey
ORDER BY g.monthly_travel_hull_area DESC, c.c_custkey ASC, g.pickup_month ASC LIMIT 100""",
    # QR join for point-in-polygon: a point sits in exactly one cell (QR_FROMGEOM), so each
    # (point, zone) pair meets once and needs no QR_INTERSECT dedup.
    "Q6": f"""
WITH zq AS (SELECT z_zonekey, z_name, z_boundary, explode(QR_LIST(z_boundary, {Q6_CELL}, 0.0)) AS qr
            FROM zone WHERE ST_INTERSECTS(ST_FROMTEXT('{BOX6}'), z_boundary))
SELECT /*+ BROADCAST(zq) */ zq.z_zonekey, zq.z_name, count(t.t_tripkey) AS total_pickups,
       avg(t.t_distance) AS avg_distance, avg(t.t_dropofftime - t.t_pickuptime) AS avg_duration
FROM trip t JOIN zq ON QR_FROMGEOM(t.t_pickuploc, {Q6_CELL}) = zq.qr
WHERE ST_WITHIN(t.t_pickuploc, zq.z_boundary)
GROUP BY zq.z_zonekey, zq.z_name ORDER BY total_pickups DESC, zq.z_zonekey ASC""",
    # The length of a two-point line is the distance between its points: no line geometry needed
    # (ST_LENGTH(ST_LINE(...)) is 3x slower at SF1).
    "Q7": """
WITH trip_lengths AS (
  SELECT t_tripkey, t_distance AS reported_distance_m,
         ST_DISTANCE(t_pickuploc, t_dropoffloc) / 0.000009 AS line_distance_m FROM trip)
SELECT t_tripkey, reported_distance_m, line_distance_m,
       reported_distance_m / NULLIF(line_distance_m, 0) AS detour_ratio
FROM trip_lengths
ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC LIMIT 100""",
    # QR distance join: each building is listed in every cell its envelope grown by 0.0045 touches,
    # each pickup in its one cell, so every (pickup, building) pair within 0.0045 meets exactly once.
    "Q8": f"""
WITH bq AS (SELECT b_buildingkey, b_name, b_boundary, explode(QR_LIST(b_boundary, {Q8_CELL}, 0.0045)) AS qr
            FROM building)
SELECT /*+ BROADCAST(bq) */ bq.b_buildingkey, bq.b_name, count(*) AS nearby_pickup_count
FROM trip t JOIN bq ON QR_FROMGEOM(t.t_pickuploc, {Q8_CELL}) = bq.qr
WHERE ST_DWITHIN(t.t_pickuploc, bq.b_boundary, 0.0045)
GROUP BY bq.b_buildingkey, bq.b_name ORDER BY nearby_pickup_count DESC, bq.b_buildingkey ASC LIMIT 100""",
    # QR self-join: both sides exploded into cells, each pair kept once (QR_INTERSECT, id1 < id2).
    "Q9": f"""
WITH b AS (SELECT b_buildingkey AS id, b_boundary AS geom, explode(QR_ENVP(b_boundary, {Q9_CELL}, 0.0)) AS qr FROM building),
pairs AS (
  SELECT /*+ BROADCAST(b2) */ b1.id AS building_1, b2.id AS building_2,
         ST_AREA(b1.geom) AS area1, ST_AREA(b2.geom) AS area2,
         ST_AREA(ST_INTERSECTION(b1.geom, b2.geom, '-1')) AS overlap_area
  FROM b b1 JOIN b b2 ON b1.qr.qr = b2.qr.qr
  WHERE b1.id < b2.id AND QR_INTERSECT(b1.qr, b2.qr, {Q9_CELL}) AND ST_INTERSECTS(b1.geom, b2.geom))
SELECT building_1, building_2, area1, area2, overlap_area,
       CASE WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0 ELSE overlap_area / (area1 + area2 - overlap_area) END AS iou
FROM pairs ORDER BY iou DESC, building_1 ASC, building_2 ASC LIMIT 100""",
    # 1) broadcast zone cells + envelopes (no geometry): candidate (zone, trip) pairs;
    # 2) one row per zone with its candidates: the boundary is copied, parsed and accelerated
    #    once (ST_WITHIN_MASK) instead of once per pair (~1 TB of copies at SF1);
    # 3) explode the hits, aggregate, LEFT JOIN so zones without trips stay.
    "Q10": f"""
WITH ze AS (SELECT z_zonekey, explode(QR_ENVP(z_boundary, {Q10_CELL}, 0.0)) AS qr FROM zone),
t AS (SELECT t_pickuploc, t_distance, t_dropofftime - t_pickuptime AS dur,
             ST_X(t_pickuploc, 0) AS x, ST_Y(t_pickuploc, 0) AS y FROM trip),
cand AS (SELECT /*+ BROADCAST(ze) */ ze.z_zonekey, struct(t.t_pickuploc AS loc, t.dur, t.t_distance AS dist) AS p
         FROM t JOIN ze ON QR_FROMGEOM(t.t_pickuploc, {Q10_CELL}) = ze.qr.qr
         WHERE t.x BETWEEN ze.qr.xmin AND ze.qr.xmax AND t.y BETWEEN ze.qr.ymin AND ze.qr.ymax),
g AS (SELECT z_zonekey, collect_list(p) AS ps FROM cand GROUP BY z_zonekey),
m AS (SELECT g.z_zonekey, g.ps, ST_WITHIN_MASK(transform(g.ps, x -> x.loc), z.z_boundary, '-1') AS mask
      FROM g JOIN zone z ON g.z_zonekey = z.z_zonekey),
hit AS (SELECT z_zonekey, e.ps.dur AS dur, e.ps.dist AS dist FROM m LATERAL VIEW explode(arrays_zip(ps, mask)) AS e WHERE e.mask),
agg AS (SELECT z_zonekey, avg(dur) AS avg_duration, avg(dist) AS avg_distance, count(*) AS num_trips FROM hit GROUP BY z_zonekey)
SELECT z.z_zonekey, z.z_name AS pickup_zone, agg.avg_duration, agg.avg_distance, coalesce(agg.num_trips, 0) AS num_trips
FROM zone z LEFT JOIN agg ON z.z_zonekey = agg.z_zonekey
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC LIMIT 100""",
    # Q10's plan for both ends of each trip: candidates from broadcast zone envelopes, one
    # ST_WITHIN_MASK per zone, then (trip, end, zone) hits. The SpatialBench SQL counts every
    # (trip, pickup zone, dropoff zone) with different zones, so per trip that is
    # pickup_zones * dropoff_zones - zones holding both ends.
    "Q11": f"""
WITH ze AS (SELECT z_zonekey, explode(QR_ENVP(z_boundary, {Q11_CELL}, 0.0)) AS qr FROM zone),
pts AS (SELECT t_tripkey AS k, 0 AS e, t_pickuploc AS loc FROM trip
        UNION ALL SELECT t_tripkey AS k, 1 AS e, t_dropoffloc AS loc FROM trip),
p AS (SELECT k, e, loc, ST_X(loc, 0) AS x, ST_Y(loc, 0) AS y FROM pts),
cand AS (SELECT /*+ BROADCAST(ze) */ ze.z_zonekey, struct(p.k, p.e, p.loc) AS v
         FROM p JOIN ze ON QR_FROMGEOM(p.loc, {Q11_CELL}) = ze.qr.qr
         WHERE p.x BETWEEN ze.qr.xmin AND ze.qr.xmax AND p.y BETWEEN ze.qr.ymin AND ze.qr.ymax),
g AS (SELECT z_zonekey, collect_list(v) AS vs FROM cand GROUP BY z_zonekey),
m AS (SELECT g.z_zonekey, g.vs, ST_WITHIN_MASK(transform(g.vs, x -> x.loc), z.z_boundary, '-1') AS mask
      FROM g JOIN zone z ON g.z_zonekey = z.z_zonekey),
hit AS (SELECT z_zonekey, h.vs.k AS k, h.vs.e AS e FROM m LATERAL VIEW explode(arrays_zip(vs, mask)) AS h WHERE h.mask),
per_trip AS (SELECT k, sum(1 - e) AS np, sum(e) AS nd FROM hit GROUP BY k),
same_zone AS (SELECT k FROM hit GROUP BY k, z_zonekey HAVING count(DISTINCT e) = 2)
SELECT (SELECT sum(np * nd) FROM per_trip) - (SELECT count(*) FROM same_zone) AS cross_zone_trip_count""",
    # k-nearest-neighbour join (knn_join): buildings broadcast into an R-tree per task, exact
    # distances. A DataFrame plan, not SQL: see q12_spark below.
    "Q12": "dataframe",
}

DUCK = {
    "scan": "SELECT count(*) FROM trip WHERE octet_length(t_pickuploc) > 0",
    "Q1": f"""
SELECT t.t_tripkey, ST_X(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lon, ST_Y(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lat,
       t.t_pickuptime, ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromText('{CENTER}')) AS distance_to_center
FROM trip t
WHERE ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromText('{CENTER}'), 0.45)
ORDER BY distance_to_center ASC, t.t_tripkey ASC LIMIT 100""",
    "Q2": """
SELECT COUNT(*) FROM trip t
WHERE ST_Intersects(ST_GeomFromWKB(t.t_pickuploc),
  (SELECT ST_GeomFromWKB(z.z_boundary) FROM zone z WHERE z.z_name = 'Coconino County' LIMIT 1))""",
    "Q3": f"""
SELECT DATE_TRUNC('month', t.t_pickuptime) AS pickup_month, COUNT(t.t_tripkey) AS total_trips,
       AVG(t.t_distance) AS avg_distance, AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
       AVG(t.t_fare) AS avg_fare
FROM trip t
WHERE ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromText('{BOX}'), 0.045)
GROUP BY pickup_month ORDER BY pickup_month""",
    "Q4": """
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM zone z JOIN (SELECT t.t_pickuploc FROM trip t ORDER BY t.t_tip DESC, t.t_tripkey ASC LIMIT 1000) top_trips
  ON ST_Within(ST_GeomFromWKB(top_trips.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name ORDER BY trip_count DESC, z.z_zonekey ASC""",
    "Q5": """
SELECT c.c_custkey, c.c_name AS customer_name, DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
       ST_Area(ST_ConvexHull(ST_Collect(LIST(ST_GeomFromWKB(t.t_dropoffloc))))) AS monthly_travel_hull_area,
       COUNT(*) AS dropoff_count
FROM trip t JOIN customer c ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month HAVING dropoff_count > 5
ORDER BY monthly_travel_hull_area DESC, c.c_custkey ASC, pickup_month ASC LIMIT 100""",
    "Q6": f"""
SELECT z.z_zonekey, z.z_name, COUNT(t.t_tripkey) AS total_pickups, AVG(t.t_distance) AS avg_distance,
       AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration
FROM trip t, zone z
WHERE ST_Intersects(ST_GeomFromText('{BOX6}'), ST_GeomFromWKB(z.z_boundary))
  AND ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name ORDER BY total_pickups DESC, z.z_zonekey ASC""",
    "Q7": """
WITH trip_lengths AS (
  SELECT t.t_tripkey, t.t_distance AS reported_distance_m,
         ST_Length(ST_MakeLine(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(t.t_dropoffloc))) / 0.000009 AS line_distance_m
  FROM trip t)
SELECT t.t_tripkey, t.reported_distance_m, t.line_distance_m,
       t.reported_distance_m / NULLIF(t.line_distance_m, 0) AS detour_ratio
FROM trip_lengths t
ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC LIMIT 100""",
    "Q8": """
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t JOIN building b
  ON ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary), 0.0045)
GROUP BY b.b_buildingkey, b.b_name ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC LIMIT 100""",
    "Q9": """
WITH b1 AS (SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom FROM building),
b2 AS (SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom FROM building),
pairs AS (SELECT b1.id AS building_1, b2.id AS building_2, ST_Area(b1.geom) AS area1, ST_Area(b2.geom) AS area2,
                 ST_Area(ST_Intersection(b1.geom, b2.geom)) AS overlap_area
          FROM b1 JOIN b2 ON b1.id < b2.id AND ST_Intersects(b1.geom, b2.geom))
SELECT building_1, building_2, area1, area2, overlap_area,
       CASE WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0 ELSE overlap_area / (area1 + area2 - overlap_area) END AS iou
FROM pairs ORDER BY iou DESC, building_1 ASC, building_2 ASC LIMIT 100""",
    "Q10": """
SELECT z.z_zonekey, z.z_name AS pickup_zone, AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
       AVG(t.t_distance) AS avg_distance, COUNT(t.t_tripkey) AS num_trips
FROM zone z LEFT JOIN trip t ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC LIMIT 100""",
    "Q11": """
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
JOIN zone pickup_zone ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(pickup_zone.z_boundary))
JOIN zone dropoff_zone ON ST_Within(ST_GeomFromWKB(t.t_dropoffloc), ST_GeomFromWKB(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey""",
    # DuckDB 2.0 spatial has no KNN join: brute force compares every pickup with every building.
    "Q12": """
SELECT t_tripkey, AVG(d) AS avg_distance_to_5_nearest FROM (
  SELECT t.t_tripkey, ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary)) AS d,
         row_number() OVER (PARTITION BY t.t_tripkey ORDER BY ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary))) AS rn
  FROM trip t CROSS JOIN building b) WHERE rn <= 5
GROUP BY t_tripkey ORDER BY avg_distance_to_5_nearest DESC, t_tripkey ASC LIMIT 100""",
}

if engine == "spark":
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-class-path {jar} --jars {jar} pyspark-shell"
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.master(extra.pop("master", "local[*]"))
         .config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.host", "127.0.0.1")
         .config("spark.driver.memory", "64g").config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false"))  # progress bars glue onto the JSON lines
    for k, v in extra.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    from geofunctions import st_register_functions
    st_register_functions()
    for table in TABLES:
        spark.read.parquet(f"{data}/{table}").createOrReplaceTempView(table)
    from pyspark.sql import functions as F
    import geofunctions as gf

    def q12_spark():
        trips = spark.table("trip").select("t_tripkey", "t_pickuploc")
        buildings = spark.table("building").select("b_boundary")
        return (gf.knn_join(trips, buildings, 5, lhs_geom="t_pickuploc", rhs_geom="b_boundary")
                .groupBy("t_tripkey").agg(F.avg("distance").alias("avg_distance_to_5_nearest"))
                .orderBy(F.desc("avg_distance_to_5_nearest"), "t_tripkey").limit(100).collect())

    queries = GF

    def execute(q):
        rows = q12_spark() if q == "dataframe" else spark.sql(q).collect()
        return [tuple(r) for r in rows]
    version = spark.version
else:
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    lit = lambda path: "'" + path.replace("'", "''") + "'"  # SQL string literal; paths may hold quotes
    for table in TABLES:
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM {lit(f'{data}/{table}/*.parquet')}")
    queries, execute = DUCK, lambda q: con.execute(q).fetchall()
    version = duckdb.__version__

for name, sql in queries.items():
    if only is not None and name not in only:
        continue
    t = time.perf_counter(); result = execute(sql); first = time.perf_counter() - t  # cold-ish: JIT/plan cache
    times = []
    for _ in range(runs):
        t = time.perf_counter(); execute(sql); times.append(time.perf_counter() - t)
    summary = summarize(name, result)
    print(json.dumps({"engine": engine, "version": version, "label": label, "query": name, "first": round(first, 3),
                      "runs": [round(x, 3) for x in times], "median": round(statistics.median(times), 3),
                      "result": summary}), flush=True)
