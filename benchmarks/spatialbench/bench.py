"""SpatialBench Q1-Q8: geofunctions on Spark vs DuckDB spatial, same Parquet, warm medians.

usage: python bench.py <spark|duckdb> <jar> <data_dir> <label> [runs] [spark.conf=value ...]
Prints one JSON line per query: {"engine","label","query","runs":[...],"median","result"}.
"""
import json, os, statistics, sys, time

engine, jar, data, label = sys.argv[1:5]
runs = int(sys.argv[5]) if len(sys.argv) > 5 else 5
extra = dict(a.split("=", 1) for a in sys.argv[6:])

from common import BOX, BOX6, CENTER, summarize

CELL = 10.0  # Q4 QR cell in degrees: best of 0.5-45 at SF1 and SF10 for a broadcast QR join
Q6_CELL = 1.0  # Q6 QR cell in degrees: 0.5-2 all within 3% at SF1
Q8_CELL = 0.05  # Q8 QR cell in degrees: best of 0.005-0.2 at SF1 and SF10

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
    spark.read.parquet(f"{data}/trip").createOrReplaceTempView("trip")
    spark.read.parquet(f"{data}/zone").createOrReplaceTempView("zone")
    spark.read.parquet(f"{data}/customer").createOrReplaceTempView("customer")
    spark.read.parquet(f"{data}/building").createOrReplaceTempView("building")
    queries, execute = GF, lambda q: [tuple(r) for r in spark.sql(q).collect()]
    version = spark.version
else:
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    lit = lambda path: "'" + path.replace("'", "''") + "'"  # SQL string literal; paths may hold quotes
    con.execute(f"CREATE VIEW trip AS SELECT * FROM {lit(f'{data}/trip/*.parquet')}")
    con.execute(f"CREATE VIEW zone AS SELECT * FROM {lit(f'{data}/zone/*.parquet')}")
    con.execute(f"CREATE VIEW customer AS SELECT * FROM {lit(f'{data}/customer/*.parquet')}")
    con.execute(f"CREATE VIEW building AS SELECT * FROM {lit(f'{data}/building/*.parquet')}")
    queries, execute = DUCK, lambda q: con.execute(q).fetchall()
    version = duckdb.__version__

for name, sql in queries.items():
    t = time.perf_counter(); result = execute(sql); first = time.perf_counter() - t  # cold-ish: JIT/plan cache
    times = []
    for _ in range(runs):
        t = time.perf_counter(); execute(sql); times.append(time.perf_counter() - t)
    summary = summarize(name, result)
    print(json.dumps({"engine": engine, "version": version, "label": label, "query": name, "first": round(first, 3),
                      "runs": [round(x, 3) for x in times], "median": round(statistics.median(times), 3),
                      "result": summary}), flush=True)
