"""SpatialBench Q1-Q4: geofunctions on Spark vs DuckDB spatial, same Parquet, warm medians.

usage: python bench.py <spark|duckdb> <jar> <data_dir> <label> [runs] [spark.conf=value ...]
Prints one JSON line per query: {"engine","label","query","runs":[...],"median","result"}.
"""
import json, os, statistics, sys, time

engine, jar, data, label = sys.argv[1:5]
runs = int(sys.argv[5]) if len(sys.argv) > 5 else 5
extra = dict(a.split("=", 1) for a in sys.argv[6:])

CENTER = "POINT (-111.7610 34.8697)"
BOX = ("POLYGON((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, "
       "-111.9060 35.0047, -111.9060 34.7347))")
CELL = 10.0  # Q4 QR cell in degrees: best of 0.5-45 at SF1 and SF10 for a broadcast QR join

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
}

if engine == "spark":
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-class-path {jar} --jars {jar} pyspark-shell"
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.master(extra.pop("master", "local[*]"))
         .config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.host", "127.0.0.1")
         .config("spark.driver.memory", "64g").config("spark.ui.enabled", "false"))
    for k, v in extra.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    from geofunctions import st_register_functions
    st_register_functions()
    spark.read.parquet(f"{data}/trip").createOrReplaceTempView("trip")
    spark.read.parquet(f"{data}/zone").createOrReplaceTempView("zone")
    queries, execute = GF, lambda q: [tuple(r) for r in spark.sql(q).collect()]
    version = spark.version
else:
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    lit = lambda path: "'" + path.replace("'", "''") + "'"  # SQL string literal; paths may hold quotes
    con.execute(f"CREATE VIEW trip AS SELECT * FROM {lit(f'{data}/trip/*.parquet')}")
    con.execute(f"CREATE VIEW zone AS SELECT * FROM {lit(f'{data}/zone/*.parquet')}")
    queries, execute = DUCK, lambda q: con.execute(q).fetchall()
    version = duckdb.__version__

def q3_summary(rows):
    """Month, count and all three averages, normalized so both engines compare equal."""
    return [(str(m)[:7], n, round(float(d), 9), round(dur.total_seconds(), 3), round(float(f), 9))
            for m, n, d, dur, f in rows]


for name, sql in queries.items():
    t = time.perf_counter(); result = execute(sql); first = time.perf_counter() - t  # cold-ish: JIT/plan cache
    times = []
    for _ in range(runs):
        t = time.perf_counter(); execute(sql); times.append(time.perf_counter() - t)
    summary = {"Q1": lambda r: [x[0] for x in r], "Q3": q3_summary,
               "Q4": lambda r: [(x[0], x[2]) for x in r]}.get(name, lambda r: r[0][0])(result)
    print(json.dumps({"engine": engine, "version": version, "label": label, "query": name, "first": round(first, 3),
                      "runs": [round(x, 3) for x in times], "median": round(statistics.median(times), 3),
                      "result": summary}), flush=True)
