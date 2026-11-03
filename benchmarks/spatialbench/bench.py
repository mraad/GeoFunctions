"""SpatialBench Q1/Q2: geofunctions on Spark vs DuckDB spatial, same Parquet, warm medians.

usage: python bench.py <spark|duckdb> <jar> <data_dir> <label> [runs] [spark.conf=value ...]
Prints one JSON line per query: {"engine","label","query","runs":[...],"median","result"}.
"""
import json, os, statistics, sys, time

engine, jar, data, label = sys.argv[1:5]
runs = int(sys.argv[5]) if len(sys.argv) > 5 else 5
extra = dict(a.split("=", 1) for a in sys.argv[6:])

CENTER = "POINT (-111.7610 34.8697)"

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

for name, sql in queries.items():
    t = time.perf_counter(); result = execute(sql); first = time.perf_counter() - t  # cold-ish: JIT/plan cache
    times = []
    for _ in range(runs):
        t = time.perf_counter(); execute(sql); times.append(time.perf_counter() - t)
    summary = result[0][0] if name != "Q1" else [r[0] for r in result]
    print(json.dumps({"engine": engine, "version": version, "label": label, "query": name, "first": round(first, 3),
                      "runs": [round(x, 3) for x in times], "median": round(statistics.median(times), 3),
                      "result": summary}), flush=True)
