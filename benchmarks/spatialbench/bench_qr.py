"""SpatialBench Q1-Q4 and Q6 on a QR-prepared layout (prepare.py): geofunctions on Spark vs DuckDB.

usage: python bench_qr.py <spark|duckdb> <jar> <prepared_dir> <cell> <label> [runs]

Both engines run the same plan. Q1-Q3 keep only the QR cells the query area can reach, so
Parquet skips every other row group. Q2 and Q4 test points against the per-cell zone pieces,
joined on the QR cell. Q2 runs in two phases: phase 1 reads the county's zone key and cells,
phase 2 prunes with them. Q4 does not prune: the top-1000 pickups are spread over the whole
world, and a list of ~700 cells costs more to evaluate than reading every piece. Q6 runs in
three phases: the zones with a piece in the box's cells that intersects the box, then those
zones' cells, then the trips in those cells joined to the pieces. Q5, Q7 and Q8 are only in
bench.py: Q5 groups by customer and Q7 has no spatial filter, so neither can prune by cell, and
Q8's buildings sit in 45% of the 1-degree cells that hold trips, so pruning skips little.
"""
import json, math, os, statistics, sys, time

engine, jar, data, cell, label = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4]), sys.argv[5]
runs = int(sys.argv[6]) if len(sys.argv) > 6 else 5

from common import BOX, BOX6, BOX6_ENV, BOX_ENV, CENTER, CENTER_XY, summarize


def cells(xmin, ymin, xmax, ymax, dist):
    """QR_LIST of an envelope; Spark asserts this equals qr_cells for the same geometry."""
    out = []
    for q in range(math.floor((xmin - dist) / cell), math.floor((xmax + dist) / cell) + 1):
        for r in range(math.floor((ymin - dist) / cell), math.floor((ymax + dist) / cell) + 1):
            v = ((q << 32) | (r & 0xFFFFFFFF)) & 0xFFFFFFFFFFFFFFFF
            out.append(v - (1 << 64) if v >= 1 << 63 else v)  # signed 64-bit, as Scala's Long
    return out


Q1_CELLS = cells(*CENTER_XY, *CENTER_XY, 0.45)
Q3_CELLS = cells(*BOX_ENV, 0.045)
# pieces are clipped to cells grown by cell*1e-6, so a piece one cell over can reach the box edge
Q6_BOX_CELLS = cells(*BOX6_ENV, cell * 1e-6)


def q6_keys(box, piece):
    return f"""SELECT DISTINCT z_zonekey FROM zone
WHERE qr IN ({in_list(Q6_BOX_CELLS)}) AND ST_INTERSECTS({box}, {piece})"""


def q6_cells(keys):
    return f"SELECT DISTINCT qr FROM zone WHERE z_zonekey IN ({in_list(keys)})"


def q6(keys, cell_list, point, piece):
    return f"""
SELECT /*+ BROADCAST(z) */ z.z_zonekey, z.z_name, count(t.t_tripkey) AS total_pickups,
       avg(t.t_distance) AS avg_distance, avg(t.t_dropofftime - t.t_pickuptime) AS avg_duration
FROM trip t JOIN zone z ON t.t_pickup_qr = z.qr
WHERE t.t_pickup_qr IN ({in_list(cell_list)}) AND z.qr IN ({in_list(cell_list)})
  AND z.z_zonekey IN ({in_list(keys)}) AND ST_WITHIN({point}, {piece})
GROUP BY z.z_zonekey, z.z_name ORDER BY total_pickups DESC, z.z_zonekey ASC"""


def in_list(values):
    return ",".join(str(v) for v in sorted(set(values))) or "NULL"


def q1(geom_from, xy):
    return f"""
SELECT t.t_tripkey, {xy}, t.t_pickuptime, ST_DISTANCE({geom_from}, {CENTER_G}) AS distance_to_center
FROM trip t
WHERE t.t_pickup_qr IN ({in_list(Q1_CELLS)}) AND ST_DWITHIN({geom_from}, {CENTER_G}, 0.45)
ORDER BY distance_to_center ASC, t.t_tripkey ASC LIMIT 100"""


def q3(geom_from):
    return f"""
SELECT date_trunc('month', t.t_pickuptime) AS pickup_month, count(t.t_tripkey) AS total_trips,
       avg(t.t_distance) AS avg_distance, avg(t.t_dropofftime - t.t_pickuptime) AS avg_duration, avg(t.t_fare) AS avg_fare
FROM trip t
WHERE t.t_pickup_qr IN ({in_list(Q3_CELLS)}) AND ST_DWITHIN({geom_from}, {BOX_G}, 0.045)
GROUP BY pickup_month ORDER BY pickup_month"""


Q2_CELLS_SQL = "SELECT z_zonekey, qr FROM zone WHERE z_name = 'Coconino County'"


def q2(rows, point, piece):
    key = min(r[0] for r in rows)  # SpatialBench's LIMIT 1, made deterministic
    cell_list = in_list(r[1] for r in rows if r[0] == key)
    return f"""
SELECT count(*) FROM trip t
JOIN (SELECT qr, z_piece FROM zone WHERE z_zonekey = {key} AND qr IN ({cell_list})) c
  ON t.t_pickup_qr = c.qr
WHERE t.t_pickup_qr IN ({cell_list}) AND ST_INTERSECTS({point}, {piece})"""


TOP_SQL = "SELECT t_pickuploc, t_pickup_qr FROM trip ORDER BY t_tip DESC, t_tripkey ASC LIMIT 1000"
# the 1000 points with their coordinates, for the envelope test against each piece
TOP_XY = "SELECT t_pickuploc, t_pickup_qr, {x} AS x, {y} AS y FROM ({top})"


def q4(point, piece, x, y):
    return f"""
SELECT /*+ BROADCAST(t) */ z.z_zonekey, z.z_name, count(*) AS trip_count
FROM top t JOIN zone z ON t.t_pickup_qr = z.qr
WHERE t.x BETWEEN z.z_xmin AND z.z_xmax AND t.y BETWEEN z.z_ymin AND z.z_ymax
  AND ST_WITHIN({point}, {piece})
GROUP BY z.z_zonekey, z.z_name ORDER BY trip_count DESC, z.z_zonekey ASC"""


if engine == "spark":
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-class-path {jar} --jars {jar} pyspark-shell"
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[*]")
             .config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.memory", "64g").config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             # push every cell to Parquet; above the threshold Spark pushes only min..max of the list
             .config("spark.sql.parquet.pushdown.inFilterThreshold", "1000000").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    import geofunctions as gf
    gf.st_register_functions()
    assert gf.qr_cells(CENTER, cell, 0.45) == Q1_CELLS and gf.qr_cells(BOX, cell, 0.045) == Q3_CELLS
    spark.read.parquet(f"{data}/trip").createOrReplaceTempView("trip")
    spark.read.parquet(f"{data}/zone").createOrReplaceTempView("zone")
    CENTER_G, BOX_G = f"ST_FROMTEXT('{CENTER}')", f"ST_FROMTEXT('{BOX}')"
    BOX_G6 = f"ST_FROMTEXT('{BOX6}')"
    assert gf.qr_cells(BOX6, cell, cell * 1e-6) == Q6_BOX_CELLS
    sql = lambda q: [tuple(r) for r in spark.sql(q).collect()]
    version = spark.version

    def run_q2():
        return sql(q2(sql(Q2_CELLS_SQL), "t.t_pickuploc", "c.z_piece"))

    def run_q4():
        top = spark.sql(TOP_XY.format(x="ST_X(t_pickuploc, 0)", y="ST_Y(t_pickuploc, 0)", top=TOP_SQL)).collect()
        spark.createDataFrame(top, "t_pickuploc binary, t_pickup_qr long, x double, y double").createOrReplaceTempView("top")
        return sql(q4("t.t_pickuploc", "z.z_piece", "t.x", "t.y"))

    def run_q6():
        keys = [r[0] for r in sql(q6_keys(BOX_G6, "z_piece"))]
        cell_list = [r[0] for r in sql(q6_cells(keys))]
        return sql(q6(keys, cell_list, "t.t_pickuploc", "z.z_piece"))

    queries = {
        "Q1": lambda: sql(q1("t.t_pickuploc", "ST_X(t.t_pickuploc, 0) AS pickup_lon, ST_Y(t.t_pickuploc, 0) AS pickup_lat")),
        "Q2": run_q2,
        "Q3": lambda: sql(q3("t.t_pickuploc")),
        "Q4": run_q4,
        "Q6": run_q6,
    }
else:
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    lit = lambda path: "'" + path.replace("'", "''") + "'"
    con.execute(f"CREATE VIEW trip AS SELECT * FROM {lit(f'{data}/trip/*.parquet')}")
    con.execute(f"CREATE VIEW zone AS SELECT * FROM {lit(f'{data}/zone/*.parquet')}")
    CENTER_G, BOX_G = f"ST_GeomFromText('{CENTER}')", f"ST_GeomFromText('{BOX}')"
    BOX_G6 = f"ST_GeomFromText('{BOX6}')"
    sql = lambda q: con.execute(q).fetchall()
    version = duckdb.__version__
    P, Z = "ST_GeomFromWKB(t.t_pickuploc)", "ST_GeomFromWKB({}.z_piece)"

    def run_q2():
        return sql(q2(sql(Q2_CELLS_SQL), P, Z.format("c")))

    def run_q4():
        g = "ST_GeomFromWKB(t_pickuploc)"
        con.execute(f"CREATE OR REPLACE TEMP TABLE top AS {TOP_XY.format(x=f'ST_X({g})', y=f'ST_Y({g})', top=TOP_SQL)}")
        return sql(q4(P, Z.format("z"), "t.x", "t.y").replace("/*+ BROADCAST(t) */ ", ""))

    def run_q6():
        keys = [r[0] for r in sql(q6_keys(BOX_G6, "ST_GeomFromWKB(z_piece)"))]
        cell_list = [r[0] for r in sql(q6_cells(keys))]
        return sql(q6(keys, cell_list, P, Z.format("z"))
                   .replace("/*+ BROADCAST(z) */ ", ""))

    queries = {
        "Q1": lambda: sql(q1(P, f"ST_X({P}) AS pickup_lon, ST_Y({P}) AS pickup_lat").replace("ST_DWITHIN", "ST_DWithin").replace("ST_DISTANCE", "ST_Distance")),
        "Q2": run_q2,
        "Q3": lambda: sql(q3(P)),
        "Q4": run_q4,
        "Q6": run_q6,
    }

for name, run in queries.items():
    t = time.perf_counter(); result = run(); first = time.perf_counter() - t
    times = []
    for _ in range(runs):
        t = time.perf_counter(); run(); times.append(time.perf_counter() - t)
    summary = summarize(name, result)
    print(json.dumps({"engine": engine, "version": version, "label": label, "cell": cell, "query": name,
                      "first": round(first, 3), "runs": [round(x, 3) for x in times],
                      "median": round(statistics.median(times), 3), "result": summary}), flush=True)
