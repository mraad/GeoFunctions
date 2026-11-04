"""ArcGIS Pro check: start Spark the way spark_esri does, then run basic functions and the readers.
Prints one PASS/FAIL line per check. Run it through run.ps1.
Usage: python pro_smoke.py <bundle folder> <jar name> <work folder (unused)>"""
import os
import re
import sys
import traceback

base, jar_name = sys.argv[1:3]
jar = os.path.join(base, jar_name)
gdb = os.path.join(base, "Shapes.gdb").replace("\\", "/")
shp = os.path.join(base, "gps.shp").replace("\\", "/")
fails = []


def check(name, fn):
    try:
        print(f"PASS {name}: {fn()}", flush=True)
    except Exception as e:
        fails.append(name)
        lines = [ln for ln in str(e).splitlines() if ln.strip()]
        key = next((ln for ln in lines if re.search(r"Error|Exception", ln)), lines[0] if lines else repr(e))
        print(f"FAIL {name}: {type(e).__name__}: {key[:300]}", flush=True)
        traceback.print_exc(file=sys.stderr)


import spark_esri  # noqa: E402

spark = spark_esri.spark_start(config={
    "spark.jars": jar,
    "spark.driver.extraClassPath": jar,
    "spark.driver.memory": "4G",
    "spark.executor.memory": "4G",
})
print("spark", spark.version, "java", spark._jvm.java.lang.System.getProperty("java.version"), flush=True)
import pyspark  # noqa: E402
print("pyspark", pyspark.__version__, "python", sys.version.split()[0], "jar", jar_name, flush=True)

import geofunctions as S  # noqa: E402
import pyspark.sql.functions as F  # noqa: E402
print("geofunctions from", S.__file__, flush=True)


def wkt_df(*wkts):
    # SQL, not createDataFrame: a Python list needs a Python worker, which Spark 4.1.1 cannot run here.
    vals = ", ".join("'" + w + "'" for w in wkts)
    return spark.sql(f"SELECT explode(array({vals})) AS wkt").select(S.st_fromtext("wkt").alias("geom"))


# --- does the jar load and work on this Spark?
check("jvm class loads", lambda: spark._jvm.java.lang.Class.forName(
    "com.esri.spark.GeoFunctions$", True, spark._jvm.Thread.currentThread().getContextClassLoader()).getName())


def t_point():
    w = spark.range(1).select(S.st_astext(S.st_point(F.lit(1.0), F.lit(2.0))).alias("w")).first().w
    assert w == "POINT (1 2)", w
    return w


check("st_point + st_astext (codegen)", t_point)


def t_sql():
    S.st_register_functions()
    w = spark.sql("SELECT ST_ASTEXT(ST_POINT(3.0, 4.0)) AS w").first().w
    assert w == "POINT (3 4)", w
    return w


check("SQL registry", t_sql)


def t_buffer():
    a = wkt_df("POINT (0 0)").select(S.st_area(S.st_buffer("geom", 10.0, 64, -1)).alias("a")).first().a
    assert 310 < a < 315, a
    return round(a, 2)


check("st_buffer + st_area", t_buffer)


def t_predicate():
    df = spark.sql("SELECT 'POINT (5 5)' AS p, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))' AS z")
    df = df.select(S.st_fromtext("p").alias("p"), S.st_fromtext("z").alias("z"))
    r = df.select(S.st_within("p", "z", -1).alias("a"), S.st_dwithin("p", "z", 1.0).alias("b")).first()
    assert r.a and r.b, r
    return tuple(r)


check("st_within + st_dwithin", t_predicate)


def t_join():
    lhs = wkt_df("POINT (5 5)", "POINT (500 500)")
    rhs = wkt_df("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
    a = lhs.join_qr(rhs, 100.0, oper="within").count()
    b = lhs.join_qr(rhs, 100.0, oper="within", broadcast="rhs").count()
    assert (a, b) == (1, 1), (a, b)
    return (a, b)


check("join_qr shuffle + broadcast (encoder)", t_join)


def t_knn():
    lhs = wkt_df("POINT (0 0)").withColumnRenamed("geom", "l")
    rhs = wkt_df("POINT (1 0)", "POINT (2 0)", "POINT (9 0)").withColumnRenamed("geom", "r")
    d = sorted(x.distance for x in lhs.knn_join(rhs, 2, lhs_geom="l", rhs_geom="r").collect())
    assert d == [1.0, 2.0], d
    return d


check("knn_join", t_knn)


def t_dump():
    donut = "POLYGON ((10 10, 90 10, 90 90, 10 90, 10 10), (40 40, 40 60, 60 60, 60 40, 40 40))"
    a = [r.a for r in wkt_df(donut).select(S.st_dump_explode("geom").alias("g")).select(S.st_area("g").alias("a")).collect()]
    assert a == [6000.0], a
    return a


check("st_dump keeps a hole", t_dump)


def t_dissolve():
    df = wkt_df("POLYGON ((99950 1000, 100050 1000, 100050 1100, 99950 1100, 99950 1000))",
                "POLYGON ((100000 1050, 100100 1050, 100100 1150, 100000 1150, 100000 1050))")
    a = [round(r.a, 6) for r in df.pairwise_dissolve(cell=100000.0, dist=100.0, wkid=3857)
         .select(S.st_area("geom").alias("a")).collect()]
    assert a == [17500.0], a
    return a


check("pairwise_dissolve", t_dissolve)


def t_dissolve_axis():
    df = wkt_df("POLYGON ((-50 1000, 50 1000, 50 1100, -50 1100, -50 1000))")
    a = [round(r.a, 6) for r in df.pairwise_dissolve(cell=100000.0, dist=100.0, wkid=3857, breaker=3)
         .select(S.st_area("geom").alias("a")).collect()]
    assert a == [10000.0], a
    return a


check("pairwise_dissolve across x=0", t_dissolve_axis)

# --- the readers on this Spark
RINGS = [(0, 0), (0, 10), (10, 10), (10, 0), (2, 2), (4, 2), (4, 4), (2, 4), (20, 0), (20, 5), (25, 5)]
PATHS = [(0, 0), (5, 5), (10, 0), (20, 0), (25, 5), (30, 0), (20, 0)]
MPTS = [(1, 2), (3, 4), (5, 6)]
PT = [(5, 7)]


def expected(xy, z, m):
    return {tuple(float(v) for v in (x, y, *([100 + k] if z else []), *([1000 + k] if m else [])))
            for k, (x, y) in enumerate(xy)}


def read_layer(layer, fn):
    df = spark.read.format("gdb").option("path", gdb).option("name", layer).load()
    shape = next(c for c in df.columns if c.lower() == "shape")
    assert df.count() == 1
    return df.select(S.st_astext(fn(shape)).alias("w")).first().w


def check_layer(layer, fn, xy, z, m):
    def run():
        w = read_layer(layer, fn)
        tag = re.match(r"[A-Z]+( ZM| Z| M)?", w).group(1) or ""
        want_tag = " ZM" if z and m else " Z" if z else " M" if m else ""
        assert tag == want_tag, f"{w[:80]} wanted{want_tag!r}"
        got = {tuple(float(v) for v in p.split()) for p in re.findall(r"-?\d[^,()]*", w)}
        assert got == expected(xy, z, m), f"{sorted(got)[:4]} vs {sorted(expected(xy, z, m))[:4]}"
        return f"{len(got)} vertices{want_tag}"
    check(f"gdb {layer} {fn.__name__}", run)


KINDS = {"xy": (False, False), "xyz": (True, False), "xym": (False, True), "xyzm": (True, True)}
for kind, (z, m) in KINDS.items():
    check_layer(f"point_{kind}", S.gdb_point, PT, z, m)
for name, xy, f2, fz, fm in (("mpoint", MPTS, S.gdb_multipoint2, S.gdb_multipointZ, S.gdb_multipointM),
                             ("line", PATHS, S.gdb_polyline2, S.gdb_polylineZ, S.gdb_polylineM),
                             ("poly", RINGS, S.gdb_polygon2, S.gdb_polygonZ, S.gdb_polygonM)):
    for kind in KINDS:
        check_layer(f"{name}_{kind}", f2, xy, False, False)
    check_layer(f"{name}_xy", fz, xy, False, False)
    check_layer(f"{name}_xyz", fz, xy, True, False)
    check_layer(f"{name}_xyzm", fz, xy, True, False)
    check_layer(f"{name}_xy", fm, xy, False, False)
    check_layer(f"{name}_xym", fm, xy, False, True)
    check_layer(f"{name}_xyzm", fm, xy, True, True)


def t_shp():
    df = spark.read.format("shp").option("path", shp).load()
    n = df.count()
    bad = df.select(S.st_astext("shape").alias("w")).filter("w is null").count()
    assert n == 100000 and bad == 0, (n, bad)
    return f"{n} rows, columns {df.columns[:4]}"


if os.path.exists(shp):
    check("shp reader gps.shp", t_shp)
else:
    print("SKIP shp reader: no gps.shp in the bundle (run.ps1 -Shp)", flush=True)

print(f"\nSUMMARY failed={len(fails)} {fails}", flush=True)
spark_esri.spark_stop()
