"""Test the arcpy helpers on ArcGIS Pro: write with to_feature_class/to_feature_table, check with
plain arcpy, read back with to_spark. Also runs the gdb_* decoders on layers ArcGIS writes itself.

PASS/FAIL: a stated expectation. NOTE: an edge case with no agreed answer; it reports what happens.
Everything is written under <work>, including ArcGIS's scratch geodatabase.
Run it through run.ps1. Usage: python pro_arcpy.py <bundle folder> <jar name> <work folder>"""
import os
import re
import sys
import traceback

base, jar_name, work = sys.argv[1:4]
jar = os.path.join(base, jar_name)
fails, notes = [], []
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def short(e):
    lines = [ln for ln in str(e).splitlines() if ln.strip()]
    key = next((ln for ln in lines if re.search(r"Error|Exception|ERROR", ln)), lines[0] if lines else repr(e))
    return f"{type(e).__name__}: {key[:260]}"


def check(name, fn):
    try:
        print(f"PASS {name}: {fn()}", flush=True)
    except Exception as e:
        fails.append(name)
        print(f"FAIL {name}: {short(e)}", flush=True)
        traceback.print_exc(file=sys.stderr)


def note(name, fn):
    try:
        print(f"NOTE {name}: ok, {fn()}", flush=True)
    except Exception as e:
        print(f"NOTE {name}: raised {short(e)}", flush=True)
        traceback.print_exc(file=sys.stderr)


import arcpy  # noqa: E402

print("arcpy", arcpy.GetInstallInfo()["Version"], arcpy.ProductInfo(), flush=True)
import spark_esri  # noqa: E402

spark = spark_esri.spark_start(config={
    "spark.jars": jar,
    "spark.driver.extraClassPath": jar,
    "spark.driver.memory": "4G",
    "spark.executor.memory": "4G",
})
import pyspark  # noqa: E402
print("spark", spark.version, "pyspark", pyspark.__version__, "python", sys.version.split()[0], flush=True)

import geofunctions as S  # noqa: E402
import make_shapes_gdb as M  # noqa: E402

S.st_register_functions()

os.makedirs(work, exist_ok=True)
arcpy.env.overwriteOutput = True
arcpy.env.scratchWorkspace = work
gdb = str(arcpy.management.CreateFileGDB(work, "helpers.gdb"))
print("work gdb", gdb, flush=True)


def wkt_df(rows):
    """rows: [(id, wkt)]. Built with SQL: a Python list needs a Python worker, broken on Spark 4.1.1."""
    vals = ", ".join(f"({i}L, '{w}')" for i, w in rows)
    return spark.sql(f"SELECT * FROM VALUES {vals} AS t(id, wkt)").select("id", S.st_fromtext("wkt").alias("geom"))


def rows_of(fc, fields):
    with arcpy.da.SearchCursor(fc, fields) as cur:
        return list(cur)


def near(a, b, tol=1e-6):
    assert abs(a - b) <= tol, f"{a} != {b}"


POINT = "POINT (1 2)"
LINE = "LINESTRING (0 0, 3 4)"
DONUT = "POLYGON ((10 10, 90 10, 90 90, 10 90, 10 10), (40 40, 40 60, 60 60, 60 40, 40 40))"


# --- to_feature_class
def t_fc_point():
    fc = os.path.join(gdb, "fc_point")
    wkt_df([(1, POINT), (2, "POINT (3 4)")]).to_feature_class("fc_point", sp_ref=3857, workspace=gdb)
    r = sorted(rows_of(fc, ["id", "SHAPE@X", "SHAPE@Y"]))
    assert r == [(1, 1.0, 2.0), (2, 3.0, 4.0)], r
    assert arcpy.Describe(fc).shapeType == "Point"
    return r


def t_fc_line():
    fc = os.path.join(gdb, "fc_line")
    wkt_df([(1, LINE)]).to_feature_class("fc_line", sp_ref=3857, workspace=gdb)
    (length,), = rows_of(fc, ["SHAPE@LENGTH"])
    near(length, 5.0)
    assert arcpy.Describe(fc).shapeType == "Polyline"
    return f"length {length}"


def t_fc_polygon():
    fc = os.path.join(gdb, "fc_polygon")
    wkt_df([(1, DONUT)]).to_feature_class("fc_polygon", sp_ref=3857, workspace=gdb)
    (area, geom), = rows_of(fc, ["SHAPE@AREA", "SHAPE@"])
    near(area, 6000.0)
    assert geom.boundary().partCount == 2 or geom.partCount == 1
    assert arcpy.Describe(fc).shapeType == "Polygon"
    return f"area {area}, hole kept"


def t_fc_srs():
    out = {}
    for wkid in (4326, 3857):
        fc = os.path.join(gdb, f"fc_sr{wkid}")
        wkt_df([(1, POINT)]).to_feature_class(f"fc_sr{wkid}", sp_ref=wkid, workspace=gdb)
        code = arcpy.Describe(fc).spatialReference.factoryCode
        assert code == wkid, (code, wkid)
        out[wkid] = code
    return out


def t_fc_geom_name():
    df = wkt_df([(1, POINT)]).withColumnRenamed("geom", "g")
    df.to_feature_class("fc_named", geom="g", sp_ref=3857, workspace=gdb)
    r = rows_of(os.path.join(gdb, "fc_named"), ["SHAPE@X", "SHAPE@Y"])
    assert r == [(1.0, 2.0)], r
    return r


def t_fc_missing():
    try:
        wkt_df([(1, POINT)]).to_feature_class("fc_bad", geom="nope", sp_ref=3857, workspace=gdb)
    except ValueError as e:
        return str(e)
    raise AssertionError("no ValueError")


def t_fc_overwrite():
    df = wkt_df([(1, POINT), (2, "POINT (3 4)")])
    df.to_feature_class("fc_twice", sp_ref=3857, workspace=gdb)
    df.to_feature_class("fc_twice", sp_ref=3857, workspace=gdb)
    n = int(arcpy.management.GetCount(os.path.join(gdb, "fc_twice"))[0])
    assert n == 2, n
    return f"{n} rows"


def t_fc_scratch():
    wkt_df([(1, POINT)]).to_feature_class("fc_scratch", sp_ref=3857)
    fc = os.path.join(arcpy.env.scratchGDB, "fc_scratch")
    assert arcpy.Exists(fc), fc
    assert os.path.normcase(work) in os.path.normcase(fc), fc
    return fc


def t_fc_string():
    df = spark.sql(f"SELECT 1L AS id, 'abc' AS label, ST_FROMTEXT('{POINT}') AS geom")
    df.to_feature_class("fc_string", sp_ref=3857, workspace=gdb)
    fields = [f.name for f in arcpy.ListFields(os.path.join(gdb, "fc_string"))]
    assert "label" in fields, f"string column dropped, got {fields}"
    return rows_of(os.path.join(gdb, "fc_string"), ["label"])


check("to_feature_class keeps a string column", t_fc_string)
check("to_feature_class point", t_fc_point)
check("to_feature_class polyline", t_fc_line)
check("to_feature_class polygon with hole", t_fc_polygon)
check("to_feature_class spatial reference", t_fc_srs)
check("to_feature_class custom geom column", t_fc_geom_name)
check("to_feature_class missing column raises ValueError", t_fc_missing)
check("to_feature_class overwrites", t_fc_overwrite)
check("to_feature_class default scratch workspace", t_fc_scratch)


# --- to_feature_table
def t_table():
    df = spark.sql("SELECT 1L AS n, 2.5D AS d, 'abc' AS s, TIMESTAMP'2026-01-02 03:04:05' AS t")
    df.to_feature_table("tbl", workspace=gdb)
    tbl = os.path.join(gdb, "tbl")
    types = {f.name: f.type for f in arcpy.ListFields(tbl)}
    assert "s" in types, f"string column dropped, got {types}"
    (n, d, s, t), = rows_of(tbl, ["n", "d", "s", "t"])
    assert (n, d, s) == (1, 2.5, "abc"), (n, d, s)
    assert (t.year, t.month, t.day, t.hour, t.minute, t.second) == (2026, 1, 2, 3, 4, 5), t
    return f"{types}"


def t_table_scratch():
    spark.sql("SELECT 1L AS n").to_feature_table("tbl_scratch")
    assert arcpy.Exists(os.path.join(arcpy.env.scratchGDB, "tbl_scratch"))
    return "written"


check("to_feature_table column types and values", t_table)
check("to_feature_table default scratch workspace", t_table_scratch)


# --- to_spark
def area_of(df):
    return [round(r.a, 6) for r in df.select(S.st_area("SHAPE").alias("a")).collect()]


def t_spark_default():
    df = S.to_spark(os.path.join(gdb, "fc_polygon"))
    assert [c.upper() for c in df.columns] == ["OBJECTID", "SHAPE"], df.columns
    a = area_of(df)
    assert a == [6000.0], a
    return f"columns {df.columns}, area {a}"


def t_spark_star():
    df = S.to_spark(os.path.join(gdb, "fc_point"), fields="*")
    assert "id" in df.columns and "SHAPE" in [c.upper() for c in df.columns], df.columns
    assert df.count() == 2
    return df.columns


def t_spark_list():
    df = S.to_spark(os.path.join(gdb, "fc_point"), fields=["id", "SHAPE"])
    assert [c.upper() for c in df.columns] == ["ID", "SHAPE"], df.columns
    return df.columns


def t_spark_where():
    df = S.to_spark(os.path.join(gdb, "fc_point"), fields=["id", "SHAPE"], where_clause="id = 2")
    r = df.select("id", S.st_astext("SHAPE").alias("w")).collect()
    assert [(x.id, x.w) for x in r] == [(2, "POINT (3 4)")], r
    return r[0].w


def t_spark_line():
    r = S.to_spark(os.path.join(gdb, "fc_line")).select(S.st_length("SHAPE").alias("l")).first()
    near(r.l, 5.0)
    return r.l


check("to_spark default fields", t_spark_default)
check("to_spark fields='*'", t_spark_star)
check("to_spark field list", t_spark_list)
check("to_spark where_clause", t_spark_where)
check("to_spark polyline", t_spark_line)


# --- Z and M through the helpers, using layers GDAL wrote
RINGS = [(0, 0), (0, 10), (10, 10), (10, 0), (2, 2), (4, 2), (4, 4), (2, 4), (20, 0), (20, 5), (25, 5)]


def expected(xy, z, m):
    return {tuple(float(v) for v in (x, y, *([100 + k] if z else []), *([1000 + k] if m else [])))
            for k, (x, y) in enumerate(xy)}


def vertices(w):
    return {tuple(float(v) for v in p.split()) for p in re.findall(r"-?\d[^,()]*", w)}


def wkt_tag(w):
    return re.match(r"[A-Z]+( ZM| Z| M)?", w).group(1) or ""


TAGS = {"xy": "", "xyz": " Z", "xym": " M", "xyzm": " ZM"}
shapes = os.path.join(base, "Shapes.gdb")


def t_zm_read(layer, kind):
    def run():
        w = S.to_spark(os.path.join(shapes, layer)).select(S.st_astext("SHAPE").alias("w")).first().w
        assert wkt_tag(w) == TAGS[kind], w[:90]
        assert vertices(w) == expected(RINGS, "z" in kind, "m" in kind), w[:90]
        return f"{len(vertices(w))} vertices{TAGS[kind]}"
    return run


def t_zm_write(layer, kind):
    def run():
        wkt = next(w for (n, _, w) in M.LAYERS if n == layer)
        name = "w_" + layer
        wkt_df([(1, wkt)]).to_feature_class(name, sp_ref=3857, workspace=gdb)
        d = arcpy.Describe(os.path.join(gdb, name))
        got = (bool(d.hasZ), bool(d.hasM))
        want = ("z" in kind, "m" in kind)
        assert got == want, f"hasZ/hasM {got}, wanted {want}"
        return f"hasZ/hasM {got}"
    return run


for kind in TAGS:
    check(f"to_spark keeps {kind} (poly_{kind})", t_zm_read(f"poly_{kind}", kind))
    check(f"to_feature_class keeps {kind} (poly_{kind})", t_zm_write(f"poly_{kind}", kind))


# --- bad input fails with a clear error, and leaves an existing target alone
def raises_keeping_target(name, df):
    def run():
        fc = os.path.join(gdb, name)
        wkt_df([(7, POINT)]).to_feature_class(name, sp_ref=3857, workspace=gdb)
        try:
            df.to_feature_class(name, sp_ref=3857, workspace=gdb)
        except ValueError as e:
            assert rows_of(fc, ["id"]) == [(7,)], "the existing feature class changed"
            return str(e)
        raise AssertionError("no ValueError")
    return run


check("to_feature_class null geometry raises", raises_keeping_target("x_null", spark.sql(
    f"SELECT 1L AS id, ST_FROMTEXT('{POINT}') AS geom UNION ALL SELECT 2L, CAST(NULL AS BINARY)")))
check("to_feature_class no rows raises", raises_keeping_target("x_empty", wkt_df([(1, POINT)]).filter("id < 0")))
check("to_feature_class mixed types raises", raises_keeping_target("x_mixed", wkt_df([(1, POINT), (2, LINE)])))


def t_multi_family():
    wkt_df([(1, "LINESTRING (0 0, 1 1)"), (2, "MULTILINESTRING ((5 5, 6 6), (7 7, 8 8))")]).to_feature_class(
        "fc_lines", sp_ref=3857, workspace=gdb)
    n = int(arcpy.management.GetCount(os.path.join(gdb, "fc_lines"))[0])
    assert n == 2, n
    return f"{n} rows"


check("to_feature_class accepts single and multi part together", t_multi_family)


def t_table_empty():
    try:
        spark.sql("SELECT 1L AS n").filter("n < 0").to_feature_table("t_empty", workspace=gdb)
    except ValueError as e:
        return str(e)
    raise AssertionError("no ValueError")


check("to_feature_table no rows raises", t_table_empty)


def t_spark_empty():
    arcpy.management.CreateFeatureclass(gdb, "fc_none", "POINT", spatial_reference=3857)
    df = S.to_spark(os.path.join(gdb, "fc_none"))
    assert df.count() == 0
    assert df.filter(S.st_area("SHAPE") > 0).count() == 0
    return df.columns


def t_spark_empty_zm():
    arcpy.management.CreateFeatureclass(gdb, "fc_none_z", "POINT", has_z="ENABLED", spatial_reference=3857)
    assert S.to_spark(os.path.join(gdb, "fc_none_z")).count() == 0
    return "no rows"


check("to_spark on an empty feature class", t_spark_empty)
check("to_spark on an empty Z feature class", t_spark_empty_zm)


def t_zm_precision():
    sr84 = arcpy.SpatialReference(4326)
    arcpy.management.CreateFeatureclass(gdb, "fc_prec", "POINT", has_z="ENABLED", has_m="ENABLED", spatial_reference=sr84)
    want = (-117.18143932399994, 34.056436279000025, 123.4567, 89.0125)
    with arcpy.da.InsertCursor(os.path.join(gdb, "fc_prec"), ["SHAPE@"]) as cur:
        cur.insertRow([arcpy.PointGeometry(arcpy.Point(want[0], want[1], want[2], want[3]), sr84, True, True)])
    w = S.to_spark(os.path.join(gdb, "fc_prec")).select(S.st_astext("SHAPE").alias("w")).first().w
    got = next(iter(vertices(w)))
    assert all(abs(a - b) < 1e-6 for a, b in zip(got, want)), (got, want)
    return got


check("to_spark Z/M coordinates keep their precision", t_zm_precision)


def n_current():
    wkt_df([(1, POINT)]).to_feature_class("fc_current", sp_ref=None, workspace=gdb)
    return "written"


note("to_feature_class sp_ref=None (needs the Pro application)", n_current)

# --- the gdb_* decoders on layers ArcGIS writes itself
PATHS = [(0, 0), (5, 5), (10, 0), (20, 0), (25, 5), (30, 0), (20, 0)]
MPTS = [(1, 2), (3, 4), (5, 6)]
PT = [(5, 7)]
TYPES = {"point": "POINT", "mpoint": "MULTIPOINT", "line": "POLYLINE", "poly": "POLYGON"}


arc = str(arcpy.management.CreateFileGDB(work, "arcgis.gdb"))
sr = arcpy.SpatialReference(3857)


def build(layer, wkt):
    name, kind = layer.split("_")
    arcpy.management.CreateFeatureclass(arc, layer, TYPES[name], spatial_reference=sr,
                                        has_z="ENABLED" if "z" in kind else "DISABLED",
                                        has_m="ENABLED" if "m" in kind else "DISABLED")
    with arcpy.da.InsertCursor(os.path.join(arc, layer), ["SHAPE@"]) as cur:
        cur.insertRow([arcpy.FromWKT(wkt, sr)])
    return f"{arcpy.Describe(os.path.join(arc, layer)).shapeType}"


for layer, _, wkt in M.LAYERS:
    check(f"arcpy writes {layer}", lambda layer=layer, wkt=wkt: build(layer, wkt))


def check_arc(layer, fn, xy, z, m):
    def run():
        df = spark.read.format("gdb").option("path", arc.replace("\\", "/")).option("name", layer).load()
        shape = next(c for c in df.columns if c.lower() == "shape")
        w = df.select(S.st_astext(fn(shape)).alias("w")).first().w
        tag = " ZM" if z and m else " Z" if z else " M" if m else ""
        assert wkt_tag(w) == tag, f"{w[:90]} wanted{tag!r}"
        assert vertices(w) == expected(xy, z, m), f"{sorted(vertices(w))[:4]} vs {sorted(expected(xy, z, m))[:4]}"
        return f"{len(vertices(w))} vertices{tag}"
    check(f"reader on ArcGIS {layer} {fn.__name__}", run)


KINDS = {"xy": (False, False), "xyz": (True, False), "xym": (False, True), "xyzm": (True, True)}
for kind, (z, m) in KINDS.items():
    check_arc(f"point_{kind}", S.gdb_point, PT, z, m)
for name, xy, f2, fz, fm in (("mpoint", MPTS, S.gdb_multipoint2, S.gdb_multipointZ, S.gdb_multipointM),
                             ("line", PATHS, S.gdb_polyline2, S.gdb_polylineZ, S.gdb_polylineM),
                             ("poly", RINGS, S.gdb_polygon2, S.gdb_polygonZ, S.gdb_polygonM)):
    for kind in KINDS:
        check_arc(f"{name}_{kind}", f2, xy, False, False)
    check_arc(f"{name}_xyz", fz, xy, True, False)
    check_arc(f"{name}_xyzm", fz, xy, True, False)
    check_arc(f"{name}_xym", fm, xy, False, True)
    check_arc(f"{name}_xyzm", fm, xy, True, True)

print(f"\nSUMMARY failed={len(fails)} {fails}", flush=True)
spark_esri.spark_stop()
