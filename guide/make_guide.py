"""Build index.html, the GeoFunctions guide, from template.html.

usage: python guide/make_guide.py   (needs the repo venv and target/geofunctions-<ver>.jar: run ./pw.sh)

Each {{fig:name}} in the template runs the code it shows on a real Spark session, draws the result
and embeds the image. So a code sample that stops working stops the build. The function reference
comes from the Python docstrings, GeoFunctions.scala, Registry.scala and the notebooks.
Nothing here is imported by the library.
"""
import ast
import base64
import glob
import html
import io
import json
import os
import re
import sys
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
SRC = os.path.join(REPO, "src", "main", "python", "geofunctions", "__init__.py")
SCALA = os.path.join(REPO, "src", "main", "scala", "com", "esri", "spark")
GDB = os.path.join(REPO, "src", "test", "resources", "Shapes.gdb")
sys.path.insert(0, os.path.dirname(os.path.dirname(SRC)))

VERSION = ET.parse(os.path.join(REPO, "pom.xml")).getroot().find("{http://maven.apache.org/POM/4.0.0}version").text

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, PathPatch, Rectangle  # noqa: E402
from matplotlib.path import Path  # noqa: E402
import shapely  # noqa: E402
from shapely.geometry import box as sbox  # noqa: E402
from shapely.geometry import LineString  # noqa: E402
from shapely.geometry.polygon import orient  # noqa: E402

BLUE, ORANGE, GREEN, RED, GRAY, INK = "#2a6fb0", "#d9822b", "#2f8f5b", "#c0392b", "#9aa3ad", "#1f2933"
LIGHT = "#eef1f4"
plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRAY, "axes.titlesize": 10.5, "axes.titleweight": "bold",
                     "xtick.color": "#5a6672", "ytick.color": "#5a6672", "font.family": "DejaVu Sans"})

os.environ.pop("SPARK_HOME", None)
JAR = os.path.join(REPO, "target", f"geofunctions-{VERSION}.jar")
H3 = sorted(glob.glob(os.path.expanduser("~/.m2/repository/com/uber/h3/*/h3-*.jar")))[-1]
os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession  # noqa: E402

spark = (SparkSession.builder.master("local[2]").appName("guide")
         .config("spark.jars", f"{JAR},{H3}")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.driver.host", "127.0.0.1").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

import geofunctions as gf  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

NS = {k: getattr(gf, k) for k in dir(gf) if re.match(r"(st|qr|h3|gdb|clip|join|knn|pairwise)_", k)}
NS.update(spark=spark, col=F.col, lit=F.lit, array=F.array, explode=F.explode, collect_list=F.collect_list,
          SHAPES_GDB=GDB)
FIGS = {}


def figure(name, title, alt, code=None, result=None):
    """Register the current matplotlib figure under a name."""
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=110, facecolor="white", bbox_inches="tight", pad_inches=0.08)
    plt.close()
    FIGS[name] = dict(title=title, alt=alt, code=code, result=result,
                      png=base64.b64encode(buf.getvalue()).decode())


def run(code):
    exec(code.strip(), NS)
    return NS


def geo(wkb):
    return shapely.from_wkb(bytes(wkb))


def draw(ax, g, color=BLUE, fill=True, alpha=0.35, lw=1.6, ms=6, ls="-", z=2):
    """Draw any shapely geometry to scale."""
    t = g.geom_type
    if t in ("MultiPolygon", "MultiLineString", "MultiPoint", "GeometryCollection"):
        for p in g.geoms:
            draw(ax, p, color, fill, alpha, lw, ms, ls, z)
    elif t == "Polygon":
        if g.is_empty:
            return
        g = orient(g)
        v, c = [], []
        for ring in [g.exterior, *g.interiors]:
            xy = list(ring.coords)
            v += xy
            c += [Path.MOVETO] + [Path.LINETO] * (len(xy) - 2) + [Path.CLOSEPOLY]
        ax.add_patch(PathPatch(Path(v, c), facecolor=color if fill else "none", alpha=alpha if fill else 1, lw=0, zorder=z))
        ax.add_patch(PathPatch(Path(v, c), facecolor="none", edgecolor=color, lw=lw, ls=ls, zorder=z + 0.1))
    elif t == "LineString":
        x, y = g.xy
        ax.plot(x, y, color=color, lw=lw + 0.4, ls=ls, zorder=z + 0.1, solid_capstyle="round")
    elif t == "Point":
        ax.plot(g.x, g.y, "o", color=color, ms=ms, mec="white", mew=0.8, zorder=z + 0.2)


def grid(ax, x0, y0, x1, y1, cell=1.0, labels=True):
    q = x0
    while q <= x1 + 1e-9:
        ax.plot([q, q], [y0, y1], color=GRAY, lw=0.7, zorder=1)
        q += cell
    r = y0
    while r <= y1 + 1e-9:
        ax.plot([x0, x1], [r, r], color=GRAY, lw=0.7, zorder=1)
        r += cell
    if labels:
        q = x0
        while q < x1 - 1e-9:
            r = y0
            while r < y1 - 1e-9:
                ax.text(q + 0.05 * cell, r + 0.05 * cell, f"{int(q // cell)},{int(r // cell)}", fontsize=7,
                        color="#6b7681", zorder=1.5)
                r += cell
            q += cell


def frame(ax, x0, y0, x1, y1, title=None, ticks=True):
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.set_aspect("equal")
    if title:
        ax.set_title(title)
    if not ticks:
        ax.set_xticks([])
        ax.set_yticks([])
    for s in ax.spines.values():
        s.set_visible(False)


def boxed(ax, x, y, w, h, text, color=BLUE, fs=9.0, bold=False, fill=None):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.012,rounding_size=0.018", fc=fill or "#f4f8fc",
                                ec=color, lw=1.4, zorder=2))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fs, color=INK, zorder=3,
            fontweight="bold" if bold else "normal", linespacing=1.3)


def arrow(ax, a, b, text=None, color=INK, off=(0, 0.02), fs=8, rad=0):
    ax.add_patch(FancyArrowPatch(a, b, arrowstyle="-|>", mutation_scale=11, lw=1.2, color=color, zorder=2.5,
                                 shrinkA=0, shrinkB=0, connectionstyle=f"arc3,rad={rad}"))
    if text:
        ax.text((a[0] + b[0]) / 2 + off[0], (a[1] + b[1]) / 2 + off[1], text, ha="center", va="bottom", fontsize=fs,
                color="#3d4852", linespacing=1.2)


def diagram(w=7.2, h=3.6):
    fig, ax = plt.subplots(figsize=(w, h))
    ax.set_xlim(-0.02, 1.02)
    ax.set_ylim(-0.03, 1.02)
    ax.axis("off")
    return ax


def table(headers, rows):
    head = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in r) + "</tr>" for r in rows)
    return f'<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>'


# ----- Diagrams -----------------------------------------------------------------------------------

def fig_architecture():
    ax = diagram(7.2, 4.1)
    boxed(ax, 0.02, 0.76, 0.28, 0.16, "Your code\nPySpark DataFrame or SQL", INK, fill=LIGHT, fs=8.5)
    boxed(ax, 0.38, 0.76, 0.28, 0.16, "Python wrappers\ngeofunctions/__init__.py", BLUE)
    boxed(ax, 0.72, 0.76, 0.26, 0.16, "arcpy bridge, joins\nPython only helpers", BLUE)
    boxed(ax, 0.38, 0.47, 0.28, 0.16, "GeoFunctions object\ntyped Column wrappers", ORANGE, fill="#fdf6ee")
    boxed(ax, 0.02, 0.47, 0.28, 0.16, "Registry\nSQL temp functions", ORANGE, fill="#fdf6ee")
    boxed(ax, 0.20, 0.18, 0.46, 0.16, "Catalyst expressions, one per function\ninterpreted eval and generated code",
          ORANGE, fill="#fdf6ee")
    boxed(ax, 0.72, 0.18, 0.26, 0.16, "Join and KNN processors\nR-tree per cell or task", ORANGE, fill="#fdf6ee", fs=8.5)
    boxed(ax, 0.02, 0.0, 0.96, 0.09, "Esri Geometry API, shaded in the jar. Geometry moves as WKB in binary columns.",
          GREEN, fill="#f0f8f3", fs=8.5)
    arrow(ax, (0.30, 0.84), (0.38, 0.84))
    arrow(ax, (0.66, 0.84), (0.72, 0.84))
    arrow(ax, (0.52, 0.76), (0.52, 0.63), "py4j", off=(0.035, -0.01))
    arrow(ax, (0.16, 0.76), (0.16, 0.63), "spark.sql", off=(0.05, -0.01))
    arrow(ax, (0.16, 0.47), (0.30, 0.34))
    arrow(ax, (0.52, 0.47), (0.46, 0.34))
    arrow(ax, (0.85, 0.76), (0.85, 0.34), "py4j", off=(0.035, 0.05))
    arrow(ax, (0.43, 0.18), (0.43, 0.09))
    arrow(ax, (0.85, 0.18), (0.85, 0.09))
    ax.text(0.01, 0.99, "Python wheel", fontsize=8.5, color=BLUE, fontweight="bold", va="top")
    ax.text(0.0, 0.41, "Shaded jar (Scala)", fontsize=8.5, color=ORANGE, fontweight="bold", va="top")
    figure("architecture", "The library has a Python wheel and a Scala jar",
           "Your code calls Python wrappers or SQL. Wrappers call the GeoFunctions object through py4j. "
           "Both reach Catalyst expressions, which use the Esri Geometry API on WKB columns.")


def fig_join_paths():
    ax = diagram(7.2, 3.5)
    ax.text(0.25, 0.97, "Default: shuffle both sides", ha="center", fontweight="bold", color=BLUE, fontsize=10)
    ax.text(0.75, 0.97, 'broadcast="lhs" or "rhs"', ha="center", fontweight="bold", color=ORANGE, fontsize=10)
    boxed(ax, 0.02, 0.72, 0.20, 0.14, "lhs rows\nqr_envp_explode", BLUE, fs=8.0)
    boxed(ax, 0.28, 0.72, 0.20, 0.14, "rhs rows\nqr_envp_explode", BLUE, fs=8.0)
    boxed(ax, 0.07, 0.42, 0.36, 0.15, "cogroup by QR cell\nboth sides move over the network", BLUE, fs=8.0)
    boxed(ax, 0.07, 0.15, 0.36, 0.15, "R-tree per cell, then\nexact oper test", BLUE)
    arrow(ax, (0.12, 0.72), (0.18, 0.57))
    arrow(ax, (0.38, 0.72), (0.32, 0.57))
    arrow(ax, (0.25, 0.42), (0.25, 0.30))
    boxed(ax, 0.52, 0.72, 0.20, 0.14, "small side\ncopied to each task", ORANGE, fill="#fdf6ee", fs=8.0)
    boxed(ax, 0.78, 0.72, 0.20, 0.14, "large side\nstays in place", ORANGE, fill="#fdf6ee", fs=8.0)
    boxed(ax, 0.57, 0.42, 0.36, 0.15, "equi-join on qr.qr\nno shuffle of the large side", ORANGE, fill="#fdf6ee", fs=8.0)
    boxed(ax, 0.57, 0.15, 0.36, 0.15, "qr_intersect, then\nthe st_* predicate", ORANGE, fill="#fdf6ee")
    arrow(ax, (0.62, 0.72), (0.68, 0.57))
    arrow(ax, (0.88, 0.72), (0.82, 0.57))
    arrow(ax, (0.75, 0.42), (0.75, 0.30))
    ax.text(0.5, 0.03, "Both paths return the same rows and the same columns.", ha="center", fontsize=9.5, color=INK)
    figure("join_paths", "join_qr has two execution paths",
           "Left: both sides shuffle by QR cell, then an R-tree per cell runs the exact test. Right: the small side is "
           "broadcast, the large side stays in place, and a Spark equi-join runs the test.")


def fig_arcpy():
    ax = diagram(7.2, 3.9)
    boxed(ax, 0.02, 0.66, 0.2, 0.17, "Spark DataFrame\nWKB geometry", ORANGE, fill="#fdf6ee")
    boxed(ax, 0.29, 0.66, 0.19, 0.17, "toPandas()\nthen checks", BLUE)
    boxed(ax, 0.54, 0.66, 0.22, 0.17, "_arrow_table\nlarge_string to string", BLUE, fs=8.0)
    boxed(ax, 0.82, 0.66, 0.16, 0.17, "CopyFeatures\nor CopyRows", GREEN, fill="#f0f8f3")
    arrow(ax, (0.22, 0.745), (0.29, 0.745))
    arrow(ax, (0.48, 0.745), (0.54, 0.745))
    arrow(ax, (0.76, 0.745), (0.82, 0.745))
    ax.text(0.5, 0.93, "to_feature_class and to_feature_table", ha="center", fontweight="bold", fontsize=10, color=INK)
    ax.text(0.385, 0.53, "ValueError before delete:\nno rows, null geometry,\nmixed geometry types", ha="center", va="center",
            fontsize=8.2, color=RED)
    boxed(ax, 0.82, 0.08, 0.16, 0.17, "Feature class\nin a geodatabase", GREEN, fill="#f0f8f3", fs=8.0)
    boxed(ax, 0.47, 0.08, 0.28, 0.17, "TableToArrowTable\nWKB, or WKT on a Z/M source", BLUE, fs=8.0)
    boxed(ax, 0.02, 0.08, 0.38, 0.17, "Spark DataFrame\nst_fromtext restores Z and M as WKB", ORANGE, fill="#fdf6ee", fs=8.0)
    arrow(ax, (0.82, 0.165), (0.75, 0.165))
    arrow(ax, (0.47, 0.165), (0.40, 0.165))
    ax.text(0.5, 0.33, "to_spark", ha="center", fontweight="bold", fontsize=10, color=INK)
    figure("arcpy", "The arcpy bridge moves data through Arrow",
           "Top: to_feature_class goes from Spark to pandas, checks, converts large strings, and writes with CopyFeatures. "
           "Bottom: to_spark reads with TableToArrowTable and restores Z and M with st_fromtext.")


def fig_pairing():
    ax = diagram(7.2, 2.9)
    rows = [("ArcGIS Pro 3.5 and earlier", "Spark 3.5, Scala 2.12", f"geofunctions-{VERSION}.jar", GREEN),
            ("ArcGIS Pro 3.7", "Spark 4.1, Scala 2.13", f"geofunctions-{VERSION}-2.13.jar", GREEN),
            ("PySpark 3.5.9", "Spark 3.5.9, Scala 2.12", f"geofunctions-{VERSION}.jar", GREEN),
            ("PySpark 4.3.0", "Spark 4.3.0, Scala 2.13", f"geofunctions-{VERSION}-2.13.jar", GREEN)]
    for i, (a, b, c, color) in enumerate(rows):
        y = 0.77 - i * 0.25
        boxed(ax, 0.01, y, 0.28, 0.17, a, BLUE, fs=8.0)
        boxed(ax, 0.36, y, 0.27, 0.17, b, GRAY, fs=8.5, fill=LIGHT)
        boxed(ax, 0.70, y, 0.29, 0.17, c, color, fs=8, fill="#f0f8f3")
        arrow(ax, (0.29, y + 0.085), (0.36, y + 0.085))
        arrow(ax, (0.63, y + 0.085), (0.70, y + 0.085))
    figure("pairing", "Use the jar that matches the Scala version of your Spark",
           "Pro 3.5 and PySpark 3.5.9 use the Scala 2.12 jar. Pro 3.7 and PySpark 4.3.0 use the Scala 2.13 jar.")


def fig_release():
    ax = diagram(7.2, 1.7)
    steps = [("pom.xml\nversion", GRAY), ("./gf.sh\n2 Scala lines", BLUE), ("verify_jar\nchecks jars", BLUE),
             ("release/\nzip, 2 jars", ORANGE), ("run.ps1\nPro tests", GREEN), ("gh release\ncreate", GREEN)]
    w, gap = 0.135, 0.038
    for i, (t, c) in enumerate(steps):
        x = 0.002 + i * (w + gap)
        boxed(ax, x, 0.25, w, 0.5, t, c, fs=8.0, fill=LIGHT if c == GRAY else None)
        if i:
            arrow(ax, (x - gap + 0.006, 0.5), (x - 0.006, 0.5))
    figure("release", "The release flow", "Set the version in pom.xml, run gf.sh, verify the jars, test on Pro, publish.")


# ----- Figures that run the library --------------------------------------------------------------

def fig_construct():
    code = '''
row = spark.range(1).select(
    st_point(lit(2.0), lit(3.0)).alias("point"),
    st_line(lit(1.0), lit(1.0), lit(4.0), lit(3.0)).alias("line"),
    st_rect(lit(1.0), lit(1.0), lit(4.0), lit(3.0)).alias("rect"),
    st_cell(lit(1.0), lit(1.0), lit(3.0), lit(2.0)).alias("cell"),
    st_box(lit(2.5), lit(2.0), lit(1.5), lit(1.0)).alias("box"),
    st_fromtext(lit("POLYGON ((1 1, 4 1, 4 4, 1 1))")).alias("polygon"),
).first()
'''
    n = run(code)["row"]
    fig, axes = plt.subplots(2, 3, figsize=(7.2, 4.6))
    notes = {"point": "st_point(2, 3)", "line": "st_line(1, 1, 4, 3)", "rect": "st_rect(1, 1, 4, 3)\nopposite corners",
             "cell": "st_cell(1, 1, 3, 2)\nlower-left, width, height", "box": "st_box(2.5, 2, 1.5, 1)\ncenter, half sizes",
             "polygon": "st_fromtext(WKT)"}
    for ax, key in zip(axes.flat, notes):
        draw(ax, geo(n[key]), ms=9)
        ax.grid(color=LIGHT, lw=0.8)
        frame(ax, 0, 0, 5, 5, notes[key])
        ax.set_xticks(range(6))
        ax.set_yticks(range(6))
    plt.tight_layout()
    figure("construct", "Functions that create a geometry",
           "Six panels show a point, a line and four polygons, each drawn from the WKB that the function returned.", code)


def fig_ops():
    code = '''
L = "POLYGON ((1 1, 5 1, 5 2, 2 2, 2 5, 1 5, 1 1))"
df = spark.range(1).select(st_fromtext(lit(L)).alias("geom"),
                           st_rect(lit(1.5), lit(0.5), lit(3.5), lit(4.0)).alias("other"))
row = df.select(
    "geom", "other",
    st_buffer("geom", lit(0.4)).alias("buffer"),
    st_convexhull("geom").alias("hull"),
    st_centroid("geom").alias("centroid"),
    st_intersection("geom", "other").alias("intersection"),
    st_translate("geom", lit(1.5), lit(1.0)).alias("translate"),
    st_extent("geom").alias("extent"),
    st_area("geom").alias("area"),
    st_length("geom").alias("length"),
).first()
'''
    n = run(code)["row"]
    base = geo(n["geom"])
    e = n["extent"]
    fig, axes = plt.subplots(2, 3, figsize=(7.2, 4.8))
    panels = [("st_buffer(geom, 0.4)", geo(n["buffer"]), ORANGE), ("st_convexhull(geom)", geo(n["hull"]), ORANGE),
              ("st_centroid(geom)", geo(n["centroid"]), RED), ("st_intersection(geom, other)", geo(n["intersection"]), ORANGE),
              ("st_translate(geom, 1.5, 1)", geo(n["translate"]), ORANGE),
              ("st_extent(geom)", sbox(e["xmin"], e["ymin"], e["xmax"], e["ymax"]), ORANGE)]
    for ax, (title, g, c) in zip(axes.flat, panels):
        draw(ax, base, GRAY, alpha=0.3, z=1)
        if "intersection" in title:
            draw(ax, geo(n["other"]), GREEN, fill=False, lw=1.1, ls="--")
        draw(ax, g, c, ms=9, fill="extent" not in title)
        frame(ax, 0, 0, 7, 6.2, title)
    plt.tight_layout()
    figure("ops", "Functions that change or measure a geometry",
           "The gray L shape is the input. Orange shapes are function output: buffer, hull, centroid, intersection, "
           "translation and extent.", code,
           f"st_area(geom) = {n['area']}, st_length(geom) = {n['length']}, "
           f"st_extent(geom) = {tuple(n['extent'])}")


def fig_predicates():
    code = '''
pairs = [("overlap",  "POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))", "POLYGON ((3 3, 6 3, 6 6, 3 6, 3 3))"),
         ("inside",   "POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))", "POLYGON ((3 3, 5 3, 5 5, 3 5, 3 3))"),
         ("touch",    "POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1))", "POLYGON ((3 2, 6 2, 6 5, 3 5, 3 2))"),
         ("apart",    "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))", "POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))")]
rows = [spark.range(1).select(
    lit(name).alias("name"),
    st_fromtext(lit(a)).alias("a"), st_fromtext(lit(b)).alias("b"))
    .select("name", "a", "b",
            st_intersects("a", "b").alias("intersects"), st_overlaps("a", "b").alias("overlaps"),
            st_contains("a", "b").alias("contains"), st_within("a", "b").alias("within"),
            st_touches("a", "b").alias("touches"), st_disjoint("a", "b").alias("disjoint"),
            st_iou("a", "b").alias("iou"), st_distance("a", "b").alias("distance"))
    .first() for name, a, b in pairs]
'''
    rows = run(code)["rows"]
    fig, axes = plt.subplots(1, 4, figsize=(7.2, 2.2))
    for ax, r in zip(axes, rows):
        draw(ax, geo(r["a"]), BLUE)
        draw(ax, geo(r["b"]), ORANGE)
        frame(ax, 0, 0, 7, 7, r["name"], ticks=False)
    plt.tight_layout()
    names = ["intersects", "overlaps", "contains", "within", "touches", "disjoint", "iou", "distance"]
    res = table(["pair (a blue, b orange)"] + [f"st_{k}" for k in names],
                [[r["name"]] + [round(r[k], 4) if isinstance(r[k], float) else str(r[k]).lower() for k in names] for r in rows])
    figure("predicates", "Spatial predicates on four pairs of polygons",
           "Four panels: two polygons that overlap, one inside the other, two that touch at an edge, and two that are apart.",
           code, res)


def fig_coords():
    code = '''
lons = [-120.0, -90.0, -60.0, -30.0, 0.0]
lats = [0.0, 20.0, 40.0, 60.0, 80.0]
grid_rows = spark.range(1).select(
    st_lontox(lit(-71.06)).alias("x"), st_lattoy(lit(42.36)).alias("y"),
    st_xtolon(lit(-7910363.016)).alias("lon"), st_ytolat(lit(5215059.175)).alias("lat"),
    st_haversine(lit(-71.06), lit(42.36), lit(-74.0), lit(40.7)).alias("boston_to_new_york_m"),
).first()
cells = [spark.range(1).select(
    st_mercator(st_rect(lit(a), lit(b), lit(a + 30.0), lit(b + 20.0))).alias("geom")).first()["geom"]
    for a in lons[:-1] for b in lats[:-1]]
'''
    n = run(code)
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.1))
    for a in n["lons"][:-1]:
        for b in n["lats"][:-1]:
            draw(axes[0], sbox(a, b, a + 30, b + 20), BLUE, alpha=0.08, lw=0.9)
    frame(axes[0], -125, -5, 5, 85, "Input: lon/lat cells of 30 by 20 degrees")
    for c in n["cells"]:
        draw(axes[1], geo(c), ORANGE, alpha=0.08, lw=0.9)
    axes[1].set_xlim(-14.5e6, 0.8e6)
    axes[1].set_ylim(-1e6, 19.5e6)
    axes[1].set_aspect("equal")
    axes[1].set_title("st_mercator output, in meters")
    axes[1].ticklabel_format(style="sci", scilimits=(6, 6))
    for s in axes[1].spines.values():
        s.set_visible(False)
    plt.tight_layout()
    r = n["grid_rows"]
    figure("coords", "st_mercator stretches cells at high latitude",
           "Left: equal cells in degrees. Right: the same cells in Web Mercator, taller near the pole.", code,
           f"x = {r['x']:.3f}, y = {r['y']:.3f}, lon = {r['lon']:.6f}, lat = {r['lat']:.6f}, "
           f"boston_to_new_york_m = {r['boston_to_new_york_m']:.1f}")


def fig_qr_grid():
    code = '''
row = spark.range(1).select(
    qr_fromxy(lit(2.5), lit(1.5), lit(1.0)).alias("qr"),
    st_xtoq(lit(2.5), lit(1.0)).alias("q"),
    st_ytor(lit(1.5), lit(1.0)).alias("r"),
    st_xtoq(lit(-0.5), lit(1.0)).alias("q_negative"),
).first()
'''
    n = run(code)["row"]
    assert n["qr"] == (n["q"] << 32) + n["r"]
    fig, ax = plt.subplots(figsize=(5.2, 3.4))
    grid(ax, -1, 0, 4, 3)
    ax.add_patch(Rectangle((2, 1), 1, 1, fc=BLUE, alpha=0.18, zorder=1))
    ax.plot(2.5, 1.5, "o", color=RED, ms=8, mec="white", zorder=3)
    ax.annotate("x = 2.5, y = 1.5\nq = 2, r = 1", (2.5, 1.5), (2.72, 2.32), fontsize=9, color=INK,
                arrowprops=dict(arrowstyle="-", color=INK, lw=0.8))
    frame(ax, -1.05, -0.05, 4.05, 3.05)
    ax.set_xlabel("x (labels in each cell are q,r)")
    ax.set_ylabel("y")
    figure("qr_grid", "The QR grid with cell = 1", "A grid of unit cells, labeled q,r. The point at 2.5, 1.5 is in cell 2,1.", code,
           f"qr = {n['qr']}, q = {n['q']}, r = {n['r']}, q_negative = {n['q_negative']}. "
           f"Check: (q << 32) + r = {(n['q'] << 32) + n['r']}.")


def fig_qr_dist():
    code = '''
point = "POINT (1.9 0.5)"
row = spark.range(1).select(
    st_fromtext(lit(point)).alias("geom"),
    qr_fromxy(lit(1.5), lit(0.5), lit(1.0)).alias("left"),
    qr_fromxy(lit(2.5), lit(0.5), lit(1.0)).alias("right"),
).select(
    "geom",
    qr_asgeom("left", lit(1.0), lit(0.2)).alias("left_cell"),
    qr_asgeom("right", lit(1.0), lit(0.2)).alias("right_cell"),
    qr_list("geom", lit(1.0)).alias("dist_0"),
    qr_list("geom", lit(1.0), lit(0.2)).alias("dist_02"),
).first()
'''
    n = run(code)["row"]
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.7))
    grid(axes[0], 1, 0, 3, 1, labels=True)
    draw(axes[0], geo(n["geom"]), RED, ms=9)
    axes[0].text(1.9, 0.62, f"{len(n['dist_0'])} cell", ha="center", fontsize=9)
    frame(axes[0], 0.6, -0.4, 3.4, 1.4, "dist = 0: cells do not overlap", ticks=True)
    grid(axes[1], 1, 0, 3, 1, labels=False)
    draw(axes[1], geo(n["left_cell"]), BLUE, alpha=0.15)
    draw(axes[1], geo(n["right_cell"]), ORANGE, alpha=0.15)
    draw(axes[1], geo(n["geom"]), RED, ms=9)
    axes[1].annotate("", (1.8, -0.28), (2.2, -0.28), arrowprops=dict(arrowstyle="<->", color=INK, lw=0.9))
    axes[1].text(2.0, -0.25, "2 x dist", ha="center", va="bottom", fontsize=8.5)
    axes[1].text(1.9, 0.62, f"{len(n['dist_02'])} cells", ha="center", fontsize=9)
    frame(axes[1], 0.6, -0.4, 3.4, 1.4, "dist = 0.2: cells overlap", ticks=True)
    plt.tight_layout()
    figure("qr_dist", "dist inflates the cell, not the geometry",
           "With dist 0 the point is in one cell. With dist 0.2 the two inflated cells overlap and the point is in both.", code,
           f"dist_0 = {list(n['dist_0'])}, dist_02 = {list(n['dist_02'])}")


def fig_qr_family():
    code = '''
df = spark.range(1).select(st_fromtext(lit(
    "POLYGON ((0.4 0.3, 3.6 0.6, 2.4 2.7, 0.4 0.3))")).alias("geom"))
row = df.select(
    qr_count("geom", lit(1.0)).alias("count"),
    qr_list("geom", lit(1.0)).alias("list"),
    qr_envp("geom", lit(1.0)).alias("envp"),
    qr_geom("geom", lit(1.0)).alias("clip"),
    "geom",
).first()
cells = df.select(explode(qr_list("geom", lit(1.0))).alias("qr")) \\
          .select(qr_asgeom("qr", lit(1.0)).alias("cell")).collect()
'''
    n = run(code)
    r = n["row"]
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.9))
    base = geo(r["geom"])
    for ax in axes:
        grid(ax, 0, 0, 4, 3, labels=False)
    draw(axes[0], base, BLUE, alpha=0.2)
    for c in n["cells"]:
        draw(axes[0], geo(c["cell"]), ORANGE, alpha=0.18, lw=1.0)
    frame(axes[0], -0.1, -0.1, 4.1, 3.1, f"qr_list: {len(r['list'])} cells")
    draw(axes[1], base, BLUE, alpha=0.2)
    e = r["envp"][0]
    axes[1].add_patch(Rectangle((e["xmin"], e["ymin"]), e["xmax"] - e["xmin"], e["ymax"] - e["ymin"], fc="none",
                                ec=RED, lw=1.5, ls="--", zorder=3))
    frame(axes[1], -0.1, -0.1, 4.1, 3.1, "qr_envp: the envelope,\nrepeated per cell")
    colors = [BLUE, ORANGE, GREEN, RED, "#7b5ea7", "#3a9aa8", "#a0702a", "#59636e", "#b65a86"]
    for i, c in enumerate(r["clip"]):
        draw(axes[2], geo(c["geom"]), colors[i % len(colors)], alpha=0.45, lw=1.0)
    frame(axes[2], -0.1, -0.1, 4.1, 3.1, f"qr_geom: {len(r['clip'])} clipped parts")
    plt.tight_layout()
    e0 = r["envp"][0]
    figure("qr_family", "Three ways to read the same cells",
           "qr_list gives cells over the envelope. qr_envp gives the envelope with each cell. qr_geom clips the geometry to each cell.",
           code,
           f"count = {r['count']}, list = {list(r['list'])[:4]} ... , "
           f"envp[0] = qr {e0['qr']}, envelope ({e0['xmin']}, {e0['ymin']}, {e0['xmax']}, {e0['ymax']})")


def fig_clip():
    code = '''
segment = array(lit(0.5), lit(0.5), lit(3.5), lit(2.2))
parts = spark.range(1).select(clip_line(segment, lit(1.0)).alias("c")).first()["c"]
total = sum(p["l"] for p in parts)
'''
    n = run(code)
    fig, ax = plt.subplots(figsize=(6.4, 4.8))
    grid(ax, 0, 0, 4, 3, labels=False)
    line = LineString([(0.5, 0.5), (3.5, 2.2)])
    colors = [BLUE, ORANGE, GREEN, RED, "#7b5ea7", "#3a9aa8"]
    for i, p in enumerate(n["parts"]):
        piece = line.intersection(sbox(p["q"], p["r"], p["q"] + 1, p["r"] + 1))
        draw(ax, piece, colors[i % len(colors)], lw=3)
        ax.text(p["q"] + 0.04, p["r"] + 0.05, f"cell {p['q']},{p['r']}\nl = {p['l']:.3f}", fontsize=8.5, linespacing=1.2)
    frame(ax, -0.05, -0.05, 4.05, 3.05)
    figure("clip", "clip_line splits one segment by cell",
           f"A segment crosses {len(n['parts'])} cells. Each color is one item of the result.", code,
           f"parts = {len(n['parts'])}, sum of l = {n['total']:.6f}, segment length = {line.length:.6f}")


def fig_join():
    code = '''
lhs = spark.range(1).select(lit(1).alias("lid"), st_fromtext(lit(
    "POLYGON ((0.5 0.5, 2.5 0.5, 2.5 2.5, 0.5 2.5, 0.5 0.5))")).alias("lgeom"))
rhs = spark.range(1).select(lit(9).alias("rid"), st_fromtext(lit(
    "POLYGON ((1.5 1.5, 3.5 1.5, 3.5 3.5, 1.5 3.5, 1.5 1.5))")).alias("rgeom"))

result = join_qr(lhs, rhs, 1.0, oper="intersects", lhs_geom="lgeom", rhs_geom="rgeom")
same = join_qr(lhs, rhs, 1.0, oper="intersects", lhs_geom="lgeom", rhs_geom="rgeom", broadcast="lhs")

left = lhs.select(qr_envp_explode("lgeom", 1.0).alias("l"))
right = rhs.select(qr_envp_explode("rgeom", 1.0).alias("r"))
shared = left.join(right, col("l.qr") == col("r.qr")) \\
             .select(col("l.qr").alias("qr"), qr_intersect("l", "r", 1.0).alias("canonical")).collect()
'''
    n = run(code)
    fig, ax = plt.subplots(figsize=(4.4, 3.8))
    grid(ax, 0, 0, 4, 4)
    a = shapely.from_wkt("POLYGON ((0.5 0.5, 2.5 0.5, 2.5 2.5, 0.5 2.5, 0.5 0.5))")
    b = shapely.from_wkt("POLYGON ((1.5 1.5, 3.5 1.5, 3.5 3.5, 1.5 3.5, 1.5 1.5))")
    draw(ax, a, BLUE, alpha=0.18)
    draw(ax, b, ORANGE, alpha=0.18)
    for s in n["shared"]:
        q, rr = s["qr"] >> 32, s["qr"] & 0xFFFFFFFF
        ax.add_patch(Rectangle((q, rr), 1, 1, fc="none", ec=GREEN if s["canonical"] else GRAY, lw=2.4 if s["canonical"] else 1.2,
                               ls="-" if s["canonical"] else ":", zorder=4))
    ax.plot(1.5, 1.5, "o", color=RED, ms=7, mec="white", zorder=5)
    ax.annotate("lower-left corner\nof the overlap", (1.5, 1.5), (2.45, 0.45), fontsize=8.2,
                arrowprops=dict(arrowstyle="-", color=INK, lw=0.8))
    frame(ax, -0.05, -0.05, 4.05, 4.05)
    canon = [s for s in n["shared"] if s["canonical"]]
    assert len(canon) == 1 and n["result"].count() == 1 and n["same"].count() == 1
    figure("join", "A pair is returned once, from its canonical cell",
           "Two squares share four cells. Only the cell that holds the lower-left corner of their overlap keeps the pair.", code,
           table(["shared qr", "q,r", "qr_intersect"],
                 [[s["qr"], f"{s['qr'] >> 32},{s['qr'] & 0xFFFFFFFF}", str(s["canonical"]).lower()] for s in n["shared"]])
           + f"<p>join_qr rows: {n['result'].count()}. broadcast rows: {n['same'].count()}. "
             f"Columns: {', '.join(n['result'].columns)}.</p>")


def fig_knn():
    code = '''
points = spark.sql("""SELECT * FROM VALUES (1, 2.0, 4.0), (2, 6.0, 1.5), (3, 8.5, 5.0) AS t(pid, x, y)""") \\
              .select("pid", st_point("x", "y").alias("geom"))
sites = spark.sql("""SELECT * FROM VALUES ('a',1.0,2.0),('b',3.5,5.5),('c',4.0,3.0),('d',5.0,0.5),
                     ('e',7.5,2.5),('f',8.0,0.5),('g',9.5,3.5),('h',7.0,6.0),('i',1.0,5.5) AS t(sid, x, y)""") \\
             .select("sid", st_point("x", "y").alias("site"))
rows = knn_join(points, sites, 2, lhs_geom="geom", rhs_geom="site").orderBy("pid", "distance").collect()
'''
    n = run(code)
    fig, ax = plt.subplots(figsize=(6.2, 3.4))
    pts = {}
    for r in n["rows"]:
        p, s = geo(r["geom"]), geo(r["site"])
        pts[r["pid"]] = p
        ax.plot([p.x, s.x], [p.y, s.y], color=ORANGE, lw=1.5, zorder=1)
        ax.text((p.x + s.x) / 2, (p.y + s.y) / 2 + 0.1, f"{r['distance']:.2f}", fontsize=8, ha="center")
    for r in n["sites"].collect():
        s = geo(r["site"])
        ax.plot(s.x, s.y, "s", color=GRAY, ms=7, zorder=2)
        ax.text(s.x + 0.12, s.y - 0.28, r["sid"], fontsize=8.5, color="#4b5563")
    for pid, p in pts.items():
        ax.plot(p.x, p.y, "o", color=BLUE, ms=9, mec="white", zorder=3)
        ax.text(p.x - 0.2, p.y + 0.14, str(pid), fontsize=9, fontweight="bold", color=BLUE)
    frame(ax, 0, -0.2, 10.4, 6.8)
    ax.grid(color=LIGHT, lw=0.7)
    figure("knn", "knn_join with k = 2", "Three blue points, each connected to its two nearest gray sites, with distances.", code,
           table(["pid", "sid", "distance"], [[r["pid"], r["sid"], round(r["distance"], 4)] for r in n["rows"]]))


def fig_dump():
    code = '''
wkt = ("MULTIPOLYGON (((10 10, 90 10, 90 90, 10 90, 10 10),"
       " (30 30, 30 70, 70 70, 70 30, 30 30)),"
       " ((42 42, 58 42, 58 58, 42 58, 42 42)),"
       " ((110 30, 150 30, 150 70, 110 70, 110 30)))")
df = spark.range(1).select(st_fromtext(lit(wkt)).alias("geom"))
parts = df.select(st_dump_explode("geom").alias("part")).collect()
outer = df.select(st_exterior_ring("geom").alias("g")).first()["g"]
'''
    n = run(code)
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.5))
    g = geo(run("source = df.first()['geom']")["source"])
    draw(axes[0], g, BLUE)
    frame(axes[0], 0, 0, 160, 100, "Input", ticks=False)
    colors = [BLUE, ORANGE, GREEN, RED]
    for i, p in enumerate(n["parts"]):
        draw(axes[1], geo(p["part"]), colors[i % 4], alpha=0.45)
    frame(axes[1], 0, 0, 160, 100, f"st_dump_explode: {len(n['parts'])} parts", ticks=False)
    draw(axes[2], geo(n["outer"]), GREEN)
    frame(axes[2], 0, 0, 160, 100, "st_exterior_ring", ticks=False)
    plt.tight_layout()
    assert len(n["parts"]) == 3
    figure("dump", "st_dump keeps holes, st_exterior_ring fills them",
           "The input has a square with a hole, an island in the hole, and a separate square. st_dump gives three parts and keeps "
           "the hole. st_exterior_ring gives two outlines with no hole.", code,
           f"parts = {[round(geo(p['part']).area) for p in n['parts']]} (area of each part), "
           f"st_exterior_ring area = {round(geo(n['outer']).area)}")


def fig_dissolve():
    code = '''
wkts = ["POLYGON ((1.2 1.2, 2.6 1.2, 2.6 2.6, 1.2 2.6, 1.2 1.2))",
        "POLYGON ((2.4 1.4, 3.8 1.4, 3.8 2.8, 2.4 2.8, 2.4 1.4))",
        "POLYGON ((3.6 2.2, 5.0 2.2, 5.0 3.6, 3.6 3.6, 3.6 2.2))",
        "POLYGON ((1.4 2.4, 2.4 2.4, 2.4 3.8, 1.4 3.8, 1.4 2.4))",
        "POLYGON ((5.6 0.6, 7.0 0.6, 7.0 2.0, 5.6 2.0, 5.6 0.6))",
        "POLYGON ((6.1 3.0, 7.6 3.0, 7.6 4.5, 6.1 4.5, 6.1 3.0),"
        " (6.5 3.4, 6.5 4.1, 7.2 4.1, 7.2 3.4, 6.5 3.4))"]
polygons = spark.createDataFrame([(w,) for w in wkts], ["wkt"]).select(st_fromtext("wkt").alias("geom"))
rows = pairwise_dissolve(polygons, cell=2.0, dist=0.0, wkid=-1).collect()
'''
    n = run(code)
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.0))
    src = NS["polygons"].collect()
    colors = [BLUE, ORANGE, GREEN, RED, "#7b5ea7", "#3a9aa8"]
    grid(axes[0], 0, 0, 8, 6, cell=2.0, labels=False)
    for i, r in enumerate(src):
        draw(axes[0], geo(r["geom"]), colors[i % 6], alpha=0.3, lw=1.1)
    frame(axes[0], -0.1, 0, 8.1, 5.2, f"Input: {len(src)} polygons, cell = 2")
    grid(axes[1], 0, 0, 8, 6, cell=2.0, labels=False)
    for i, r in enumerate(n["rows"]):
        draw(axes[1], geo(r["geom"]), colors[i % 6], alpha=0.35)
    frame(axes[1], -0.1, 0, 8.1, 5.2, f"pairwise_dissolve: {len(n['rows'])} polygons")
    plt.tight_layout()
    union = shapely.union_all([geo(r["geom"]) for r in src])
    out = sum(geo(r["geom"]).area for r in n["rows"])
    assert abs(out - union.area) < 1e-6, (out, union.area)
    figure("dissolve", "pairwise_dissolve merges across cell edges and keeps holes",
           "Six overlapping polygons, some across cell edges, become three polygons. The hole in the right polygon stays.", code,
           f"Output rows = {len(n['rows'])}. Output area = {out:.4f}. shapely union_all area of the input = {union.area:.4f}.")


def fig_gdb():
    code = '''
def layer(name):
    return spark.read.format("gdb").option("path", SHAPES_GDB).option("name", name).load()

poly = layer("poly_xyzm").select(gdb_polygonM("SHAPE").alias("geom"))
text = poly.select(st_astext("geom").alias("wkt")).first()["wkt"]
'''
    n = run(code)
    layer = n["layer"]
    fig, axes = plt.subplots(1, 4, figsize=(7.2, 1.9))
    for ax, (nm, fn, lim) in zip(axes, [("point_xy", gf.gdb_point, (0, 0, 10, 10)), ("mpoint_xy", gf.gdb_multipoint2, (0, 0, 8, 8)),
                                        ("line_xy", gf.gdb_polyline2, (-1, -14, 31, 18)), ("poly_xy", gf.gdb_polygon2, (-1, -9, 26, 18))]):
        draw(ax, geo(layer(nm).select(fn("SHAPE")).first()[0]), BLUE, ms=8)
        frame(ax, *lim, nm, ticks=False)
    plt.tight_layout()
    rows = []
    for kind in ("xy", "xyz", "xym", "xyzm"):
        s = layer(f"poly_{kind}").first()["SHAPE"]
        per = len(s["coords"]) // sum(s["parts"])
        fn = {"xy": gf.gdb_polygon2, "xyz": gf.gdb_polygonZ, "xym": gf.gdb_polygonM, "xyzm": gf.gdb_polygonM}[kind]
        fname = {"xy": "gdb_polygon2", "xyz": "gdb_polygonZ", "xym": "gdb_polygonM", "xyzm": "gdb_polygonM"}[kind]
        w = layer(f"poly_{kind}").select(gf.st_astext(fn("SHAPE")).alias("w")).first()["w"]
        rows.append([f"poly_{kind}", per, fname, w[:58] + " ..."])
    figure("gdb", "Layers in the test file Shapes.gdb", "A point, a multipoint, a two-path line and a polygon with a hole and a second part.",
           code, table(["layer", "values per vertex", "function", "st_astext result"], rows))


def fig_h3():
    code = '''
places = spark.sql("""SELECT * FROM VALUES
    (42.3601, -71.0589), (42.3736, -71.1097), (42.3467, -71.0972),
    (42.3188, -71.0846), (42.3875, -71.0995) AS t(lat, lon)""")
cells = places.select("lat", "lon", h3_latlng_to_cell("lat", "lon", lit(7)).alias("cell")) \\
              .select("lat", "lon", "cell", h3_cell_to_boundary("cell").alias("hex")).collect()
'''
    n = run(code)
    fig, ax = plt.subplots(figsize=(5.2, 3.6))
    for r in n["cells"]:
        draw(ax, geo(r["hex"]), ORANGE, alpha=0.22)
        ax.plot(r["lon"], r["lat"], "o", color=BLUE, ms=6, mec="white", zorder=4)
    ax.margins(0.08)
    ax.autoscale()
    ax.set_aspect(1 / 0.74)
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    ax.grid(color=LIGHT, lw=0.7)
    for s in ax.spines.values():
        s.set_visible(False)
    figure("h3", "H3 cells at resolution 7", "Five points, each inside its H3 hexagon at resolution 7.", code,
           f"cell[0] = {n['cells'][0]['cell']}")


def fig_bench():
    rows = [json.loads(line) for line in open(os.path.join(REPO, "benchmarks", "spatialbench", "results", "sf1-sf10.jsonl"))]

    def median(engine, q):
        for r in rows:
            if (r["engine"], r["sf"], r["layout"], r["query"]) == (engine, "sf1", "asis", q) and r.get("version"):
                return None if r.get("skipped") else r.get("median")
        return None

    qs = [f"Q{i}" for i in range(1, 13)]
    fig, ax = plt.subplots(figsize=(7.0, 3.5))
    for i, q in enumerate(qs):
        s, d = median("spark", q), median("duckdb", q)
        y = len(qs) - i
        if s and d:
            ax.plot([s, d], [y, y], color=LIGHT, lw=3, zorder=1)
        if s:
            ax.plot(s, y, "o", color=BLUE, ms=7, zorder=3, label="GeoFunctions on Spark 4.3.0" if i == 0 else None)
        if d:
            ax.plot(d, y, "D", color=ORANGE, ms=6, zorder=3, label="DuckDB 2.0.0" if i == 0 else None)
        else:
            ax.text(620, y, "DuckDB: no result", fontsize=7.5, va="center", ha="right", color="#5a6672")
    ax.set_xscale("log")
    ax.set_xlim(0.02, 700)
    ax.set_yticks(range(1, len(qs) + 1))
    ax.set_yticklabels(qs[::-1])
    ax.set_xlabel("median query time in seconds, log scale (SF1, files as published)")
    ax.grid(axis="x", color=LIGHT, lw=0.8)
    ax.legend(loc="upper right", frameon=False, fontsize=8.5)
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
    for s in ax.spines.values():
        s.set_visible(False)
    figure("bench", "SpatialBench SF1 query times",
           "Dot plot of median time for queries 1 to 12 at scale factor 1, GeoFunctions on Spark and DuckDB, on a log scale.")


# ----- Function reference -------------------------------------------------------------------------

def sql_names():
    """Python function -> SQL names, by way of GeoFunctions.scala and Registry.scala."""
    reg = {}
    for sql, cls in re.findall(r'^\s*funcReg\.createOrReplaceTempFunction\("(\w+)", (\w+),', open(os.path.join(SCALA, "Registry.scala")).read(), re.M):
        reg.setdefault(cls, []).append(sql)
    scala = open(os.path.join(SCALA, "GeoFunctions.scala")).read()
    wrappers = {}
    for m in re.finditer(r"def (\w+)\(.*?toColumn\((\w+)\(", scala, re.S):
        wrappers[m.group(1)] = m.group(2)
    return reg, wrappers


def notebooks():
    used = {}
    for path in sorted(glob.glob(os.path.join(REPO, "notebooks", "*.ipynb"))):
        try:
            cells = json.load(open(path))["cells"]
        except (ValueError, KeyError):
            continue
        code = "\n".join("".join(c["source"]) for c in cells if c["cell_type"] == "code")
        for name in set(re.findall(r"\b([a-z0-9_A-Z]+)\(", code)):
            used.setdefault(name, []).append(os.path.basename(path))
    return used


def sections():
    """Function -> section, from the headings of functions.md."""
    out, sect, order = {}, None, []
    for line in open(os.path.join(REPO, "functions.md")):
        if line.startswith("## ") and "Contents" not in line and "Usage" not in line:
            sect = line[3:].strip()
            order.append(sect)
        m = re.match(r"### `?([A-Za-z0-9_]+)\(", line)
        if m and sect:
            out[m.group(1)] = sect
    return out, order


def parse_doc(doc):
    lines = (doc or "").split("\n")
    text, params, ret, raises, example, i = [], {}, "", [], [], 0
    types, cur = {}, None
    while i < len(lines):
        line = lines[i]
        s = line.strip()
        if s.startswith("Example::"):
            example = [ln.strip() if ln.startswith("    ") and not ln.startswith("     ") else ln for ln in lines[i + 1:]]
            break
        m = re.match(r":param (\*?\*?\w+):\s*(.*)", s)
        if m:
            cur = ("p", m.group(1))
            params[m.group(1)] = m.group(2)
        elif re.match(r":type (\*?\*?\w+):", s):
            m = re.match(r":type (\*?\*?\w+):\s*(.*)", s)
            types[m.group(1)] = m.group(2)
            cur = None
        elif s.startswith(":return:"):
            ret = s[8:].strip()
            cur = ("r", None)
        elif s.startswith(":rtype:"):
            cur = None
        elif s.startswith(":raises"):
            m = re.match(r":raises (\w+):\s*(.*)", s)
            raises.append(list(m.groups()) if m else ["", s])
            cur = ("x", None)
        elif cur and line.startswith(" ") and s:
            if cur[0] == "p":
                params[cur[1]] += " " + s
            elif cur[0] == "r":
                ret += " " + s
            else:
                raises[-1][1] += " " + s
        elif not cur:
            text.append(s)
        i += 1
    para = [p.replace("\n", " ").strip() for p in "\n".join(text).split("\n\n") if p.strip()]
    return para, params, types, ret, raises, [e for e in example if e.strip()]


def inline(text):
    text = html.escape(text)
    text = re.sub(r"``([^`]+)``", r"<code>\1</code>", text)
    return text


def reference():
    tree = ast.parse(open(SRC).read())
    src = open(SRC).read()
    fns = [n for n in tree.body if isinstance(n, ast.FunctionDef) and not n.name.startswith("_")]
    reg, wrappers = sql_names()
    used = notebooks()
    where, order = sections()
    missing = [f.name for f in fns if f.name not in where]
    assert not missing, f"not in functions.md: {missing}"
    stale = sorted(set(where) - {f.name for f in fns})
    assert not stale, f"in functions.md, not in the code: {stale}"
    by = {s: [] for s in order}
    for f in fns:
        by[where[f.name]].append(f)
    toc, out = [], []
    n_sql = set()
    for sect in order:
        items = sorted(by[sect], key=lambda f: f.name)
        if not items:
            continue
        sid = "ref-" + re.sub(r"[^a-z]+", "-", sect.lower()).strip("-")
        toc.append(f'<li><a href="#{sid}">{html.escape(sect)}</a> ({len(items)})</li>')
        out.append(f'<h3 id="{sid}">{html.escape(sect)}</h3>')
        for f in items:
            sig = ast.unparse(f.args)
            para, params, types, ret, raises, example = parse_doc(ast.get_docstring(f))
            body = ast.get_source_segment(src, f) or ""
            jvm = re.findall(r"GeoFunctions\.(\w+)\(", body)
            sql = sorted({s for j in jvm for s in reg.get(wrappers.get(j, ""), [])})
            n_sql.update(sql)
            cls = sorted({wrappers[j] for j in jvm if j in wrappers})
            nb = used.get(f.name, [])
            out.append(f'<section class="fn" id="fn-{f.name}"><h4>{f.name}</h4>')
            out.append(f'<pre class="sig">{f.name}({html.escape(sig)})</pre>')
            out.extend(f"<p>{inline(p)}</p>" for p in para)
            if params:
                out.append('<table class="params"><thead><tr><th>Parameter</th><th>Type</th><th>Description</th></tr></thead><tbody>')
                for k, v in params.items():
                    out.append(f"<tr><td><code>{html.escape(k)}</code></td><td><code>{html.escape(types.get(k, ''))}</code></td>"
                               f"<td>{inline(v)}</td></tr>")
                out.append("</tbody></table>")
            facts = []
            if ret:
                facts.append(("Returns", inline(ret)))
            for typ, why in raises:
                facts.append(("Raises", f"<code>{html.escape(typ)}</code>: {inline(why)}"))
            if sql:
                facts.append(("SQL name", ", ".join(f"<code>{s}</code>" for s in sql)))
            elif jvm:
                facts.append(("SQL name", "None. Use the Python function."))
            else:
                facts.append(("SQL name", "None. This function is Python only."))
            if cls:
                facts.append(("Scala", ", ".join(f"<code>{c}</code>" for c in cls)))
            if nb:
                facts.append(("Notebooks", ", ".join(f'<a href="../notebooks/{b}">{b[:-6]}</a>' for b in nb[:5])
                              + (f" and {len(nb) - 5} more" if len(nb) > 5 else "")))
            out.append('<dl class="facts">' + "".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in facts) + "</dl>")
            if example:
                out.append('<pre><code>' + html.escape("\n".join(re.sub(r"\s{4,}\.", "\n    .", e) for e in example)) + "</code></pre>")
            out.append("</section>")
    all_sql = sorted({s for v in reg.values() for s in v})
    unmapped = sorted(set(all_sql) - n_sql)
    head = f"<p>The library has {len(fns)} public Python functions and {len(all_sql)} SQL names.</p>"
    head += "<ul class=\"cols\">" + "".join(toc) + "</ul>"
    extra = ""
    if unmapped:
        extra = ("<p>These SQL names have no Python function with the same expression: "
                 + ", ".join(f"<code>{s}</code>" for s in unmapped) + ".</p>")
    return head + extra + "\n".join(out), len(fns), len(all_sql)


def notebook_table():
    rows = []
    for path in sorted(glob.glob(os.path.join(REPO, "notebooks", "*.ipynb"))):
        name = os.path.basename(path)
        try:
            cells = json.load(open(path))["cells"]
        except (ValueError, KeyError):
            cells = []
        first = next(("".join(c["source"]).strip() for c in cells if c["cell_type"] == "markdown" and "".join(c["source"]).strip()), "")
        title = re.sub(r"[#*`\[\]]", "", first.split("\n")[0]).strip()[:90]
        rows.append(f'<tr><td><a href="../notebooks/{name}">{name[:-6]}</a></td><td>{html.escape(title)}</td></tr>')
    return ("<table><thead><tr><th>Notebook</th><th>First heading in the notebook</th></tr></thead><tbody>"
            + "".join(rows) + "</tbody></table>"), len(rows)


# ----- Page ---------------------------------------------------------------------------------------

def render_fig(name, number):
    f = FIGS[name]
    code = f'<pre><code>{html.escape(f["code"].strip())}</code></pre>' if f["code"] else ""
    res = f["result"] or ""
    if res and not res.lstrip().startswith("<"):
        res = f'<pre class="result"><code>{html.escape(res)}</code></pre>'
    elif res:
        res = f'<div class="result">{res}</div>'
    return (f'<figure id="fig-{name}"><img src="data:image/png;base64,{f["png"]}" alt="{html.escape(f["alt"], quote=True)}">'
            f'<figcaption><b>Figure {number}.</b> {html.escape(f["title"])}.</figcaption></figure>{code}{res}')


def main():
    for build in (fig_architecture, fig_join_paths, fig_arcpy, fig_pairing, fig_release, fig_construct, fig_ops, fig_predicates,
                  fig_coords, fig_qr_grid, fig_qr_dist, fig_qr_family, fig_clip, fig_join, fig_knn, fig_dump, fig_dissolve, fig_gdb,
                  fig_h3, fig_bench):
        build()
        print("figure", build.__name__, flush=True)
    page = open(os.path.join(HERE, "template.html")).read()
    numbers = {}
    for name in re.findall(r"\{\{fig:(\w+)\}\}", page):
        assert name not in numbers, name
        numbers[name] = len(numbers) + 1
    unused = set(FIGS) - set(numbers)
    assert not unused, f"figures not in the template: {unused}"
    page = re.sub(r"\{\{fig:(\w+)\}\}", lambda m: render_fig(m.group(1), numbers[m.group(1)]), page)
    page = re.sub(r"\{\{ref:(\w+)\}\}", lambda m: f'<a href="#fig-{m.group(1)}">Figure {numbers[m.group(1)]}</a>', page)
    ref, n_py, n_sql = reference()
    nb, n_nb = notebook_table()
    page = (page.replace("{{reference}}", ref).replace("{{notebooks}}", nb).replace("{{version}}", VERSION)
            .replace("{{n_py}}", str(n_py)).replace("{{n_sql}}", str(n_sql)).replace("{{n_nb}}", str(n_nb)))
    left = re.findall(r"\{\{[^}]*\}\}", page)
    assert not left, left
    with open(os.path.join(HERE, "index.html"), "w") as out:
        out.write(page)
    print(f"index.html: {len(page) // 1024} KB, {len(numbers)} figures, {n_py} functions, {n_sql} SQL names, {n_nb} notebooks")
    spark.stop()


if __name__ == "__main__":
    main()
