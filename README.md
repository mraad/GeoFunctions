# GeoFunctions

This is a small collection of PySpark functions useful for working with geospatial data using the [Esri Geometry API for Java](https://github.com/Esri/geometry-api-java.git).
It is typically used within an ArcGIS Pro conda environment, and the spark engine is exposed using the [Spark Esri](https://github.com/mraad/spark-esri) package.

NOTE: Use the jar that matches your ArcGIS Pro. The release zip carries both.

| ArcGIS Pro | Spark in Pro | Jar | Status |
|---|---|---|---|
| 3.5 and earlier | 3.5 | `geofunctions-<ver>.jar` (Scala 2.12) | Supported |
| 3.6 | | | Does not work (not retested) |
| 3.7 | 4.1 | `geofunctions-<ver>-2.13.jar` (Scala 2.13) | Tested on 3.7.3; see [ArcGIS Pro 3.7](#arcgis-pro-37) |

NOTE: The default build targets **Spark 3.5.9** and Scala 2.12. Java 11 is the build baseline;
Java 17 is also supported and smoke-tested. A **Spark 4.3.0 / Scala 2.13 / Java 17** build
(the latest Spark) exists behind `-P spark-4.3`; see [Spark 4](#spark-4).

### Core Geometry Dependency

The Scala implementation depends on
[`com.esri.geometry:esri-geometry-api:2.3.1`](https://github.com/Esri/geometry-api-java.git).
The dependency is shaded into the GeoFunctions JAR, so runtime users do not install it
separately. A source build needs the snapshot in its Maven repository; install it once if
Maven cannot resolve it:

```shell
git clone https://github.com/Esri/geometry-api-java.git
cd geometry-api-java
mvn install
```

Keep the source checkout and the coordinate in `pom.xml` aligned when updating this
dependency.

### ArcGIS Pro 3.7

Pro 3.7.3 ships Spark 4.1.1, Scala 2.13, Java 17 and Python 3.13. Use
`geofunctions-<ver>-2.13.jar`. The Scala 2.12 jar cannot load on Spark 4. The 2.13 jar is built for
Spark 4.3 and was tested on Spark 4.1.1 and 4.1.3 through `spark_esri`: basic functions, joins, the
`gdb` reader on every layer type and the `shp` reader. The tests ran as Python scripts in the conda
environment, not inside the Pro application. H3, the arcpy bridge and the notebooks are not tested
on 3.7.

Install PySpark 4.1.3 in the environment. Python workers crash on Spark 4.1.1 on Windows with
Python 3.13 ([SPARK-53759](https://issues.apache.org/jira/browse/SPARK-53759), Esri KB 000039267),
so `createDataFrame` from a Python list, `rdd.map()`, `@udf` and `@pandas_udf` fail with
`Python worker exited unexpectedly`. Spark SQL and the GeoFunctions functions do not need a worker.

```shell
pip install pyspark==4.1.3
set SPARK_HOME=%CONDA_PREFIX%\Lib\site-packages\pyspark
```

Restart the notebook kernel after you set `SPARK_HOME`.

### Create New Conda Environment

Using ArcGIS Python Command Prompt:

```shell
cd %HOMEPATH%
conda remove -n geofunctions --yes --all
conda create -n geofunctions --yes --clone arcgispro-py3
proswap geofunctions

git clone https://github.com/mraad/spark-esri.git
cd spark-esri
pip install .
```

Install the geofunctions package using:

```shell
pip install --no-deps <path-to>/geofunctions-0.38-py3-none-any.whl
```

Optional packages to install:

```shell
pip install geopandas mapclassify folium xyzservices duckdb fastparquet
```

The arcpy bridge (`to_spark`, `to_feature_class`, `to_feature_table`) additionally needs
`pyarrow` and `pandas`. Both ship with ArcGIS Pro, so there is nothing to install there. They
are **optional** dependencies of this package — declared only in the `jupyter` extra, so a
base `pip install` omits them — and those three functions are the only ones that need them.
Everything else works without them.

### Standalone PySpark

The wheel also works outside ArcGIS Pro against a plain PySpark install. It declares
`pyspark>=3.5.9,<5`, so it installs against either supported line:

```shell
pip install pyspark==3.5.9   # pairs with the default -P spark-3.5 jar
pip install pyspark==4.3.0   # pairs with the -P spark-4.3 jar; see Spark 4 below
```

**The jar is not interchangeable between them.** It is compiled with Spark `provided`-scope
so it loads on any host of its own line, but a Scala 2.12 jar on PySpark 4.x fails with
`NoClassDefFoundError: scala/collection/SeqOps`. Match the jar to the PySpark major version.

If `SPARK_HOME` is set (sdkman installs one, for instance) PySpark uses those jars instead of
its own bundled ones, and a mismatched Spark there produces the same class-loading failures.
Unset it when running against a pip-installed PySpark.

### Java compatibility

Java 11 and Java 17 both pass the QR processor smoke tests and produce identical join
results. The project continues to compile for Java 11 so the same artifact can run on either
JVM. When Spark is embedded directly in Maven on Java 17, open Spark's required JDK module:

```shell
JAVA_TOOL_OPTIONS=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  mvn -DskipTests=false test
```

The normal PySpark launcher supplies its Java 17 module options. On Java 11, Arrow workloads
may additionally need `-Dio.netty.tryReflectionSetAccessible=true`, as described in the
[Spark 3.5.9 requirements](https://spark.apache.org/docs/3.5.9/).

Java 21 is not supported by Spark 3.5.9. Official Java 21 support starts with Spark 4.0
— see below.

### Spark 4

`mvn -P spark-4.3 clean package` builds against **Spark 4.3.0** (the latest release),
Scala 2.13.18 and Java 17. Pair the jar with `pip install pyspark==4.3.0`. Tests are skipped by
default, so ask for them explicitly to run the suite:

```shell
mvn -P spark-4.3 -DskipTests=false clean package
```

`./gf.sh` ships this build as `geofunctions-<ver>-2.13.jar`, also inside the release zip. Use it
against standalone Spark 4.3 hosts and clusters, and in ArcGIS Pro 3.7. ArcGIS Pro 3.5 needs the
default 3.5.9 jar. Spark 4.3 is the only
Spark 4 line; the `spark-4.0` profile is gone.

The three `com.esri` dependencies have **never been published to Maven Central** — the
`com.esri` group there holds only Esri's own artifacts. Get them from their releases. Each
release carries a Spark 3.5 / Scala 2.12 jar and a Spark 4.3 / Scala 2.13 jar; each profile here
links the readers built for its own Spark line.

| Dependency | Release | Scala 2.13 jar |
|---|---|---|
| [WebMercator](https://github.com/mraad/WebMercator/releases/tag/v1.15) | 1.15 | `webmercator-1.15-2.13.jar` |
| [spark-shp](https://github.com/mraad/spark-shp/releases/tag/v0.33) | 0.33 | `spark-shp-0.33-4.3-2.13.jar` |
| [FileGDB](https://github.com/mraad/FileGDB/releases/tag/v0.69) | 0.69 | `filegdb-0.69-4.3-2.13.jar` |

```shell
mvn install:install-file -Dfile=webmercator-1.15-2.13.jar \
  -DgroupId=com.esri -DartifactId=webmercator -Dversion=1.15-2.13 -Dpackaging=jar
mvn install:install-file -Dfile=spark-shp-0.33-4.3-2.13.jar \
  -DgroupId=com.esri -DartifactId=spark-shp -Dversion=0.33-4.3-2.13 -Dpackaging=jar
mvn install:install-file -Dfile=filegdb-0.69-4.3-2.13.jar \
  -DgroupId=com.esri -DartifactId=filegdb -Dversion=0.69-4.3-2.13 -Dpackaging=jar
```

Building them from source works too:

```shell
# from the directory holding the sibling checkouts
(cd WebMercator && mvn -P scala-2.13 clean install)
(cd spark-shp   && mvn -P spark-4.3,esri-geometry-github clean install)
(cd FileGDB     && mvn -P spark-4.3 clean install)
```

On [SpatialBench](https://sedona.apache.org/spatialbench/queries/) Q1/Q2, Spark 4.3 is 0–19% faster than 3.5.9 for the same library code. At
600M trips it matches DuckDB on one machine (see
[`benchmarks/spatialbench`](benchmarks/spatialbench/README.md), which covers all 12 queries).

`esri-geometry-github` has to be named explicitly: Maven deactivates every `activeByDefault`
profile the moment `-P` selects one, and that is the profile supplying the geometry API
coordinates.

Add the shaded jar to the session and register the SQL functions:

```python
# Spark 4.x: the release asset is geofunctions-0.38-2.13.jar (a local -P spark-4.3 build names it geofunctions-0.38.jar)
spark = SparkSession.builder.config("spark.jars", "<path-to>/geofunctions-0.38-2.13.jar").getOrCreate()

from geofunctions import st_register_functions
st_register_functions()   # only needed for the ST_*/QR_* SQL names
```

The DataFrame API (`from geofunctions import st_point, ...`) does not need registration.

### H3

H3 functions additionally need the Uber `h3` jar — it is a `provided` dependency, is not
bundled in the shaded jar, and ArcGIS Pro does not ship it either, so every environment has to
add it. It has to reach **both the driver and the executors**: the expressions evaluate on the
executors, so a driver-only classpath entry works under `local` and then fails everywhere else.
Without the jar the session starts fine and only fails once an H3 function actually runs, with
`NoClassDefFoundError: com/uber/h3core/H3Core`.

`pom.xml` owns the version — read it out of the `com.uber:h3` dependency rather than trusting
a number copied into this file, then download the jar directly (no build tool needed):

```shell
# whatever <version> sits under the com.uber:h3 dependency in pom.xml
H3_VER=4.4.0
curl -O "https://repo1.maven.org/maven2/com/uber/h3/${H3_VER}/h3-${H3_VER}.jar"
```

With Maven installed, `mvn dependency:get -Dartifact=com.uber:h3:${H3_VER}` is equivalent and
leaves it under `~/.m2/repository/com/uber/h3/${H3_VER}/`. Either way, list it next to the
shaded jar — `spark.jars` covers the driver and the executors in one go:

```python
import os
from glob import glob

gf_jar = os.path.expanduser("~/geofunctions-0.38.jar")
# glob, so the h3 version stays wherever pom.xml put it
h3_jar = glob(os.path.expanduser("~/.m2/repository/com/uber/h3/*/h3-*.jar"))[0]
spark = SparkSession.builder.config("spark.jars", f"{gf_jar},{h3_jar}").getOrCreate()
```

`os.path.expanduser` is not decoration: nothing in the Python or Spark launch path expands
`~`, so a literal tilde is resolved against the working directory and the jar silently is not
found.

### If you wire the session up by hand

Prefer the `spark.jars` recipe above. If you set `PYSPARK_SUBMIT_ARGS` yourself, three things
bite:

- `--driver-class-path` is separated by `os.pathsep`, `--jars` is comma-separated. Using
  colons for `--jars` fails the launch with `Java gateway process exited before sending its
  port number`.
- pyspark parses the variable with `shlex.split()` in POSIX mode, which **eats backslashes**.
  On Windows — the ArcGIS Pro platform — `C:\Users\me\geofunctions-0.38.jar` reaches the JVM
  as `C:Usersmegeofunctions-0.38.jar`, and a path containing a space splits into two
  arguments. Use forward slashes (`Path(p).as_posix()`) or `shlex.quote`.
- `PYSPARK_PYTHON` should point at your interpreter, or the worker picks whatever `python3`
  is first on `PATH` and fails with `PYTHON_VERSION_MISMATCH`.

### Functions

**Note:** Look at the notebooks for example usages of the functions. See [functions.md](functions.md) for the full reference. The reproducible [QR cell-size benchmark](benchmarks/README.md) selects and times a grid for the 5-million-point world time-zone join.

**Null inputs produce null.** Every function returns null when any argument is null, except
`st_isempty`, which treats a null geometry as empty. See
[functions.md](functions.md#null-inputs-produce-null).

**`dist` inflates the cell, not the geometry.** Cell `(q, r)` spans
`[q*cell - dist, (q+1)*cell + dist]` on each axis, so adjacent cells overlap by `2 * dist`
and a geometry near a boundary lands in more than one cell.

**QR arguments are checked before allocation.** `cell` must be finite and positive, and
`dist` must be finite and non-negative. A QR key stores signed 32-bit `q` and `r` indices;
requests outside that range, or requests producing more cells than a Spark array can hold,
fail with an actionable error instead of wrapping to another key.

- clip_line(line, cell): Splits a line into per-cell segments, returning array of (q, r, l).
- clip_line_dist(line, cell, dist=0.0): Same as clip_line with cell padding.
- qr_asgeom(qr, cell, dist=0.0): Returns the rectangle geometry of the quad region.
- qr_contains_geom(qr, cell, geom): True if the QR cell fully contains the geometry.
- qr_envp(geom, cell, dist=0.0): Returns list of qr,envelope of the quad region.
- qr_fromxy(x, y, cell): Returns the quad region containing the point (x, y).
- qr_geom(geom, cell, dist=0.0, wkid=-1): Returns the intersections of the quad regions and the geometry.
- qr_intersect(lhs, rhs, cell): Returns true when matching QR groups overlap in their canonical lower-left cell.
- qr_cells(geom, cell, dist=0.0): Returns the qr_list cells of one WKT/WKB geometry on the driver, for pruning a QR-sorted table.
- qr_list(geom, cell, dist=0.0): Returns a list of quad regions.
- st_astext(geom): Returns the WKT representation of the geometry.
- st_box(x, y, h, v=None): Returns a rectangle centered on x/y, width 2*h and height 2*v (v defaults to h).
- st_buffer(geom, distance, num_vertices=36, wkid=-1): Returns a buffer around the geometry.
- st_cell(x, y, w, h=None): Returns a rectangle with its lower-left corner at x/y (h defaults to w).
- st_centroid(geom): Returns the centroid of the geometry.
- st_contains(lhs, rhs, wkid=-1): Returns true if the lhs geometry contains the rhs geometry.
- st_distance(lhs, rhs): Returns the distance between two geometries.
- st_dwithin(lhs, rhs, dist): Returns true if the planar distance between two geometries is at most dist.
- st_euclid(x1, y1, x2, y2): Returns the euclidean distance between two points.
- st_fromtext(text): Returns a geometry from a WKT representation.
- st_haversine(lon1, lat1, lon2, lat2): Returns the haversine distance between two points.
- st_intersection(lhs, rhs, wkid=-1): Returns the intersection of two geometries.
- st_isempty(geom): Returns true if the geometry is empty.
- st_lattoy(lat): Returns the y mercator coordinate of a latitude.
- st_line(x1, y1, x2, y2): Returns a line.
- st_lontox(lon): Returns the x mercator coordinate of a longitude.
- st_manhattan(x1, y1, x2, y2): Returns the Manhattan distance between two points.
- st_point(x,y): Returns a point.
- st_polygon(*points): Returns a polygon.
- st_polyline(*points): Returns a polyline.
- st_qtox(q, cell, dist=0.0): Returns the x coordinate of a q value.
- st_rect(x1, y1, x2, y2): Returns a rectangle geometry.
- st_rtoy(r, cell, dist=0.0): Returns the y coordinate of an r value.
- st_within_mask(points, polygon, wkid=-1): Returns st_within(points[i], polygon) for each point, reading the polygon once.
- st_x(geom, index=0): Returns the x coordinate of a geometry at an index.
- st_xtolon(x): Returns the longitude of an x coordinate.
- st_xtoq(x, cell): Returns the q value of an x coordinate.
- st_xy(geom, index=0): Returns the x and y coordinates of a geometry at an index.
- st_y(geom, index=0): Returns the y coordinate of a geometry at an index.
- st_ytolat(y): Returns the latitude of a y coordinate.
- st_ytor(y, cell): Returns the r value of a y coordinate.

The list above is a subset. Other areas covered in [functions.md](functions.md):

- **H3**: h3_latlng_to_cell, h3_cell_to_boundary (needs the Uber `h3` jar on the classpath).
- **Explode helpers**: qr_geom_explode, qr_envp_explode, qr_list_explode, st_dump_explode — wrap `explode()` around the array-returning functions.
- **Spatial joins**: join_qr(lhs, rhs, cell, ..., broadcast=None), also usable as `df.join_qr(...)`. Pass `broadcast="lhs"` or `"rhs"` when that side is small.
- **Nearest neighbours**: knn_join(lhs, rhs, k, lhs_geom, rhs_geom), also usable as `df.knn_join(...)`. Exact k nearest rhs rows per lhs row; rhs is broadcast.
- **Aggregation**: st_union_col, qr_count, pairwise_dissolve.
- **GDB decoding**: gdb_point, gdb_multipoint2, gdb_multipointM, gdb_multipointZ, gdb_polyline2, gdb_polylineM, gdb_polylineZ, gdb_polygon2, gdb_polygonM, gdb_polygonZ — turn the `Shape` struct from the `gdb` reader into WKB, for xy, xyz, xym and xyzm layers.
- **ArcGIS bridge**: to_spark, to_feature_class, to_feature_table.
- **SQL**: st_register_functions() registers the `ST_*`/`QR_*`/`H3_*` names for Spark SQL.

### Running the notebooks outside ArcGIS Pro

All 63 notebooks were run with local PySpark 3.5.9, shapely 2.1.2 and geopandas 1.1.1. Each run adds a first cell that starts a `SparkSession` with the jar and H3, and skips the `spark_esri` cells. 45 notebooks pass. The other 18 need something outside this library:

- ArcGIS Pro (`arcpy` or `spark_esri`): 00_Spark_Start, GeoFunctions, ParquetToFeatureClass, PointNearPoint3, SparkToFeatureClass.
- Extra Python packages: AI_With_PySpark (plotly), FileGDBRead and FileGDBWrite (GDAL), GWR and WeightedStandardDistance (statsmodels), Optuna and UNet_Optuna (optuna), Pipelines_Pipelines (sparkgeo), Tanks (torch).
- Your own data: GridSearch and OneHotEncoding (`your_data.csv`), OneMillionText (`~/data/OneMillionRows.csv`).
- StandardDist is a code fragment. It uses a `df` that you supply.

`toPandas()` returns WKB as `bytearray`, and shapely 2.1 `from_wkb` accepts only `bytes`. The notebooks convert first: `gp.GeoSeries.from_wkb(pdf.geometry.map(bytes, na_action="ignore"))`. Use the same conversion in your own code.
