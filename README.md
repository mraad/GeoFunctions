# GeoFunctions

This is a small collection of PySpark functions useful for working with geospatial data using the [Esri Geometry API for Java](https://github.com/Esri/geometry-api-java.git).
It is typically used within an ArcGIS Pro conda environment, and the spark engine is exposed using the [Spark Esri](https://github.com/mraad/spark-esri) package.

NOTE: This works in Pro up to version 3.5. This does NOT work in Pro 3.6 (yet).

NOTE: This targets **Spark 3.5.9** and Scala 2.12. Java 11 is the build baseline;
Java 17 is also supported and smoke-tested. Java 21 and Spark 4.0 are not supported yet.

### Core Geometry Dependency

The Scala implementation depends on
[`com.esri.geometry:esri-geometry-api:2.2.5-SNAPSHOT`](https://github.com/Esri/geometry-api-java.git).
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
pip install --no-deps <path-to>/geofunctions-0.31-py3-none-any.whl
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

The wheel also works outside ArcGIS Pro against a plain PySpark install. Use 3.5.9 — it is
what the jar is built and tested against:

```shell
pip install pyspark==3.5.9
```

The jar is compiled with Spark `provided`-scope, so it will load on any 3.5.x host, but
3.5.9 is the supported combination.

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

Java 21 is not supported by Spark 3.5.9. Official Java 21 support starts with Spark 4.0,
which also requires Scala 2.13; adopting it therefore needs a coordinated Spark/Scala
migration rather than only changing `JAVA_HOME`.

Add the shaded jar to the session and register the SQL functions:

```python
spark = SparkSession.builder.config("spark.jars", "<path-to>/geofunctions-0.31.jar").getOrCreate()

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

gf_jar = os.path.expanduser("~/geofunctions-0.31.jar")
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
  On Windows — the ArcGIS Pro platform — `C:\Users\me\geofunctions-0.31.jar` reaches the JVM
  as `C:Usersmegeofunctions-0.31.jar`, and a path containing a space splits into two
  arguments. Use forward slashes (`Path(p).as_posix()`) or `shlex.quote`.
- `PYSPARK_PYTHON` should point at your interpreter, or the worker picks whatever `python3`
  is first on `PATH` and fails with `PYTHON_VERSION_MISMATCH`.

### Functions

**Note:** Look at the notebooks for example usages of the functions. See [functions.md](functions.md) for the full reference.

**Null inputs are generally the caller's responsibility.** Most functions skip per-row null
checks for speed, so filter or coalesce nulls before calling; otherwise a null may be read as
`0`/`-1` and produce a plausible but wrong result. `qr_list` and `qr_intersect` are explicit
exceptions: they propagate a null required argument, and `join_qr` excludes rows whose QR or
required geometry fields are null. See
[functions.md](functions.md#null-inputs-are-the-callers-responsibility).

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
- qr_list(geom, cell, dist=0.0): Returns a list of quad regions.
- st_astext(geom): Returns the WKT representation of the geometry.
- st_box(x, y, h, v=None): Returns a rectangle centered on x/y, width 2*h and height 2*v (v defaults to h).
- st_buffer(geom, distance, num_vertices=36, wkid=-1): Returns a buffer around the geometry.
- st_cell(x, y, w, h=None): Returns a rectangle with its lower-left corner at x/y (h defaults to w).
- st_centroid(geom): Returns the centroid of the geometry.
- st_contains(lhs, rhs, wkid=-1): Returns true if the lhs geometry contains the rhs geometry.
- st_distance(lhs, rhs): Returns the distance between two geometries.
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
- **Spatial joins**: join_qr(lhs, rhs, cell, ...), also usable as `df.join_qr(...)`.
- **Aggregation**: st_union_col, qr_count, pairwise_dissolve.
- **GDB decoding**: gdb_polyline2, gdb_polygon2, gdb_polygonM, gdb_polygonZ — turn the `Shape` struct from the `gdb`/`shp` readers into WKB.
- **ArcGIS bridge**: to_spark, to_feature_class, to_feature_table.
- **SQL**: st_register_functions() registers the `ST_*`/`QR_*`/`H3_*` names for Spark SQL.
