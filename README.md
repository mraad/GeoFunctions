# GeoFunctions

This is a small collection of PySpark functions useful for working with geospatial data using the [Esri Geometry Library](https://github.com/Esri/geometry-api-java).
It is typically used within an ArcGIS Pro conda environment, and the spark engine is exposed using the [Spark Esri](https://github.com/mraad/spark-esri) package.

NOTE: This works in Pro up to version 3.5. This does NOT work in Pro 3.6 (yet).

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
pip install --no-deps <path-to>/geofunctions-0.28-py3-none-any.whl
```

Optional packages to install:

```shell
pip install geopandas mapclassify folium xyzservices duckdb fastparquet
```

### Standalone PySpark

The wheel also works outside ArcGIS Pro against a plain PySpark install:

```shell
pip install pyspark==3.5.9   # 3.5.4 on Windows
```

Add the shaded jar to the session and register the SQL functions:

```python
spark = SparkSession.builder.config("spark.jars", "<path-to>/geofunctions-0.28.jar").getOrCreate()

from geofunctions import st_register_functions
st_register_functions()   # only needed for the ST_*/QR_* SQL names
```

The DataFrame API (`from geofunctions import st_point, ...`) does not need registration.
H3 functions additionally need the Uber `h3` jar on the classpath — it is a `provided`
dependency and is not bundled.

### Functions

**Note:** Look at the notebooks for example usages of the functions. See [functions.md](functions.md) for the full reference.

**Null inputs are the caller's responsibility.** These functions skip per-row null checks
for speed, so filter nulls out before calling — a null is silently read as `0`/`-1` and
produces a plausible but wrong result rather than an error. See
[functions.md](functions.md#null-inputs-are-the-callers-responsibility).

**`dist` inflates the cell, not the geometry.** Cell `(q, r)` spans
`[q*cell - dist, (q+1)*cell + dist]` on each axis, so adjacent cells overlap by `2 * dist`
and a geometry near a boundary lands in more than one cell.

- clip_line(line, cell): Splits a line into per-cell segments, returning array of (q, r, l).
- clip_line_dist(line, cell, dist=0.0): Same as clip_line with cell padding.
- qr_asgeom(qr, cell, dist=0.0): Returns the rectangle geometry of the quad region.
- qr_contains_geom(qr, cell, geom): True if the QR cell fully contains the geometry.
- qr_envp(geom, cell, dist=0.0): Returns list of qr,envelope of the quad region.
- qr_fromxy(x, y, cell): Returns the quad region containing the point (x, y).
- qr_geom(geom, cell, dist=0.0, wkid=-1): Returns the intersections of the quad regions and the geometry.
- qr_intersect(lhs, rhs, cell): Returns the lower left status of two quad regions.
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
