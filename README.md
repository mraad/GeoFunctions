# GeoFunctions

This is a small collection of PySpark functions useful for working with geospatial data using the [Esri Geometry Library](https://github.com/Esri/geometry-api-java).
It is typically used within an ArcGIS Pro conda environment, and the spark engine is exposed using the [Spark Esri]
(https://github.com/mraad/spark-esri) package.

### Create New Conda Environment

Using ArcGIS Python Command Prompt:

```shell
cd %HOMEPATH%
conda create -n geofunctions --yes --clone arcgispro-py3
proswap geofunctions
git clone https://github.com/mraad/spark-esri.git
cd spark-esri
pip install .
```

Install the geofunctions package using:

```shell
pip install geofunctions-0.5-py3-none-any.whl
```

Optional packages to install:

```shell
pip install geopandas mapclassify folium xyzservices
```

### Functions

**Note:** Look at the notebooks for example usages of the functions.

- qr_clip(geom, cell, dist=0.0): Returns the intersections of the quad regions and the geometry.
- qr_envp(geom, cell, dist=0.0): Returns list of qr,envelope of the quad region.
- qr_fromxy(x, y, cell): Returns the quad region containing the point (x, y).
- qr_intersect(lhs, rhs, cell): Returns the lower left status of two quad regions.
- qr_list(geom, cell, dist=0.0): Returns a list of quad regions.
- st_astext(geom): Returns the WKT representation of the geometry.
- st_box(x, y, width, height): Returns a rectangle.
- st_buffer(geom, dist, num_vertices=36): Returns a buffer around the geometry.
- st_cell(x, y, cell): Return a rectangle of a cell.
- st_centroid(geom): Returns the centroid of the geometry.
- st_contains(lhs, rhs): Returns true if the lhs geometry contains the rhs geometry.
- st_distance(lhs, rhs): Returns the distance between two geometries.
- st_euclid(x1, y1, x2, y2): Returns the euclidean distance between two points.
- st_fromtext(wkt): Returns a geometry from a WKT representation.
- st_haversine(x1, y1, x2, y2): Returns the haversine distance between two points.
- st_intersection(lhs, rhs): Returns the intersection of two geometries.
- st_isempty(geom): Returns true if the geometry is empty.
- st_lattoy(lat): Returns the y mercator coordinate of a latitude.
- st_line(x1, y1, x2, y2): Returns a line.
- st_lontox(lon): Returns the x mercator coordinate of a longitude.
- st_manhattan(x1, y1, x2, y2): Returns the Manhattan distance between two points.
- st_point(x,y): Returns a point.
- st_polygon(*point): Returns a polygon.
- st_polyline(*point): Returns a polyline.
- st_qtox(q, cell, dist=0.0): Returns the x coordinate of a q value.
- st_rect(x1, y1, x2, y2): Returns a rectangle.
- st_rtoy(r, cell, dist=0.0): Returns the y coordinate of an r value.
- st_x(geom, index=0): Returns the x coordinate of a geometry at an index.
- st_xtolon(x): Returns the longitude of an x coordinate.
- st_xtoq(x, cell): Returns the q value of an x coordinate.
- st_xy(geom, index=0): Returns the x and y coordinates of a geometry at an index.
- st_y(geom, index=0): Returns the y coordinate of a geometry at an index.
- st_ytolat(y): Returns the latitude of a y coordinate.
- st_ytor(y, cell): Returns the r value of a y coordinate.

### On MacOS (Optional)

- https://spark.apache.org/docs/latest/ml-linalg-guide.html
- https://github.com/luhenry/netlib

```shell
OPENBLAS="$(brew --prefix openblas)" pip install PACKAGENAME
```

### References

- https://towardsdatascience.com/how-to-create-voronoi-regions-with-geospatial-data-in-python-adbb6c5f2134
- https://medium.com/analytics-vidhya/create-voronoi-regions-with-python-28720b9c70d8
- https://github.com/WZBSocialScienceCenter/geovoronoi
