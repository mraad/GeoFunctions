# GeoFunctions - Complete Function Reference

A comprehensive Spark-based geospatial library providing SQL functions for geometry operations, spatial indexing, and data transformations.

---

## Table of Contents

1. [Registration & Setup](#registration--setup)
2. [H3 Indexing](#h3-indexing)
3. [Geometry Construction](#geometry-construction)
4. [Geometry Conversion & Serialization](#geometry-conversion--serialization)
5. [Coordinate Transformations](#coordinate-transformations)
6. [Quadtree Grid Operations](#quadtree-grid-operations)
7. [Point & Coordinate Extraction](#point--coordinate-extraction)
8. [Distance Calculations](#distance-calculations)
9. [Geometry Properties](#geometry-properties)
10. [Spatial Relationships](#spatial-relationships)
11. [Geometry Manipulation](#geometry-manipulation)
12. [Projection & Reference Systems](#projection--reference-systems)
13. [Geometry Collections & Aggregation](#geometry-collections--aggregation)
14. [GDB Format Conversion](#gdb-format-conversion)
15. [Line Clipping Operations](#line-clipping-operations)
16. [Spatial Joins & DataFrames](#spatial-joins--dataframes)
17. [ArcGIS Integration](#arcgis-integration)

---

## Registration & Setup

### st_register_functions()
Registers all ST_XXX SQL functions with the active Spark context, making them available for use in Spark SQL queries.

**Returns:** None

---

## H3 Indexing

Hierarchical hexagonal indexing for global spatial coverage.

**Requires the Uber `h3` jar on the driver *and* the executors.** It is a `provided`
dependency, is not bundled in the shaded jar, and ArcGIS Pro does not ship it either — every
environment has to add it. See [the README](README.md#standalone-pyspark) for how to obtain it
and put it on both classpaths.

Missing, it does not fail at session start — only when an H3 function first executes, with
`NoClassDefFoundError: com/uber/h3core/H3Core`. `H3Instance` is a lazy `val` evaluated inside
expression `eval`, which runs on the executors, so a driver-only classpath entry is not enough
on any master other than `local`.

### h3_cell_to_boundary(cell)
Convert an H3 cell identifier to its boundary geometry.

**Parameters:**
- `cell` (Union[Column, str]): H3 cell identifier or column name

**Returns:** Column with boundary geometry (alias: "geom")

### h3_latlng_to_cell(lat, lng, res)
Convert latitude and longitude coordinates to an H3 cell.

**Parameters:**
- `lat` (Union[Column, str, float]): Latitude value or column name
- `lng` (Union[Column, str, float]): Longitude value or column name
- `res` (Union[Column, str, int]): H3 resolution level (0-15)

**Returns:** Column with H3 cell identifier (alias: "cell")

---

## Geometry Construction

Functions for creating basic geometric shapes.

### st_point(x, y)
Create a point geometry from x and y coordinates.

**Parameters:**
- `x` (Union[Column, str]): X coordinate or column name
- `y` (Union[Column, str]): Y coordinate or column name

**Returns:** Column with point geometry (alias: "geom")

### st_line(x1, y1, x2, y2)
Create a polyline from two coordinate pairs.

**Parameters:**
- `x1, y1` (Union[Column, str]): First point coordinates
- `x2, y2` (Union[Column, str]): Second point coordinates

**Returns:** Column with polyline geometry (alias: "geom")

### st_rect(x1, y1, x2, y2)
Create a rectangular polygon from two opposite corner coordinates.

**Parameters:**
- `x1, y1` (Union[Column, str]): First corner coordinates
- `x2, y2` (Union[Column, str]): Opposite corner coordinates

**Returns:** Column with rectangular polygon (alias: "geom")

### st_cell(x, y, w, h)
Create a rectangular polygon with lower-left corner at x/y with width w and height h.

**Parameters:**
- `x, y` (Union[Column, str]): Lower-left corner coordinates
- `w` (Union[Column, str, float]): Width
- `h` (Union[Column, str, float], Optional): Height (defaults to width if not provided)

**Returns:** Column with rectangular polygon (alias: "geom")

### st_box(x, y, h, v)
Create a rectangular polygon centered at x/y with horizontal and vertical padding.

**Parameters:**
- `x, y` (Union[Column, str]): Center coordinates
- `h` (Union[Column, str]): Horizontal padding
- `v` (Union[Column, str], Optional): Vertical padding (defaults to h if not provided)

**Returns:** Column with rectangular polygon (alias: "geom")

### st_polyline(*points)
Convert an array of points to a polyline geometry.

**Parameters:**
- `*points` (Variable): Point geometries or array column

**Returns:** Column with polyline geometry (alias: "geom")

**Raises:** ValueError if no points are provided

### st_multipoint(*points)
Convert an array of points to a multipoint geometry.

**Parameters:**
- `*points` (Variable): Point geometries or array column

**Returns:** Column with multipoint geometry (alias: "geom")

**Raises:** ValueError if no points are provided

### st_polyline2(xy)
Convert an XY coordinate array to a polyline geometry.

**Parameters:**
- `xy` (Union[Column, str]): Array of [x, y] coordinates or column name

**Returns:** Column with polyline geometry (alias: "geom")

### st_polygon(*points)
Convert an array of points to a polygon geometry.

**Parameters:**
- `*points` (Variable): Point geometries or array column

**Returns:** Column with polygon geometry (alias: "geom")

**Raises:** ValueError if no points are provided

### st_polygon2(xy)
Convert an XY coordinate array to a polygon geometry.

**Parameters:**
- `xy` (Union[Column, str]): Array of [x, y] coordinates or column name

**Returns:** Column with polygon geometry (alias: "geom")

---

## Geometry Conversion & Serialization

Functions for converting between geometry formats and text representations.

### st_astext(geom)
Convert a geometry to WKT (Well-Known Text) string representation.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")

**Returns:** Column with WKT string (alias: "text")

### st_asgeojson(geom)
Convert a geometry to GeoJSON string representation.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")

**Returns:** Column with GeoJSON string (alias: "geojson")

### st_fromtext(text)
Create a geometry from a WKT string representation.

**Parameters:**
- `text` (Union[Column, str]): WKT string or column name (default: "text")

**Returns:** Column with geometry (alias: "geom")

---

## Coordinate Transformations

Functions for converting between coordinate systems and projections.

### st_lontox(lon)
Convert longitude to x coordinate in meters (Web Mercator projection).

**Parameters:**
- `lon` (Union[Column, str]): Longitude value or column name (default: "lon")

**Returns:** Column with x coordinate in meters (alias: "x")

### st_lattoy(lat)
Convert latitude to y coordinate in meters (Web Mercator projection).

**Parameters:**
- `lat` (Union[Column, str]): Latitude value or column name (default: "lat")

**Returns:** Column with y coordinate in meters (alias: "y")

### st_xtolon(x)
Convert x coordinate in meters to longitude (Web Mercator projection).

**Parameters:**
- `x` (Union[Column, str]): X coordinate in meters or column name (default: "x")

**Returns:** Column with longitude value (alias: "lon")

### st_ytolat(y)
Convert y coordinate in meters to latitude (Web Mercator projection).

**Parameters:**
- `y` (Union[Column, str]): Y coordinate in meters or column name (default: "y")

**Returns:** Column with latitude value (alias: "lat")

---

## Quadtree Grid Operations

Functions for quadtree-based spatial indexing and grid operations.

A **QR** is a grid cell packed into a single `bigint`: the column index `q` in the high
32 bits, the row index `r` in the low 32 bits. `q = floor(x / cell)`, `r = floor(y / cell)`.

**The `dist` parameter inflates the cell, not the geometry.** Cell `(q, r)` spans
`[q*cell - dist, (q+1)*cell + dist]` on each axis, so neighbouring cells overlap by
`2 * dist` and a geometry near a boundary belongs to more than one cell. This is what
makes a QR equi-join find pairs that straddle a cell edge — set `dist` to the largest
search distance you care about.

Every function that takes `dist` follows this convention, so `qr_count`, `qr_list`,
`qr_envp`, `qr_geom` and `qr_envp_geom` agree on the candidate cell set for a given
`(cell, dist)`.

### st_lontoq(lon, cell)
Convert longitude to a q value (column index in quadtree grid).

**Parameters:**
- `lon` (Union[Column, str]): Longitude value or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name

**Returns:** Column with q value (alias: "q")

### st_lattor(lat, cell)
Convert latitude to an r value (row index in quadtree grid).

**Parameters:**
- `lat` (Union[Column, str]): Latitude value or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name

**Returns:** Column with r value (alias: "r")

### st_qtox(q, cell, dist)
Convert q value (column index) to x coordinate in meters.

**Parameters:**
- `q` (Union[Column, str]): Q value (column index) or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding/offset in meters (default: 0.0)

**Returns:** Column with x coordinate in meters (alias: "x")

### st_rtoy(r, cell, dist)
Convert r value (row index) to y coordinate in meters.

**Parameters:**
- `r` (Union[Column, str]): R value (row index) or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding/offset in meters (default: 0.0)

**Returns:** Column with y coordinate in meters (alias: "y")

### st_xtoq(x, cell)
Convert x meters to a q value (column index in quadtree grid).

**Parameters:**
- `x` (Union[Column, str]): X coordinate in meters or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name

**Returns:** Column with q value (alias: "q")

### st_ytor(y, cell)
Convert y meters to an r value (row index in quadtree grid).

**Parameters:**
- `y` (Union[Column, str]): Y coordinate in meters or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name

**Returns:** Column with r value (alias: "r")

### qr_envp(geom, cell, dist)
Compute the qr/envp (quadtree envelope) of a geometry.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding in meters (default: 0.0)

**Returns:** Column with array of qr/envp values

### qr_envp_geom(geom, cell, dist, wkid)
Compute the qr/envp/geom of a geometry (includes clipped geometry for each QR cell).

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding in meters (default: 0.0)
- `wkid` (Union[Column, str, int]): Spatial reference ID (default: -1)

**Returns:** Column with array of qr/envp/geom structures

### qr_envp_explode(geom, cell, dist)
Explode the qr/envp of a geometry into separate rows.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding in meters (default: 0.0)

**Returns:** Column with exploded qr (alias: "qr")

### qr_envp_geom_explode(geom, cell, dist, wkid)
Explode the qr/envp/geom of a geometry into separate rows.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding in meters (default: 0.0)
- `wkid` (Union[Column, str, int]): Spatial reference ID (default: -1)

**Returns:** Column with exploded qr (alias: "qr")

### qr_geom(geom, cell, dist, wkid)
Compute the list of qr/geom (geometry clipped to each QR cell).

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding in meters (default: 0.0)
- `wkid` (Union[Column, str, int]): Spatial reference ID (default: -1)

**Returns:** Column with array of qr/geom structures

### qr_geom_explode(geom, cell, dist, wkid)
Explode the qr/geom of a geometry into separate rows.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding in meters (default: 0.0)
- `wkid` (Union[Column, str, int]): Spatial reference ID (default: -1)

**Returns:** Column with exploded qr (alias: "qr")

### qr_list(geom, cell, dist)
Compute the qr list of a geometry.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding in meters (default: 0.0)

**Returns:** Column with array of qr values

### qr_list_explode(geom, cell, dist)
Explode the qr list of a geometry into separate rows.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding in meters (default: 0.0)

**Returns:** Column with exploded qr (alias: "qr")

### qr_count(geom, cell, dist)
Return the number of QR cells that the geometry covers.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding in meters (default: 0.0)

**Returns:** Column with count of QR cells

### qr_intersect(lhs, rhs, cell)
Check if the qr/envp of two geometries intersect.

**Parameters:**
- `lhs` (Union[Column, str]): Left hand side qr/envp or column name
- `rhs` (Union[Column, str]): Right hand side qr/envp or column name
- `cell` (Union[Column, int, float]): Cell size in meters

**Returns:** Boolean column indicating if qr/envp intersect

### qr_fromxy(x, y, cell)
Compute the qr value for a given x/y coordinate.

**Parameters:**
- `x` (Union[Column, str]): X coordinate or column name
- `y` (Union[Column, str]): Y coordinate or column name
- `cell` (Union[Column, str, int, float]): Cell size in meters or column name

**Returns:** Column with qr value (alias: "qr")

### qr_fromgeom(geom, cell)
Compute the qr value for a geometry's lower-left envelope point.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name
- `cell` (Union[Column, str, int, float]): Cell size in meters or column name

**Returns:** Column with qr value (alias: "qr")

### qr_asgeom(qr, cell, dist)
Compute the geometry of a qr (returns the QR cell as a rectangle).

**Parameters:**
- `qr` (Union[Column, str]): QR code or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `dist` (Union[Column, str, float, int]): Cell padding in meters (default: 0.0)

**Returns:** Column with qr geometry (alias: "geom")

### qr_contains_geom(qr, cell, geom)
Check if the geometry is fully inside a QR cell.

**Parameters:**
- `qr` (Union[Column, str]): QR code or column name
- `cell` (Union[Column, str, float, int]): Cell size in meters or column name
- `geom` (Union[Column, str]): Geometry or column name

**Returns:** Boolean column indicating if qr contains geom

---

## Point & Coordinate Extraction

Functions for extracting coordinates and points from geometries.

### st_x(geom, index)
Get the x coordinate of a geometry at a point index.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")
- `index` (Union[Column, str, int]): Point index (default: 0)

**Returns:** Column with x coordinate (alias: "x")

### st_y(geom, index)
Get the y coordinate of a geometry at a point index.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")
- `index` (Union[Column, str, int]): Point index (default: 0)

**Returns:** Column with y coordinate (alias: "y")

### st_xy(geom, index)
Get the x/y coordinate of a geometry at a point index.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")
- `index` (Union[Column, str, int]): Point index (default: 0)

**Returns:** Column with x/y coordinate as array (alias: "xy")

### st_centroid(geom)
Get the centroid of a geometry.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")

**Returns:** Column with centroid as point (alias: "geom")

### st_centroid_xy(geom)
Get the centroid XY coordinates of a geometry.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")

**Returns:** Column with centroid as XY array (alias: "xy")

### st_extent(geom)
Get the extent (bounding box) of a geometry as [xmin, ymin, xmax, ymax].

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")

**Returns:** Column with array of [xmin, ymin, xmax, ymax] (alias: "extent")

---

## Distance Calculations

Functions for calculating distances between points and geometries.

### st_euclid(x1, y1, x2, y2)
Compute the Euclidean distance between two points.

Formula: sqrt((x2-x1)² + (y2-y1)²)

**Parameters:**
- `x1, y1` (Union[Column, str]): First point coordinates
- `x2, y2` (Union[Column, str]): Second point coordinates

**Returns:** Column with Euclidean distance

### st_manhattan(x1, y1, x2, y2)
Compute the Manhattan distance between two points.

Formula: |x2-x1| + |y2-y1|

**Parameters:**
- `x1, y1` (Union[Column, str, float, int]): First point coordinates
- `x2, y2` (Union[Column, str, float, int]): Second point coordinates

**Returns:** Column with Manhattan distance

### st_haversine(lon1, lat1, lon2, lat2)
Compute the Haversine distance between two points on a sphere.

The great-circle distance between two points on Earth given their longitudes and latitudes.

**Parameters:**
- `lon1, lat1` (Union[Column, str, float, int]): First point coordinates
- `lon2, lat2` (Union[Column, str, float, int]): Second point coordinates

**Returns:** Column with Haversine distance in meters

### st_distance(lhs, rhs)
Compute the distance between two geometries.

**Parameters:**
- `lhs` (Union[Column, str]): Left hand side geometry or column name (default: "lgeom")
- `rhs` (Union[Column, str]): Right hand side geometry or column name (default: "rgeom")

**Returns:** Column with distance value

---

## Geometry Properties

Functions for calculating and analyzing geometry properties.

### st_area(geom)
Compute the area of a geometry.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")

**Returns:** Column with area value (alias: "area")

### st_length(geom)
Compute the length of a geometry.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")

**Returns:** Column with length value (alias: "length")

### st_isempty(geom)
Check if a geometry is empty.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")

**Returns:** Boolean column indicating if geometry is empty

---

## Spatial Relationships

Functions for testing spatial relationships between geometries.

### st_intersects(lhs, rhs, wkid)
Check if two geometries intersect.

**Parameters:**
- `lhs` (Union[Column, str]): Left hand side geometry or column name (default: "lgeom")
- `rhs` (Union[Column, str]): Right hand side geometry or column name (default: "rgeom")
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Boolean column indicating if geometries intersect

### st_intersects_box(geom, xmin, ymin, xmax, ymax)
Check if a geometry intersects with a bounding box.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")
- `xmin, ymin, xmax, ymax` (Union[Column, int, float]): Bounding box coordinates (defaults: -180, -90, 180, 90)

**Returns:** Boolean column indicating if geometry intersects the box

### st_intersection(lhs, rhs, wkid)
Compute the intersection of two geometries.

**Parameters:**
- `lhs` (Union[Column, str]): Left hand side geometry or column name (default: "lgeom")
- `rhs` (Union[Column, str]): Right hand side geometry or column name (default: "rgeom")
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Column with intersection geometry (alias: "geom")

### st_overlaps(lhs, rhs, wkid)
Check if two geometries overlap.

Geometries overlap if they have some but not all points in common, are of the same dimension, and their interior intersection is non-empty.

**Parameters:**
- `lhs` (Union[Column, str]): Left hand side geometry or column name (default: "lgeom")
- `rhs` (Union[Column, str]): Right hand side geometry or column name (default: "rgeom")
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Boolean column indicating if geometries overlap

### st_contains(lhs, rhs, wkid)
Check if lhs geometry contains rhs geometry.

**Parameters:**
- `lhs` (Union[Column, str]): Left hand side (container) geometry or column name (default: "lgeom")
- `rhs` (Union[Column, str]): Right hand side (contained) geometry or column name (default: "rgeom")
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Boolean column indicating if lhs contains rhs

### st_within(lhs, rhs, wkid)
Check if lhs geometry is within rhs geometry (inverse of st_contains).

**Parameters:**
- `lhs` (Union[Column, str]): Left hand side (inner) geometry or column name (default: "lgeom")
- `rhs` (Union[Column, str]): Right hand side (outer) geometry or column name (default: "rgeom")
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Boolean column indicating if lhs is within rhs

### st_touches(lhs, rhs, wkid)
Check if lhs geometry touches rhs geometry.

Geometries touch if they have at least one point in common, but their interiors do not intersect.

**Parameters:**
- `lhs` (Union[Column, str]): Left hand side geometry or column name (default: "lgeom")
- `rhs` (Union[Column, str]): Right hand side geometry or column name (default: "rgeom")
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Boolean column indicating if geometries touch

### st_disjoint(lhs, rhs, wkid)
Check if lhs geometry is disjoint from the rhs geometry.

Geometries are disjoint if they have no point in common.

**Parameters:**
- `lhs` (Union[Column, str]): Left hand side geometry or column name (default: "lgeom")
- `rhs` (Union[Column, str]): Right hand side geometry or column name (default: "rgeom")
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Boolean column indicating if geometries are disjoint

### st_iou(lhs, rhs, wkid)
Calculate the Intersection over Union (IoU) of two geometries.

Formula: IoU = Area(intersection) / Area(union)

Commonly used in object detection and image segmentation for measuring overlap.

**Parameters:**
- `lhs` (Union[Column, str]): Left hand side geometry or column name (default: "lgeom")
- `rhs` (Union[Column, str]): Right hand side geometry or column name (default: "rgeom")
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Column with IoU value 0.0-1.0 (alias: "iou")

---

## Geometry Manipulation

Functions for transforming and modifying geometries.

### st_translate(geom, dx, dy)
Translate a geometry by dx, dy offset.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")
- `dx` (Union[Column, str, float]): X translation offset (default: 0.0)
- `dy` (Union[Column, str, float]): Y translation offset (default: 0.0)

**Returns:** Column with translated geometry (alias: "geom")

### st_buffer(geom, distance, num_vertices, wkid)
Create a buffer polygon representing all points within a given distance from the geometry.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name
- `distance` (Union[Column, str, float, int]): Buffer distance in geometry units or column name
- `num_vertices` (Union[float, int]): Number of vertices to approximate curves (default: 36)
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Column with buffered geometry (alias: "geom")

### st_convexhull(geom)
Get the convex hull of a geometry.

Returns the smallest convex polygon that contains the geometry.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")

**Returns:** Column with convex hull geometry (alias: "geom")

### st_simplify(geom, wkid)
Simplify a geometry by removing redundant vertices.

Creates a topologically equivalent geometry with fewer vertices while preserving the overall shape.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Column with simplified geometry (alias: "geom")

### st_repair(geom, wkid)
Repair a geometry by fixing topological errors.

Attempts to fix invalid geometries such as self-intersections, incorrect ring orientations, and other topological issues.

**Parameters:**
- `geom` (Union[Column, str]): Geometry or column name (default: "geom")
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Column with repaired geometry (alias: "geom")

### st_exterior_ring(geom)
Get the exterior ring of a polygon.

Extracts the outer boundary ring of a polygon, discarding any holes.

**Parameters:**
- `geom` (Union[Column, str]): Polygon geometry or column name

**Returns:** Column with exterior ring as polyline (alias: "geom")

---

## Projection & Reference Systems

Functions for converting between coordinate systems.

### st_mercator(geom)
Convert coordinates from WGS84 to Web Mercator projection (EPSG:3857).

**Parameters:**
- `geom` (Union[Column, str]): Geometry in WGS84 or column name (default: "geom")

**Returns:** Column with projected geometry in Web Mercator (alias: "geom")

### st_wgs84(geom)
Convert coordinates from Web Mercator to WGS84 projection.

**Parameters:**
- `geom` (Union[Column, str]): Geometry in Web Mercator or column name (default: "geom")

**Returns:** Column with projected geometry in WGS84 (alias: "geom")

---

## Geometry Collections & Aggregation

Functions for working with multi-part geometries and collections.

### st_dump(geom)
Dump the parts of a multi-part geometry into an array of single-part geometries.

**Parameters:**
- `geom` (Union[Column, str]): Multi-part geometry or column name (default: "geom")

**Returns:** Column with array of single-part geometries

### st_dump_explode(geom)
Explode the parts of a multi-part geometry into separate rows.

**Parameters:**
- `geom` (Union[Column, str]): Multi-part geometry or column name (default: "geom")

**Returns:** Column with exploded single-part geometry (alias: "geom")

### st_union_col(coll, wkid)
Get the union of a collection of geometries.

Combines all geometries in the collection into a single geometry.

**Parameters:**
- `coll` (Union[Column, str]): Collection/array of geometries or column name
- `wkid` (Union[Column, str, int]): Spatial reference WKID (default: -1)

**Returns:** Column with union of all geometries (alias: "geom")

---

## GDB Format Conversion

Functions for converting GDB (Geodatabase) format geometries.

### gdb_polyline(shape)
Convert a GDB shape column to a polyline.

**Parameters:**
- `shape` (Union[Column, str]): GDB shape column or column name (default: "Shape")

**Returns:** Column with polyline geometry (alias: "geom")

### gdb_polyline2(shape)
Convert a GDB shape column to a polyline (version 2).

**Parameters:**
- `shape` (Union[Column, str]): GDB shape column or column name (default: "Shape")

**Returns:** Column with polyline geometry (alias: "geom")

### gdb_polygon(shape)
Convert a GDB shape column to a polygon.

**Parameters:**
- `shape` (Union[Column, str]): GDB shape column or column name (default: "Shape")

**Returns:** Column with polygon geometry (alias: "geom")

### gdb_polygon2(shape)
Convert a GDB shape column to a polygon (version 2).

**Parameters:**
- `shape` (Union[Column, str]): GDB shape column or column name (default: "Shape")

**Returns:** Column with polygon geometry (alias: "geom")

### gdb_polygonM(shape)
Convert a GDB shape column to a polygon with M values.

**Parameters:**
- `shape` (Union[Column, str]): GDB shape column or column name (default: "Shape")

**Returns:** Column with polygon M geometry (alias: "geom")

### gdb_polygonZ(shape)
Convert a GDB shape column to a polygon with Z values.

**Parameters:**
- `shape` (Union[Column, str]): GDB shape column or column name (default: "Shape")

**Returns:** Column with polygon Z geometry (alias: "geom")

---

## Line Clipping Operations

Split a **single line segment** against the QR grid and report how much of it falls in
each cell.

Note the input type: `line` is an **array of four doubles** `[x1, y1, x2, y2]` — one
segment, *not* a geometry column. To clip a polyline, explode it into segments first.
The output is an **array of structs** `(q: int, r: int, l: double)`, one entry per cell
the segment crosses, where `l` is the clipped length inside that cell.

With `dist = 0` the cells tile the plane, so the returned lengths sum exactly to the
segment length. With `dist > 0` the padded cells overlap, so the sum exceeds it.

### clip_line(line, cell)
Split a segment across the QR grid.

**Parameters:**
- `line` (Union[Column, str]): Array of 4 doubles `[x1, y1, x2, y2]`, or column name
- `cell` (Union[Column, str, float, int]): Cell size or column name

**Returns:** Column with `array<struct<q: int, r: int, l: double>>`

**Raises:** `IllegalArgumentException` if `cell <= 0`, if the array does not hold exactly
4 elements, or if the segment spans more cells than fit in an `int`.

### st_clipline(line, cell)
Alias for `clip_line`.

### clip_line_dist(line, cell, dist)
Split a segment across the QR grid, with each cell inflated by `dist` on all four sides.

**Parameters:**
- `line` (Union[Column, str]): Array of 4 doubles `[x1, y1, x2, y2]`, or column name
- `cell` (Union[Column, str, float, int]): Cell size or column name
- `dist` (Union[Column, str, float, int]): Cell padding (default: 0.0)

**Returns:** Column with `array<struct<q: int, r: int, l: double>>`

**Raises:** `IllegalArgumentException` if `cell <= 0`, `dist < 0`, if the array does not
hold exactly 4 elements, or if the segment spans more cells than fit in an `int`.

### st_cliplinedist(line, cell, dist)
Alias for `clip_line_dist`.

---

## Spatial Joins & DataFrames

Functions for performing spatial operations on DataFrames.

### join_qr(lhs, rhs, cell, dist, qr, oper, wkid, lhs_geom, rhs_geom, acceleration)
Spatially join two DataFrames using quadtree-based spatial indexing.

Performs an efficient spatial join between two DataFrames using a quadtree (QR) indexing strategy for improved performance on large datasets.

**Parameters:**
- `lhs` (DataFrame): Left hand side dataframe
- `rhs` (DataFrame): Right hand side dataframe
- `cell` (float): QR cell size in meters
- `dist` (float): QR offset/padding distance in meters (default: 0.0)
- `qr` (str): Name of the QR field in dataframes (default: "qr")
- `oper` (str): Spatial operation ("none", "intersection", etc.) (default: "none")
- `wkid` (Union[str, int]): Spatial reference WKID (default: -1)
- `lhs_geom` (str): Name of left geometry field (default: "geom")
- `rhs_geom` (str): Name of right geometry field (default: "geom")
- `acceleration` (str): Geometry acceleration mode ("mild", "medium", "hot") (default: "mild")

**Returns:** DataFrame with joined results

### pairwise_dissolve(buf, cell, dist, wkid, cell_mul, breaker, geom_name)
Dissolve a DataFrame by cell using a progressive multi-resolution approach.

Performs iterative spatial dissolve operations with progressively increasing cell sizes. Separates polygons fully contained in a QR cell from those spanning multiple cells for efficient processing.

**Parameters:**
- `buf` (DataFrame): DataFrame with polygons to dissolve
- `cell` (float): Initial QR cell size in meters (default: 100,000.0)
- `dist` (float): QR padding distance in meters (default: 100.0)
- `wkid` (int): Spatial reference WKID (default: 3857)
- `cell_mul` (float): Cell size multiplier for each iteration (default: 10.0)
- `breaker` (int): Maximum iterations (circuit breaker) (default: 10)
- `geom_name` (str): Name of geometry column (default: "geom")

**Returns:** DataFrame with dissolved polygons

---

## ArcGIS Integration

Functions for integration with ArcGIS and geodatabase operations.

These are the only functions that require `arcpy`, `pyarrow` and `pandas`. None of the three
is a declared dependency of this package (`pyarrow` and `pandas` come in via the `jupyter`
extra); ArcGIS Pro ships all three. Every `import arcpy` here is function-local, as is the
`import pyarrow` in `to_feature_table` and `to_feature_class` (`to_spark` reaches pyarrow only
through the table `arcpy.da.TableToArrowTable` hands back); pandas arrives through
`df.toPandas()` and `tab.to_pandas()`. So the module still imports cleanly outside Pro and a
missing package surfaces at call time rather than at import time.

`to_feature_class` stamps `{"esri.encoding": "WKB", "esri.sr_wkt": ...}` field metadata on
the `SHAPE` column before writing, which is what lets ArcGIS read the WKB back as geometry.

### to_feature_table(df, feature_table_name, workspace)
Converts a Spark DataFrame to an ArcGIS feature table.

**Parameters:**
- `df` (DataFrame): Spark DataFrame to convert
- `feature_table_name` (str): Name of output feature table
- `workspace` (str): Workspace for feature table ("memory", "scratch", or geodatabase path) (default: "scratch")

**Returns:** None

### to_feature_class(df, feature_class_name, geom, sp_ref, workspace)
Converts a Spark DataFrame to an ArcGIS feature class.

**Parameters:**
- `df` (DataFrame): Spark DataFrame to convert
- `feature_class_name` (str): Name of output feature class
- `geom` (str): Name of geometry column with WKB values (default: "geom")
- `sp_ref` (Optional[int]): Spatial reference WKID (default: 3857 for Web Mercator; negative values use active map's spatial reference)
- `workspace` (str): Workspace for feature class ("memory", "scratch", or geodatabase path) (default: "scratch")

**Returns:** None

### to_spark(feature_class_name, fields, where_clause)
Converts an ArcGIS feature class to a Spark DataFrame.

**Parameters:**
- `feature_class_name` (str): Name or path of the feature class
- `fields` (Optional[Union[List[str], str]]): Fields to include in DataFrame
  - `None`: Only [OBJECTID, SHAPE] fields (default)
  - `"*"`: All fields from the feature class
  - `List[str]`: Specific field names to include
- `where_clause` (Optional[str]): SQL where clause to filter features (default: None)

**Returns:** Spark DataFrame with feature class data

---

## Usage Notes

- All geometry-producing functions return columns with sensible default aliases (e.g., "geom" for geometries, "area" for area calculations)
- Parameters accepting `Union[Column, str]` can accept either a PySpark Column or a string column name
- Numeric parameters can be Column references or literal values which will be converted to literals using `lit()`
- The `wkid` parameter specifies spatial reference using EPSG/ESRI Well-Known IDs (-1 means unspecified)
- Most functions work within a Spark SQL context and require an active SparkSession

### Null inputs are the caller's responsibility

**Never pass a null argument to a GeoFunction.** For speed these functions do not check
for null on every row — filter or coalesce upstream instead:

```python
df.filter(F.col("lon").isNotNull() & F.col("lat").isNotNull()) \
  .withColumn("geom", st_point("lon", "lat"))
```

A null that reaches a function does not raise. It is silently read as `0.0` / `0` /
`-1.0` / `-1` depending on the type and the execution path, and you get a plausible but
wrong answer: `st_point(NULL, 2.0)` yields `POINT (-1 2)`, and
`h3_latlng_to_cell(NULL, lng, 7)` yields the cell on the equator. If you are seeing
geometry at odd coordinates, look for an unfiltered null column.
