import os
import pyarrow as pa
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import lit, array, explode, col, collect_list
from typing import Union, Optional, List


def st_register_functions() -> None:
    """Register ST_XXX SQL functions with active spark context.

    This function registers all spatial SQL functions (ST_XXX) with the active
    Spark context, making them available for use in Spark SQL queries.

    :return: None
    :rtype: None

    Example::

        st_register_functions()
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    sc._jvm.com.esri.spark.Registry.registerFunctions()


def h3_cell_to_boundary(
        cell: Union[Column, str] = "cell",
) -> Column:
    """Convert H3 cell to boundary.

    :param cell: The H3 cell identifier or column name
    :type cell: Union[Column, str]
    :return: The boundary geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("boundary", h3_cell_to_boundary("h3_cell"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.h3CellToBoundary(
        _to_java_column(cell),
    )).alias("geom")


def h3_latlng_to_cell(
        lat: Union[Column, str, float],
        lng: Union[Column, str, float],
        res: Union[Column, str, int],
) -> Column:
    """Convert latitude and longitude to H3 cell.

    :param lat: The latitude value or column name
    :type lat: Union[Column, str, float]
    :param lng: The longitude value or column name
    :type lng: Union[Column, str, float]
    :param res: The H3 resolution level (0-15)
    :type res: Union[Column, str, int]
    :return: The H3 cell identifier with alias "cell"
    :rtype: Column

    Example::

        df.withColumn("h3_cell", h3_latlng_to_cell("latitude", "longitude", 9))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(lat, float):
        lat = lit(float(lat))
    if isinstance(lng, float):
        lng = lit(float(lng))
    if isinstance(res, int):
        res = lit(int(res))
    return Column(sc._jvm.com.esri.spark.GeoFunctions.h3LatLngToCell(
        _to_java_column(lat),
        _to_java_column(lng),
        _to_java_column(res),
    )).alias("cell")


def st_translate(
        geom: Union[Column, str] = "geom",
        dx: Union[Column, str, float] = 0.0,
        dy: Union[Column, str, float] = 0.0,
) -> Column:
    """Translate a geometry by dx, dy.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param dx: The x translation offset in units of the geometry. Default is 0.0
    :type dx: Union[Column, str, float]
    :param dy: The y translation offset in units of the geometry. Default is 0.0
    :type dy: Union[Column, str, float]
    :return: The translated geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("translated", st_translate("geom", 100.0, 50.0))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(dx, float):
        dx = lit(float(dx))
    if isinstance(dy, float):
        dy = lit(float(dy))
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stTranslate(
        _to_java_column(geom),
        _to_java_column(dx),
        _to_java_column(dy),
    )).alias("geom")


def st_area(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Compute the area of a geometry.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: The area value with alias "area"
    :rtype: Column

    Example::

        df.withColumn("polygon_area", st_area("geom"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stArea(
        _to_java_column(geom),
    )).alias("area")


def st_length(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Compute the length of a geometry.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: The length value with alias "length"
    :rtype: Column

    Example::

        df.withColumn("line_length", st_length("geom"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stLength(
        _to_java_column(geom),
    )).alias("length")


def st_point(
        x: Union[Column, str] = "x",
        y: Union[Column, str] = "y",
) -> Column:
    """Create a point from the coordinates (x, y).

    :param x: The x coordinate or column name
    :type x: Union[Column, str]
    :param y: The y coordinate or column name
    :type y: Union[Column, str]
    :return: A point geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("point", st_point("lon", "lat"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stPoint(
        _to_java_column(x),
        _to_java_column(y),
    )).alias("geom")


def st_line(
        x1: Union[Column, str],
        y1: Union[Column, str],
        x2: Union[Column, str],
        y2: Union[Column, str],
) -> Column:
    """Create a polyline from the coordinates (x1, y1, x2, y2).

    :param x1: The first point x coordinate or column name
    :type x1: Union[Column, str]
    :param y1: The first point y coordinate or column name
    :type y1: Union[Column, str]
    :param x2: The second point x coordinate or column name
    :type x2: Union[Column, str]
    :param y2: The second point y coordinate or column name
    :type y2: Union[Column, str]
    :return: A polyline geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("line", st_line("x1", "y1", "x2", "y2"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stLine(
        _to_java_column(x1),
        _to_java_column(y1),
        _to_java_column(x2),
        _to_java_column(y2),
    )).alias("geom")


def st_rect(
        x1: Union[Column, str],
        y1: Union[Column, str],
        x2: Union[Column, str],
        y2: Union[Column, str],
) -> Column:
    """Create a polygon rectangle from the coordinates (x1, y1, x2, y2).

    :param x1: The first corner x coordinate or column name
    :type x1: Union[Column, str]
    :param y1: The first corner y coordinate or column name
    :type y1: Union[Column, str]
    :param x2: The opposite corner x coordinate or column name
    :type x2: Union[Column, str]
    :param y2: The opposite corner y coordinate or column name
    :type y2: Union[Column, str]
    :return: A rectangular polygon geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("rect", st_rect("xmin", "ymin", "xmax", "ymax"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stRect(
        _to_java_column(x1),
        _to_java_column(y1),
        _to_java_column(x2),
        _to_java_column(y2),
    )).alias("geom")


def st_cell(
        x: Union[Column, str],
        y: Union[Column, str],
        w: Union[Column, str, float],
        h: Optional[Union[Column, str, float]] = None,
) -> Column:
    """Create a polygon rectangle with lower left corner at x/y with width w and height h.

    :param x: The left x coordinate or column name
    :type x: Union[Column, str]
    :param y: The lower y coordinate or column name
    :type y: Union[Column, str]
    :param w: The width of the cell or column name
    :type w: Union[Column, str, float]
    :param h: The height of the cell or column name. If h is None, then h = w
    :type h: Optional[Union[Column, str, float]]
    :return: A rectangular polygon geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("cell", st_cell("x", "y", 100.0, 50.0))
        df.withColumn("square_cell", st_cell("x", "y", 100.0))  # h defaults to w
    """
    if h is None:
        h = w
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(w, (float, int)):
        w = lit(float(w))
    if isinstance(h, (float, int)):
        h = lit(float(h))
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stCell(
        _to_java_column(x),
        _to_java_column(y),
        _to_java_column(w),
        _to_java_column(h),
    )).alias("geom")


def st_box(
        x: Union[Column, str],
        y: Union[Column, str],
        h: Union[Column, str],
        v: Optional[Union[Column, str]] = None,
) -> Column:
    """Create a polygon rectangle with center at x/y with width = 2*h and height = 2*v.

    :param x: The center x coordinate or column name
    :type x: Union[Column, str]
    :param y: The center y coordinate or column name
    :type y: Union[Column, str]
    :param h: The horizontal padding of the box or column name
    :type h: Union[Column, str]
    :param v: The vertical padding of the box or column name. If v is None, then v = h
    :type v: Optional[Union[Column, str]]
    :return: A rectangular polygon geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("box", st_box("x", "y", "h_pad", "v_pad"))
        df.withColumn("square_box", st_box("x", "y", "padding"))  # v defaults to h
    """
    if v is None:
        v = h
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(h, (float, int)):
        h = lit(float(h))
    if isinstance(v, (float, int)):
        v = lit(float(v))
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stBox(
        _to_java_column(x),
        _to_java_column(y),
        _to_java_column(h),
        _to_java_column(v),
    )).alias("geom")


def st_astext(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Convert a geometry to a WKT string representation.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: The WKT (Well-Known Text) string representation with alias "text"
    :rtype: Column

    Example::

        df.withColumn("wkt", st_astext("geom"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stAsText(
        _to_java_column(geom),
    )).alias("text")


def st_asgeojson(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Convert a geometry to a GeoJSON string representation.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: The GeoJSON string representation with alias "geojson"
    :rtype: Column

    Example::

        df.withColumn("geojson_str", st_asgeojson("geom"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stAsGeoJSON(
        _to_java_column(geom),
    )).alias("geojson")


def st_fromtext(
        text: Union[Column, str] = "text",
) -> Column:
    """Create a geometry from a WKT string representation.

    :param text: The WKT string or column name. Default is "text"
    :type text: Union[Column, str]
    :return: The geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("geom", st_fromtext("wkt_column"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stFromText(
        _to_java_column(text),
    )).alias("geom")


def st_lontox(
        lon: Union[Column, str] = "lon",
) -> Column:
    """Convert a longitude to an x coordinate in meters (Web Mercator projection).

    :param lon: The longitude value or column name
    :type lon: Union[Column, str]
    :return: The x coordinate in meters with alias "x"
    :rtype: Column

    Example::

        df.withColumn("x_meters", st_lontox("longitude"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stLonToX(
        _to_java_column(lon),
    )).alias("x")


def st_lattoy(
        lat: Union[Column, str] = "lat",
) -> Column:
    """Convert a latitude to a y coordinate in meters (Web Mercator projection).

    :param lat: The latitude value or column name
    :type lat: Union[Column, str]
    :return: The y coordinate in meters with alias "y"
    :rtype: Column

    Example::

        df.withColumn("y_meters", st_lattoy("latitude"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stLatToY(
        _to_java_column(lat),
    )).alias("y")


def st_xtolon(
        x: Union[Column, str] = "x",
) -> Column:
    """Convert an x coordinate in meters to a longitude (Web Mercator projection).

    :param x: The x coordinate in meters or column name
    :type x: Union[Column, str]
    :return: The longitude value with alias "lon"
    :rtype: Column

    Example::

        df.withColumn("longitude", st_xtolon("x_meters"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stXToLon(
        _to_java_column(x),
    )).alias("lon")


def st_ytolat(
        y: Union[Column, str] = "y",
) -> Column:
    """Convert a y coordinate in meters to a latitude (Web Mercator projection).

    :param y: The y coordinate in meters or column name
    :type y: Union[Column, str]
    :return: The latitude value with alias "lat"
    :rtype: Column

    Example::

        df.withColumn("latitude", st_ytolat("y_meters"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stYToLat(
        _to_java_column(y),
    )).alias("lat")


def st_lontoq(
        lon: Union[Column, str],
        cell: Union[Column, str, float, int],
) -> Column:
    """Convert a longitude to a q value (column index in quadtree grid).

    :param lon: The longitude value or column name
    :type lon: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :return: The q value (column index) with alias "q"
    :rtype: Column

    Example::

        df.withColumn("q", st_lontoq("longitude", 10000.0))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (float, int)):
        cell = lit(float(cell))
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stLonToQ(
        _to_java_column(lon),
        _to_java_column(cell),
    )).alias("q")


def st_lattor(
        lat: Union[Column, str],
        cell: Union[Column, str, float, int],
) -> Column:
    """Convert a latitude to an r value (row index in quadtree grid).

    :param lat: The latitude value or column name
    :type lat: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :return: The r value (row index) with alias "r"
    :rtype: Column

    Example::

        df.withColumn("r", st_lattor("latitude", 10000.0))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (float, int)):
        cell = lit(float(cell))
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stLatToR(
        _to_java_column(lat),
        _to_java_column(cell),
    )).alias("r")


def st_qtox(
        q: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
) -> Column:
    """Convert q value (column index) to x coordinate in meters.

    :param q: The q value (column index) or column name
    :type q: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding/offset in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :return: The x coordinate in meters with alias "x"
    :rtype: Column

    Example::

        df.withColumn("x", st_qtox("q", 10000.0))
        df.withColumn("x", st_qtox("q", 10000.0, 100.0))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (float, int)):
        cell = lit(float(cell))
    if isinstance(dist, (float, int)):
        dist = lit(float(dist))
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stQToX(
        _to_java_column(q),
        _to_java_column(cell),
        _to_java_column(dist),
    )).alias("x")


def st_rtoy(
        r: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
) -> Column:
    """Convert r value (row index) to y coordinate in meters.

    :param r: The r value (row index) or column name
    :type r: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding/offset in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :return: The y coordinate in meters with alias "y"
    :rtype: Column

    Example::

        df.withColumn("y", st_rtoy("r", 10000.0))
        df.withColumn("y", st_rtoy("r", 10000.0, 100.0))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (float, int)):
        cell = lit(float(cell))
    if isinstance(dist, (float, int)):
        dist = lit(float(dist))
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stQToX(
        _to_java_column(r),
        _to_java_column(cell),
        _to_java_column(dist),
    )).alias("y")


def st_polyline(
        *points,
) -> Column:
    """Convert an array of st_point to a polyline geometry.

    :param points: Variable number of point geometries or array column
    :type points: Variable arguments of Column
    :return: The polyline geometry with alias "geom"
    :rtype: Column
    :raises ValueError: If no points are provided

    Example::

        df.withColumn("line", st_polyline(st_point("x1", "y1"), st_point("x2", "y2")))
        df.withColumn("line", st_polyline("point_array"))
    """
    sc = SparkContext._active_spark_context
    if points:
        arr = array(list(points)) if len(points) > 1 else points[0]
        return Column(sc._jvm.com.esri.spark.GeoFunctions.stPolyline(
            _to_java_column(arr)
        )).alias("geom")
    else:
        raise ValueError("st_polyline expects a list of st_point instances.")


def st_multipoint(
        *points,
) -> Column:
    """Convert an array of st_point to a multipoint geometry.

    :param points: Variable number of point geometries or array column
    :type points: Variable arguments of Column
    :return: The multipoint geometry with alias "geom"
    :rtype: Column
    :raises ValueError: If no points are provided

    Example::

        df.withColumn("multipt", st_multipoint(st_point("x1", "y1"), st_point("x2", "y2")))
        df.withColumn("multipt", st_multipoint("point_array"))
    """
    sc = SparkContext._active_spark_context
    if points:
        arr = array(list(points)) if len(points) > 1 else points[0]
        return Column(sc._jvm.com.esri.spark.GeoFunctions.stMultipoint(
            _to_java_column(arr)
        )).alias("geom")
    else:
        raise ValueError("st_multipoint expects a list of st_point instances.")


def st_polyline2(
        xy: Union[Column, str],
) -> Column:
    """Convert an XY coordinate array to a polyline geometry.

    :param xy: Array of [x, y] coordinates or column name
    :type xy: Union[Column, str]
    :return: The polyline geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("line", st_polyline2("xy_array"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stPolyline2(
        _to_java_column(xy),
    )).alias("geom")


def st_polygon(
        *points,
) -> Column:
    """Convert an array of st_point to a polygon geometry.

    :param points: Variable number of point geometries or array column
    :type points: Variable arguments of Column
    :return: The polygon geometry with alias "geom"
    :rtype: Column
    :raises ValueError: If no points are provided

    Example::

        df.withColumn("poly", st_polygon(st_point("x1", "y1"), st_point("x2", "y2"), st_point("x3", "y3")))
        df.withColumn("poly", st_polygon("point_array"))
    """
    sc = SparkContext._active_spark_context
    if points:
        arr = array(list(points)) if len(points) > 1 else points[0]
        return Column(sc._jvm.com.esri.spark.GeoFunctions.stPolygon(
            _to_java_column(arr)
        )).alias("geom")
    else:
        raise ValueError("st_polygon expects a list of st_point instances.")


def st_polygon2(
        xy: Union[Column, str],
) -> Column:
    """Convert an XY coordinate array to a polygon geometry.

    :param xy: Array of [x, y] coordinates or column name
    :type xy: Union[Column, str]
    :return: The polygon geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("poly", st_polygon2("xy_array"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stPolygon2(
        _to_java_column(xy),
    )).alias("geom")


def st_intersection(
        lhs: Union[Column, str] = "lgeom",
        rhs: Union[Column, str] = "rgeom",
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Compute the intersection of two geometries.

    :param lhs: The left hand side geometry or column name. Default is "lgeom"
    :type lhs: Union[Column, str]
    :param rhs: The right hand side geometry or column name. Default is "rgeom"
    :type rhs: Union[Column, str]
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: The intersection geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("intersection", st_intersection("geom1", "geom2", 4326))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stIntersection(
            _to_java_column(lhs),
            _to_java_column(rhs),
            _to_java_column(wkid),
        )
    ).alias("geom")


def st_intersects(
        lhs: Union[Column, str] = "lgeom",
        rhs: Union[Column, str] = "rgeom",
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Check if two geometries intersect.

    :param lhs: The left hand side geometry or column name. Default is "lgeom"
    :type lhs: Union[Column, str]
    :param rhs: The right hand side geometry or column name. Default is "rgeom"
    :type rhs: Union[Column, str]
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: Boolean column indicating if geometries intersect
    :rtype: Column

    Example::

        df.filter(st_intersects("geom1", "geom2", 4326))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stIntersects(
            _to_java_column(lhs),
            _to_java_column(rhs),
            _to_java_column(wkid),
        )
    )


def st_intersects_box(
        geom: Union[Column, str] = "geom",
        xmin: Union[Column, int, float] = -180.0,
        ymin: Union[Column, int, float] = -90.0,
        xmax: Union[Column, int, float] = 180.0,
        ymax: Union[Column, int, float] = 90.0,
) -> Column:
    """Check if a geometry intersects with a bounding box.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :param xmin: The minimum x coordinate of the box. Default is -180.0
    :type xmin: Union[Column, int, float]
    :param ymin: The minimum y coordinate of the box. Default is -90.0
    :type ymin: Union[Column, int, float]
    :param xmax: The maximum x coordinate of the box. Default is 180.0
    :type xmax: Union[Column, int, float]
    :param ymax: The maximum y coordinate of the box. Default is 90.0
    :type ymax: Union[Column, int, float]
    :return: Boolean column indicating if geometry intersects the box
    :rtype: Column

    Example::

        df.filter(st_intersects_box("geom", -180, -90, 180, 90))
        df.filter(st_intersects_box("geom", "xmin_col", "ymin_col", "xmax_col", "ymax_col"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(xmin, (int, float)):
        xmin = lit(float(xmin))
    if isinstance(ymin, (int, float)):
        ymin = lit(float(ymin))
    if isinstance(xmax, (int, float)):
        xmax = lit(float(xmax))
    if isinstance(ymax, (int, float)):
        ymax = lit(float(ymax))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stIntersectsBox(
            _to_java_column(geom),
            _to_java_column(xmin),
            _to_java_column(ymin),
            _to_java_column(xmax),
            _to_java_column(ymax),
        )
    )


def st_overlaps(
        lhs: Union[Column, str] = "lgeom",
        rhs: Union[Column, str] = "rgeom",
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Check if two geometries overlap.

    Geometries overlap if they have some but not all points in common,
    are of the same dimension, and the intersection of their interiors is non-empty.

    :param lhs: The left hand side geometry or column name. Default is "lgeom"
    :type lhs: Union[Column, str]
    :param rhs: The right hand side geometry or column name. Default is "rgeom"
    :type rhs: Union[Column, str]
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: Boolean column indicating if geometries overlap
    :rtype: Column

    Example::

        df.filter(st_overlaps("geom1", "geom2", 4326))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stOverlaps(
            _to_java_column(lhs),
            _to_java_column(rhs),
            _to_java_column(wkid),
        )
    )


def st_contains(
        lhs: Union[Column, str] = "lgeom",
        rhs: Union[Column, str] = "rgeom",
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Check if lhs geometry contains rhs geometry.

    Returns true if the left geometry contains the right geometry
    (no points of rhs lie outside of lhs, and at least one interior point is inside lhs).

    :param lhs: The left hand side (container) geometry or column name. Default is "lgeom"
    :type lhs: Union[Column, str]
    :param rhs: The right hand side (contained) geometry or column name. Default is "rgeom"
    :type rhs: Union[Column, str]
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: Boolean column indicating if lhs contains rhs
    :rtype: Column

    Example::

        df.filter(st_contains("polygon", "point", 4326))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stContains(
            _to_java_column(lhs),
            _to_java_column(rhs),
            _to_java_column(wkid),
        )
    )


def st_within(
        lhs: Union[Column, str] = "lgeom",
        rhs: Union[Column, str] = "rgeom",
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Check if lhs geometry is within rhs geometry.

    Returns true if the left geometry is completely within the right geometry
    (inverse of st_contains).

    :param lhs: The left hand side (inner) geometry or column name. Default is "lgeom"
    :type lhs: Union[Column, str]
    :param rhs: The right hand side (outer) geometry or column name. Default is "rgeom"
    :type rhs: Union[Column, str]
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: Boolean column indicating if lhs is within rhs
    :rtype: Column

    Example::

        df.filter(st_within("point", "polygon", 4326))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stWithin(
            _to_java_column(lhs),
            _to_java_column(rhs),
            _to_java_column(wkid),
        )
    )


def st_touches(
        lhs: Union[Column, str] = "lgeom",
        rhs: Union[Column, str] = "rgeom",
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Check if lhs geometry touches rhs geometry.

    Returns true if the geometries have at least one point in common, but their
    interiors do not intersect.

    :param lhs: The left hand side geometry or column name. Default is "lgeom"
    :type lhs: Union[Column, str]
    :param rhs: The right hand side geometry or column name. Default is "rgeom"
    :type rhs: Union[Column, str]
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: Boolean column indicating if geometries touch
    :rtype: Column

    Example::

        df.filter(st_touches("polygon1", "polygon2", 4326))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stTouches(
            _to_java_column(lhs),
            _to_java_column(rhs),
            _to_java_column(wkid),
        )
    )


def st_disjoint(
        lhs: Union[Column, str] = "lgeom",
        rhs: Union[Column, str] = "rgeom",
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Check if lhs geometry is disjoint from the rhs geometry.

    Returns true if the geometries have no point in common.

    :param lhs: The left hand side geometry or column name. Default is "lgeom"
    :type lhs: Union[Column, str]
    :param rhs: The right hand side geometry or column name. Default is "rgeom"
    :type rhs: Union[Column, str]
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: Boolean column indicating if geometries are disjoint
    :rtype: Column

    Example::

        df.filter(st_disjoint("geom1", "geom2", 4326))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stDisjoint(
            _to_java_column(lhs),
            _to_java_column(rhs),
            _to_java_column(wkid),
        )
    )


def st_iou(
        lhs: Union[Column, str] = "lgeom",
        rhs: Union[Column, str] = "rgeom",
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Calculate the Intersection over Union (IoU) of two geometries.

    IoU = Area(intersection) / Area(union)
    Commonly used in object detection and image segmentation for measuring overlap.

    :param lhs: The left hand side geometry or column name. Default is "lgeom"
    :type lhs: Union[Column, str]
    :param rhs: The right hand side geometry or column name. Default is "rgeom"
    :type rhs: Union[Column, str]
    :param wkid: The spatial reference WKID of the geometries. Default is -1
    :type wkid: Union[Column, str, int]
    :return: The IoU value (0.0 to 1.0) with alias "iou"
    :rtype: Column

    Example::

        df.withColumn("iou_score", st_iou("predicted_geom", "ground_truth", 4326))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stIoU(
            _to_java_column(lhs),
            _to_java_column(rhs),
            _to_java_column(wkid),
        )
    ).alias("iou")


def st_isempty(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Check if geometry is empty.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: Boolean column indicating if geometry is empty
    :rtype: Column

    Example::

        df.filter(~st_isempty("geom"))  # Filter out empty geometries
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stIsEmpty(
            _to_java_column(geom),
        )
    )


def st_euclid(
        x1: Union[Column, str],
        y1: Union[Column, str],
        x2: Union[Column, str],
        y2: Union[Column, str],
) -> Column:
    """Compute the Euclidean distance between two points.

    Calculates the straight-line distance using the formula:
    sqrt((x2-x1)² + (y2-y1)²)

    :param x1: The x coordinate of the first point or column name
    :type x1: Union[Column, str]
    :param y1: The y coordinate of the first point or column name
    :type y1: Union[Column, str]
    :param x2: The x coordinate of the second point or column name
    :type x2: Union[Column, str]
    :param y2: The y coordinate of the second point or column name
    :type y2: Union[Column, str]
    :return: The Euclidean distance
    :rtype: Column

    Example::

        df.withColumn("distance", st_euclid("x1", "y1", "x2", "y2"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stEuclid(
            _to_java_column(x1),
            _to_java_column(y1),
            _to_java_column(x2),
            _to_java_column(y2),
        )
    )


def st_distance(
        lhs: Union[Column, str] = "lgeom",
        rhs: Union[Column, str] = "rgeom",
) -> Column:
    """Compute the distance between two geometries.

    :param lhs: The left hand side geometry or column name. Default is "lgeom"
    :type lhs: Union[Column, str]
    :param rhs: The right hand side geometry or column name. Default is "rgeom"
    :type rhs: Union[Column, str]
    :return: The distance value
    :rtype: Column

    Example::

        df.withColumn("dist", st_distance("geom1", "geom2"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stDistance(
            _to_java_column(lhs),
            _to_java_column(rhs),
        )
    )


def qr_envp(
        geom: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
) -> Column:
    """Compute the qr/envp of a geometry.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :return: Array of qr/envp values
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (int, float)):
        cell = lit(float(cell))
    if isinstance(dist, (int, float)):
        dist = lit(float(dist))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.qrEnvp(
            _to_java_column(geom),
            _to_java_column(cell),
            _to_java_column(dist),
        )
    )


def qr_envp_geom(
        geom: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Compute the qr/envp/geom of a geometry.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :param wkid: The spatial reference ID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: Array of qr/envp/geom values
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (int, float)):
        cell = lit(float(cell))
    if isinstance(dist, (int, float)):
        dist = lit(float(dist))
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.qrEnvpGeom(
            _to_java_column(geom),
            _to_java_column(cell),
            _to_java_column(dist),
            _to_java_column(wkid),
        )
    )


def qr_envp_explode(
        geom: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
) -> Column:
    """Explode the qr/envp of a geometry into separate rows.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :return: The exploded qr with alias "qr"
    :rtype: Column
    """
    return explode(
        qr_envp(geom, cell, dist)
    ).alias("qr", metadata={"cell": cell, "dist": dist})


def qr_envp_geom_explode(
        geom: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Explode the qr/envp/geom of a geometry into separate rows.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :param wkid: The spatial reference ID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: The exploded qr with alias "qr"
    :rtype: Column
    """
    return explode(
        qr_envp_geom(geom, cell, dist, wkid)
    ).alias("qr", metadata={"cell": cell, "dist": dist})


def qr_geom(
        geom: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Compute the list of qr/geom of a geometry.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :param wkid: The spatial reference ID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: Array of qr/geom structures
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (int, float)):
        cell = lit(float(cell))
    if isinstance(dist, (int, float)):
        dist = lit(float(dist))
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.qrGeom(
            _to_java_column(geom),
            _to_java_column(cell),
            _to_java_column(dist),
            _to_java_column(wkid),
        )
    )


def qr_asgeom(
        qr: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
) -> Column:
    """Compute the geometry of a qr.

    :param qr: The qr code or column name
    :type qr: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :return: The qr geometry with alias "geom"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (int, float)):
        cell = lit(float(cell))
    if isinstance(dist, (int, float)):
        dist = lit(float(dist))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.qrAsGeom(
            _to_java_column(qr),
            _to_java_column(cell),
            _to_java_column(dist),
        )
    )


def qr_contains_geom(
        qr: Union[Column, str],
        cell: Union[Column, str, float, int],
        geom: Union[Column, str],
) -> Column:
    """Check if the geometry is fully inside a QR cell.

    :param qr: The QR code or column name
    :type qr: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :return: Boolean column indicating if qr contains geom
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (int, float)):
        cell = lit(float(cell))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.qrContainsGeom(
            _to_java_column(qr),
            _to_java_column(cell),
            _to_java_column(geom),
        )
    )


def qr_geom_explode(
        geom: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Explode the qr/geom of a geometry into separate rows.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :param wkid: The spatial reference ID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: The exploded qr with alias "qr"
    :rtype: Column
    """
    return explode(
        qr_geom(geom, cell, dist, wkid)
    ).alias("qr", metadata={"cell": cell, "dist": dist, "wkid": wkid})


def qr_list(
        geom: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
) -> Column:
    """Compute the qr list of a geometry.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :return: Array of qr values
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (int, float)):
        cell = lit(float(cell))
    if isinstance(dist, (int, float)):
        dist = lit(float(dist))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.qrList(
            _to_java_column(geom),
            _to_java_column(cell),
            _to_java_column(dist),
        )
    )


def qr_list_explode(
        geom: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
) -> Column:
    """Explode the qr list of a geometry into separate rows.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :return: The exploded qr with alias "qr"
    :rtype: Column
    """
    return explode(
        qr_list(geom, cell, dist)
    ).alias("qr", metadata={"cell": cell, "dist": dist})


def qr_count(
        geom: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
) -> Column:
    """Return the number of QR that the geometry covers.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :return: The number of QR cells that the geometry covers
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (int, float)):
        cell = lit(float(cell))
    if isinstance(dist, (int, float)):
        dist = lit(float(dist))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.qrCount(
            _to_java_column(geom),
            _to_java_column(cell),
            _to_java_column(dist),
        )
    )


def qr_intersect(
        lhs: Union[Column, str],
        rhs: Union[Column, str],
        cell: Union[Column, int, float],
) -> Column:
    """Check if the qr/envp of two geometries intersect.

    :param lhs: The left hand side qr/envp or column name
    :type lhs: Union[Column, str]
    :param rhs: The right hand side qr/envp or column name
    :type rhs: Union[Column, str]
    :param cell: The cell size in meters
    :type cell: Union[Column, int, float]
    :return: Boolean column indicating if qr/envp intersect
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (int, float)):
        cell = lit(float(cell))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.qrIntersect(
            _to_java_column(lhs),
            _to_java_column(rhs),
            _to_java_column(cell),
        ))


def qr_fromxy(
        x: Union[Column, str],
        y: Union[Column, str],
        cell: Union[Column, str, int, float],
) -> Column:
    """Compute the qr value for a given x/y coordinate.

    :param x: The x coordinate or column name
    :type x: Union[Column, str]
    :param y: The y coordinate or column name
    :type y: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, int, float]
    :return: The qr value with alias "qr"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (int, float)):
        cell = lit(float(cell))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.qrFromXY(
            _to_java_column(x),
            _to_java_column(y),
            _to_java_column(cell),
        )).alias("qr")


def qr_fromgeom(
        geom: Union[Column, str],
        cell: Union[Column, str, int, float],
) -> Column:
    """Compute the qr value for a given geometry lower/left envelope point.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, int, float]
    :return: The qr value with alias "qr"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (int, float)):
        cell = lit(float(cell))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.qrFromGeom(
            _to_java_column(geom),
            _to_java_column(cell),
        )).alias("qr")


def st_x(
        geom: Union[Column, str] = "geom",
        index: Union[Column, str, int] = 0,
) -> Column:
    """Get the x coordinate of a geometry at a point index.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :param index: The point index. Default is 0
    :type index: Union[Column, str, int]
    :return: The x coordinate with alias "x"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(index, (int, float)):
        index = lit(int(index))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stX(
            _to_java_column(geom),
            _to_java_column(index),
        )).alias("x")


def st_y(
        geom: Union[Column, str] = "geom",
        index: Union[Column, str, int] = 0,
) -> Column:
    """Get the y coordinate of a geometry at a point index.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :param index: The point index. Default is 0
    :type index: Union[Column, str, int]
    :return: The y coordinate with alias "y"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(index, (int, float)):
        index = lit(int(index))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stY(
            _to_java_column(geom),
            _to_java_column(index),
        )).alias("y")


def st_manhattan(
        x1: Union[Column, str, float, int],
        y1: Union[Column, str, float, int],
        x2: Union[Column, str, float, int],
        y2: Union[Column, str, float, int],
) -> Column:
    """Compute the Manhattan distance between two points.

    The Manhattan distance is the sum of absolute differences: |x2-x1| + |y2-y1|

    :param x1: The x coordinate of the first point or column name
    :type x1: Union[Column, str, float, int]
    :param y1: The y coordinate of the first point or column name
    :type y1: Union[Column, str, float, int]
    :param x2: The x coordinate of the second point or column name
    :type x2: Union[Column, str, float, int]
    :param y2: The y coordinate of the second point or column name
    :type y2: Union[Column, str, float, int]
    :return: The Manhattan distance
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(x1, (int, float)):
        x1 = lit(float(x1))
    if isinstance(y1, (int, float)):
        y1 = lit(float(y1))
    if isinstance(x2, (int, float)):
        x2 = lit(float(x2))
    if isinstance(y2, (int, float)):
        y2 = lit(float(y2))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stManhattan(
            _to_java_column(x1),
            _to_java_column(y1),
            _to_java_column(x2),
            _to_java_column(y2),
        ))


def st_haversine(
        lon1: Union[Column, str, float, int],
        lat1: Union[Column, str, float, int],
        lon2: Union[Column, str, float, int],
        lat2: Union[Column, str, float, int],
) -> Column:
    """Compute the Haversine distance between two points on a sphere.

    The Haversine distance is the great-circle distance between two points on Earth,
    given their longitudes and latitudes.

    :param lon1: The longitude of the first point or column name
    :type lon1: Union[Column, str, float, int]
    :param lat1: The latitude of the first point or column name
    :type lat1: Union[Column, str, float, int]
    :param lon2: The longitude of the second point or column name
    :type lon2: Union[Column, str, float, int]
    :param lat2: The latitude of the second point or column name
    :type lat2: Union[Column, str, float, int]
    :return: The Haversine distance in meters
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(lon1, (int, float)):
        lon1 = lit(float(lon1))
    if isinstance(lat1, (int, float)):
        lat1 = lit(float(lat1))
    if isinstance(lon2, (int, float)):
        lon2 = lit(float(lon2))
    if isinstance(lat2, (int, float)):
        lat2 = lit(float(lat2))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stHaversine(
            _to_java_column(lon1),
            _to_java_column(lat1),
            _to_java_column(lon2),
            _to_java_column(lat2),
        ))


def st_xtoq(
        x: Union[Column, str],
        cell: Union[Column, str, float, int],
) -> Column:
    """Convert x meters to a q value (column index in quadtree grid).

    :param x: The x coordinate in meters or column name
    :type x: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :return: The q value (column index) with alias "q"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (float, int)):
        cell = lit(float(cell))
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stXToQ(
        _to_java_column(x),
        _to_java_column(cell),
    )).alias("q")


def st_ytor(
        y: Union[Column, str],
        cell: Union[Column, str, float, int],
) -> Column:
    """Convert y meters to an r value (row index in quadtree grid).

    :param y: The y coordinate in meters or column name
    :type y: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :return: The r value (row index) with alias "r"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (float, int)):
        cell = lit(float(cell))
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stXToQ(
        _to_java_column(y),
        _to_java_column(cell),
    )).alias("r")


def st_xy(
        geom: Union[Column, str] = "geom",
        index: Union[Column, str, int] = 0,
) -> Column:
    """Get the x/y coordinate of a geometry at a point index.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :param index: The point index. Default is 0
    :type index: Union[Column, str, int]
    :return: The x/y coordinate as array with alias "xy"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(index, (int, float)):
        index = lit(int(index))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stXY(
            _to_java_column(geom),
            _to_java_column(index),
        )).alias("xy")


def st_centroid(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Get the centroid of a geometry.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: The centroid as a point with alias "geom"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stCentroid(
            _to_java_column(geom),
        )).alias("geom")


def st_centroid_xy(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Get the centroid XY coordinates of a geometry.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: The centroid as XY array with alias "xy"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stCentroidXY(
            _to_java_column(geom),
        )).alias("xy")


def st_buffer(
        geom: Union[Column, str],
        distance: Union[Column, str, float, int],
        num_vertices: Union[float, int] = 36,
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Get the buffer of a geometry.

    Creates a polygon representing all points within a given distance from the geometry.

    :param geom: The geometry or column name
    :type geom: Union[Column, str]
    :param distance: The buffer distance in units of the geometry or column name
    :type distance: Union[Column, str, float, int]
    :param num_vertices: The number of vertices to approximate curves. Default is 36
    :type num_vertices: Union[float, int]
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: The buffered geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("buffer", st_buffer("geom", 100.0))
        df.withColumn("precise_buffer", st_buffer("geom", 100.0, 72, 4326))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(distance, (int, float)):
        distance = lit(float(distance))
    if isinstance(num_vertices, (int, float)):
        num_vertices = lit(int(num_vertices))
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stBuffer(
            _to_java_column(geom),
            _to_java_column(distance),
            _to_java_column(num_vertices),
            _to_java_column(wkid),
        )).alias("geom")


def st_convexhull(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Get the convex hull of a geometry.

    The convex hull is the smallest convex polygon that contains the geometry.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: The convex hull geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("hull", st_convexhull("geom"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stConvexHull(
            _to_java_column(geom),
        )).alias("geom")


def st_mercator(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Convert the coordinates from WGS84 to Web Mercator projection.

    Projects geographic coordinates (latitude/longitude) to Web Mercator (EPSG:3857).

    :param geom: The geometry in WGS84 or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: The projected geometry in Web Mercator with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("web_merc", st_mercator("wgs84_geom"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stMercator(
            _to_java_column(geom),
        )).alias("geom")


def st_wgs84(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Convert the coordinates from Web Mercator to WGS84 projection.

    Projects Web Mercator coordinates (EPSG:3857) to geographic coordinates (latitude/longitude).

    :param geom: The geometry in Web Mercator or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: The projected geometry in WGS84 with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("wgs84", st_wgs84("web_merc_geom"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stWGS84(
            _to_java_column(geom),
        )).alias("geom")


def st_union_col(
        coll: Union[Column, str],
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Get the union of a collection of geometries.

    Combines all geometries in the collection into a single geometry.

    :param coll: A collection/array of geometries or column name
    :type coll: Union[Column, str]
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: The union of all geometries with alias "geom"
    :rtype: Column

    Example::

        df.groupBy("group_id").agg(collect_list("geom").alias("geoms")) \\
          .withColumn("union", st_union_col("geoms", 4326))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stUnionCol(
            _to_java_column(coll),
            _to_java_column(wkid),
        )).alias("geom")


def st_exterior_ring(
        geom: Union[Column, str],
) -> Column:
    """Get the exterior ring of a polygon.

    Extracts the outer boundary ring of a polygon, discarding any holes.

    :param geom: A polygon geometry or column name
    :type geom: Union[Column, str]
    :return: The exterior ring as a polyline with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("outer_ring", st_exterior_ring("polygon"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stExteriorRing(
            _to_java_column(geom),
        )).alias("geom")


def st_extent(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Get the extent (xmin, ymin, xmax, ymax) of a geometry.

    Returns the minimum bounding rectangle as an array [xmin, ymin, xmax, ymax].

    :param geom: A geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: Array of [xmin, ymin, xmax, ymax] with alias "extent"
    :rtype: Column

    Example::

        df.withColumn("bbox", st_extent("geom"))
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stExtent(
            _to_java_column(geom),
        )).alias("extent")


def st_simplify(
        geom: Union[Column, str] = "geom",
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Simplify a geometry by removing redundant vertices.

    Creates a topologically equivalent geometry with fewer vertices while
    preserving the overall shape.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: The simplified geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("simplified", st_simplify("complex_geom", 4326))
    """
    sc = SparkContext._active_spark_context
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stSimplify(
            _to_java_column(geom),
            _to_java_column(wkid),
        )
    ).alias("geom")


def st_repair(
        geom: Union[Column, str] = "geom",
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Repair a geometry by fixing topological errors.

    Attempts to fix invalid geometries such as self-intersections, incorrect
    ring orientations, and other topological issues.

    :param geom: The geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[Column, str, int]
    :return: The repaired geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("fixed", st_repair("invalid_geom", 4326))
    """
    sc = SparkContext._active_spark_context
    if isinstance(wkid, int):
        wkid = lit(str(wkid))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stRepair(
            _to_java_column(geom),
            _to_java_column(wkid),
        )
    ).alias("geom")


def st_dump(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Dump the parts of a multi-part geometry.

    Converts a multi-part geometry (MultiPoint, MultiLineString, MultiPolygon,
    GeometryCollection) into an array of single-part geometries.

    :param geom: The multi-part geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: Array of single-part geometries
    :rtype: Column

    Example::

        df.withColumn("parts", st_dump("multipolygon"))
    """
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stDump(
            _to_java_column(geom),
        )
    )


def st_dump_explode(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Explode the parts of a multi-part geometry into separate rows.

    Converts a multi-part geometry into multiple rows, each containing a
    single-part geometry.

    :param geom: The multi-part geometry or column name. Default is "geom"
    :type geom: Union[Column, str]
    :return: Exploded single-part geometry with alias "geom"
    :rtype: Column

    Example::

        df.withColumn("part", st_dump_explode("multipolygon"))
    """
    return explode(st_dump(geom)).alias("geom")


def gdb_polyline(
        shape: Union[Column, str] = "Shape",
) -> Column:
    """Convert a GDB shape column to a polyline.

    :param shape: GDB shape column or column name. Default is "Shape"
    :type shape: Union[Column, str]
    :return: Polyline geometry with alias "geom"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.gdbPolyline2(_to_java_column(shape))
    ).alias("geom")


def gdb_polyline2(
        shape: Union[Column, str] = "Shape",
) -> Column:
    """Convert a GDB shape column to a polyline (version 2).

    :param shape: GDB shape column or column name. Default is "Shape"
    :type shape: Union[Column, str]
    :return: Polyline geometry with alias "geom"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.gdbPolyline2(_to_java_column(shape))
    ).alias("geom")


def gdb_polygon(
        shape: Union[Column, str] = "Shape",
) -> Column:
    """Convert a GDB shape column to a polygon.

    :param shape: GDB shape column or column name. Default is "Shape"
    :type shape: Union[Column, str]
    :return: Polygon geometry with alias "geom"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.gdbPolygon2(_to_java_column(shape))
    ).alias("geom")


def gdb_polygon2(
        shape: Union[Column, str] = "Shape",
) -> Column:
    """Convert a GDB shape column to a polygon (version 2).

    :param shape: GDB shape column or column name. Default is "Shape"
    :type shape: Union[Column, str]
    :return: Polygon geometry with alias "geom"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.gdbPolygon2(_to_java_column(shape))
    ).alias("geom")


def gdb_polygonM(
        shape: Union[Column, str] = "Shape",
) -> Column:
    """Convert a GDB shape column to a polygon with M values.

    :param shape: GDB shape column or column name. Default is "Shape"
    :type shape: Union[Column, str]
    :return: Polygon M geometry with alias "geom"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.gdbPolygonM(_to_java_column(shape))
    ).alias("geom")


def gdb_polygonZ(
        shape: Union[Column, str] = "Shape",
) -> Column:
    """Convert a GDB shape column to a polygon with Z values.

    :param shape: GDB shape column or column name. Default is "Shape"
    :type shape: Union[Column, str]
    :return: Polygon Z geometry with alias "geom"
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.gdbPolygonZ(_to_java_column(shape))
    ).alias("geom")


def clip_line(
        line: Union[Column, str],
        cell: Union[Column, str, float, int],
) -> Column:
    """Clip a line geometry by a cell size.

    :param line: The line geometry or column name
    :type line: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :return: The clipped line geometry
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (float, int)):
        cell = lit(float(cell))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.clipLine(
            _to_java_column(line),
            _to_java_column(cell),
        ))


def st_clipline(
        line: Union[Column, str],
        cell: Union[Column, str, float, int],
) -> Column:
    """Clip a line geometry by a cell size.

    Alias for clip_line function.

    :param line: The line geometry or column name
    :type line: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :return: The clipped line geometry
    :rtype: Column

    Example:

        df.withColumn("clipped\", st_clipline("geom\", 10000.0))
    """
    return clip_line(line, cell)


def clip_line_dist(
        line: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
) -> Column:
    """Clip a line geometry by a cell size with optional padding.

    :param line: The line geometry or column name
    :type line: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding/offset in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :return: The clipped line geometry
    :rtype: Column
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    if isinstance(cell, (float, int)):
        cell = lit(float(cell))
    if isinstance(dist, (float, int)):
        dist = lit(float(dist))
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.clipLineDist(
            _to_java_column(line),
            _to_java_column(cell),
            _to_java_column(dist),
        ))


def st_cliplinedist(
        line: Union[Column, str],
        cell: Union[Column, str, float, int],
        dist: Union[Column, str, float, int] = 0.0,
) -> Column:
    """Clip a line geometry by a cell size with optional padding.

    Alias for clip_line_dist function.

    :param line: The line geometry or column name
    :type line: Union[Column, str]
    :param cell: The cell size in meters or column name
    :type cell: Union[Column, str, float, int]
    :param dist: The cell padding/offset in meters. Default is 0.0
    :type dist: Union[Column, str, float, int]
    :return: The clipped line geometry
    :rtype: Column

    Example:

        df.withColumn("clipped\", st_cliplinedist("geom\", 10000.0, 100.0))
    """
    return clip_line_dist(line, cell, dist)


def join_qr(
        lhs: DataFrame,
        rhs: DataFrame,
        cell: float,
        dist: float = 0.0,
        qr: str = "qr",
        oper: str = "none",
        wkid: Union[str, int] = -1,
        lhs_geom: str = "geom",
        rhs_geom: str = "geom",
        acceleration: str = "mild",
) -> DataFrame:
    """Spatially join dataframes using quadtree-based spatial indexing.

    This function performs an efficient spatial join between two DataFrames using
    a quadtree (QR) indexing strategy for improved performance on large datasets.

    :param lhs: The left hand side dataframe
    :type lhs: DataFrame
    :param rhs: The right hand side dataframe
    :type rhs: DataFrame
    :param cell: The QR cell size in meters
    :type cell: float
    :param dist: The QR offset/padding distance in meters. Default is 0.0
    :type dist: float
    :param qr: The name of the QR field in the dataframes. Default is "qr"
    :type qr: str
    :param oper: The spatial operation to perform ("none", "intersection", etc.). Default is "none"
    :type oper: str
    :param wkid: The spatial reference WKID. Default is -1
    :type wkid: Union[str, int]
    :param lhs_geom: The name of the left geometry field. Default is "geom"
    :type lhs_geom: str
    :param rhs_geom: The name of the right geometry field. Default is "geom"
    :type rhs_geom: str
    :param acceleration: The geometry acceleration mode: "mild", "medium", or "hot". Default is "mild"
    :type acceleration: str
    :return: DataFrame with joined results
    :rtype: DataFrame

    Example::

        result = join_qr(df1, df2, cell=10000.0, wkid=3857)
        result = join_qr(df1, df2, cell=10000.0, dist=100.0, oper="intersection")
    """
    if qr not in lhs.columns and qr not in rhs.columns:
        ldf = lhs.withColumnRenamed(lhs_geom, "lgeom").withColumn(
            "lqr", qr_envp_explode("lgeom", cell, dist)
        )
        rdf = rhs.withColumnRenamed(rhs_geom, "rgeom").withColumn(
            "rqr", qr_envp_explode("rgeom", cell, dist)
        )
        return (
            ldf
            .join(rdf, ldf.lqr.qr == rdf.rqr.qr)
            .filter(qr_intersect("lqr", "rqr", cell))
            .drop("lqr", "rqr")
        )
    else:
        sc = SparkContext._active_spark_context
        ss = lhs.sparkSession if hasattr(lhs, "sparkSession") else lhs.sql_ctx
        return DataFrame(
            sc._jvm.com.esri.spark.JoinQRInnerProcessor.apply(
                lhs._jdf,
                rhs._jdf,
                float(cell),
                qr,
                oper,
                str(wkid),
                lhs_geom,
                rhs_geom,
                acceleration,
            ),
            ss)


def to_feature_table(
        df: DataFrame,
        feature_table_name: str,
        workspace: str = "scratch",
) -> None:
    """Converts a Spark DataFrame to an ArcGIS feature table.

    :param df: Spark DataFrame to convert
    :type df: DataFrame
    :param feature_table_name: Name of the output feature table
    :type feature_table_name: str
    :param workspace: Workspace where the feature table will be created.
        Can be "memory", "scratch", or a path to a geodatabase.
        Default is "scratch"
    :type workspace: str
    :return: None
    :rtype: None

    Example::

        to_feature_table(df, "my_table", workspace="scratch")
        to_feature_table(df, "my_table", workspace="/path/to/my.gdb")
    """
    import arcpy

    if workspace == "scratch":
        workspace = arcpy.env.scratchGDB

    pdf = df.toPandas()
    tab = pa.Table.from_pandas(pdf)
    fc = os.path.join(workspace, feature_table_name)
    arcpy.management.Delete(fc)
    arcpy.management.CopyRows(tab, fc)


def to_feature_class(
        df: DataFrame,
        feature_class_name: str,
        geom: str = "geom",
        sp_ref: Optional[int] = 3857,
        workspace: str = "scratch",
) -> None:
    """Converts a Spark DataFrame to an ArcGIS feature class.

    :param df: Spark DataFrame to convert
    :type df: DataFrame
    :param feature_class_name: Name of the output feature class
    :type feature_class_name: str
    :param geom: Name of the geometry column with WKB values. Default is "geom"
    :type geom: str
    :param sp_ref: A spatial reference WKID. Default is 3857 (Web Mercator).
        If negative, gets the active map's spatial reference
    :type sp_ref: Optional[int]
    :param workspace: Workspace where the feature class will be created.
        Can be "memory", "scratch", or a path to a geodatabase.
        Default is "scratch"
    :type workspace: str
    :return: None
    :rtype: None

    Example::

        to_feature_class(df, "my_fc", geom="geom", sp_ref=4326)
        to_feature_class(df, "my_fc", workspace="/path/to/my.gdb")
    """
    import arcpy

    if workspace == "scratch":
        workspace = arcpy.env.scratchGDB

    pdf = df.withColumnRenamed(geom, "SHAPE").toPandas()
    if isinstance(sp_ref, int) and sp_ref <= 0:
        project = arcpy.mp.ArcGISProject("CURRENT")
        sp_ref = project.activeMap.spatialReference
    elif isinstance(sp_ref, int) and sp_ref > 0:
        sp_ref = arcpy.SpatialReference(sp_ref)
    metadata = {"esri.encoding": "WKB", "esri.sr_wkt": sp_ref.exportToString()}
    pa_shape = pa.field("SHAPE", pa.binary(), nullable=False, metadata=metadata)

    schema = pa.Schema.from_pandas(pdf)
    shape_index = schema.get_field_index("SHAPE")
    schema = schema.set(shape_index, pa_shape)
    tab = pa.Table.from_pandas(pdf, schema=schema)
    fc = os.path.join(workspace, feature_class_name)
    arcpy.management.Delete(fc)
    arcpy.management.CopyFeatures(tab, fc)


def to_spark(
        feature_class_name: str,
        fields: Optional[List[str] | str] = None,
        where_clause: Optional[str] = None,
) -> DataFrame:
    """Converts an ArcGIS feature class to a Spark DataFrame.

    The fields parameter can be:
        - None: Only [OBJECTID, SHAPE] fields (default)
        - "*": All fields from the feature class
        - List of field names: Specific fields to include

    :param feature_class_name: Name or path of the feature class
    :type feature_class_name: str
    :param fields: Fields to include in the DataFrame. Can be None, "*", or a list of field names.
        Default is None which includes only OBJECTID and SHAPE
    :type fields: Optional[Union[List[str], str]]
    :param where_clause: SQL where clause to filter features. Default is None (no filter)
    :type where_clause: Optional[str]
    :return: Spark DataFrame with the feature class data
    :rtype: DataFrame

    Example::

        df = to_spark("my_feature_class")  # Only OBJECTID and SHAPE
        df = to_spark("my_feature_class", fields="*")  # All fields
        df = to_spark("my_feature_class", fields=["OBJECTID", "SHAPE", "Name"])
        df = to_spark("my_feature_class", where_clause="Population > 100000")
    """
    import arcpy

    if fields is None:
        fields = ["OBJECTID", "SHAPE"]

    if fields == "*":
        fields = [f.name for f in arcpy.ListFields(feature_class_name)]

    if where_clause is None:
        where_clause = ""

    tab = arcpy.da.TableToArrowTable(feature_class_name, fields, where_clause, "WKB")
    return (SparkSession
            .builder
            .getOrCreate()
            .createDataFrame(tab.to_pandas())
            )


def pairwise_dissolve(
        buf: DataFrame,
        cell: float = 100_000.0,
        dist: float = 100.0,
        wkid: int = 3857,
        cell_mul: float = 10.0,
        breaker: int = 10,
        geom_name: str = "geom",
) -> DataFrame:
    """Dissolve the dataframe by cell using a progressive multi-resolution approach.

    This function performs iterative spatial dissolve operations, progressively
    increasing the cell size. It separates polygons that are fully contained in a QR
    cell from those that span multiple cells, processing them separately for efficiency.

    :param buf: A DataFrame with polygons to dissolve
    :type buf: DataFrame
    :param cell: The initial QR cell size in meters. Default is 100,000.0
    :type cell: float
    :param dist: The QR padding distance in meters. Default is 100.0
    :type dist: float
    :param wkid: The spatial reference WKID for the geometry. Default is 3857
    :type wkid: int
    :param cell_mul: The cell size multiplier for each iteration. Default is 10.0
    :type cell_mul: float
    :param breaker: Maximum number of iterations (circuit breaker). Default is 10
    :type breaker: int
    :param geom_name: The name of the geometry column. Default is "geom"
    :type geom_name: str
    :return: DataFrame with dissolved polygons
    :rtype: DataFrame

    Example::

        dissolved = pairwise_dissolve(df, cell=100000.0, wkid=3857)
        dissolved = buf.pairwise_dissolve(cell=50000.0, dist=50.0, breaker=15)
    """

    def _dissolve(
            df: DataFrame,
            cell_: float,
    ) -> DataFrame:
        """Dissolve the polygons in a dataframe given a current cell size.

        :param df: A reference to a dataframe with polygons
        :type df: DataFrame
        :param cell_: A QR cell size in meters
        :type cell_: float
        :return: A new dataframe with the dissolved polygons
        :rtype: DataFrame
        """
        return (
            df
            # Clip each polygon by its overlapping QR envp inflated by dist.
            .withColumn("qr", qr_geom_explode("geom", cell_, dist, wkid))
            # Get the QR and clipped geometry.
            .select("qr.qr", "qr.geom")
            # Collect all the clipped geometries by QR.
            .groupBy("qr")
            .agg(collect_list("geom").alias("col"))
            # Union each collection.
            .select("qr", st_union_col("col", wkid).alias("geom"))
            # Multipart to single part.
            .withColumn("geom", st_dump_explode("geom"))
            # Get only the exterior rings.
            .withColumn("geom", st_exterior_ring("geom"))
            # Set a flag indicating if the ring (the dissolved polygon) is fully in a QR.
            .withColumn("in_qr", qr_contains_geom("qr", cell_, "geom"))
            # .checkpoint(eager=True)
            .localCheckpoint()
        )

    dfu = None  # Resulting dataframe union.
    # Rename geometry column.
    rename = False
    if geom_name != "geom":
        buf = buf.withColumnRenamed(geom_name, "geom")
        rename = True
    # Circuit breaker, in case bottom count condition is not met and prevent inf loop.
    for n in range(0, breaker):
        # Dissolved the dataframe by cell.
        res = _dissolve(buf, cell)
        # Get all the dissolved polygons that are not in a QR.
        buf = res.filter(col("in_qr") == False).localCheckpoint()  # .checkpoint(eager=True)
        # Get all the dissolved polygons that are fully (by envelope) in a QR.
        res = res.filter(col("in_qr") == True).localCheckpoint()  # .checkpoint(eager=True)
        # If not first time through, keep union all the "inside qr" polygons.
        dfu = res if dfu is None else dfu.unionAll(res).localCheckpoint()  # .checkpoint(eager=True)
        # Increment the cell size.
        cell *= cell_mul
        # Check if there are no more polygons to dissolve.
        if buf.count() == 0:
            break

    if rename:
        dfu = dfu.withColumnRenamed("geom", geom_name)
    return dfu.drop("qr", "in_qr").localCheckpoint()  # .checkpoint(eager=True)


DataFrame.pairwise_dissolve = pairwise_dissolve
DataFrame.join_qr = join_qr
DataFrame.to_feature_table = to_feature_table
DataFrame.to_feature_class = to_feature_class
