import os
import pyarrow as pa
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import lit, array, explode, col, collect_list
from typing import Union, Optional, List


def st_register_functions() -> None:
    """Register ST_XXX SQL functions with active spark context.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    sc._jvm.com.esri.spark.Registry.registerFunctions()


def st_translate(
        geom: Union[Column, str] = "geom",
        dx: Union[Column, str, float] = 0.0,
        dy: Union[Column, str, float] = 0.0,
) -> Column:
    """Translate a geometry by dx, dy.

    :param geom: The geometry.
    :param dx: The x translation.
    :param dy: The y translation.
    :return: The translated geometry.
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
    ))


def st_area(geom: Union[Column, str] = "geom") -> Column:
    """Compute the area of a geometry.

    :param geom: The geometry.
    :return: The area.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stArea(
        _to_java_column(geom),
    ))


def st_length(geom: Union[Column, str] = "geom") -> Column:
    """Compute the length of a geometry.

    :param geom: The geometry.
    :return: The length.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stLength(
        _to_java_column(geom),
    ))


def st_point(
        x: Union[Column, str] = "x",
        y: Union[Column, str] = "y",
) -> Column:
    """Create a point from the coordinates (x, y).

    :param x: The x coordinate.
    :param y: The y coordinate.
    :return: A point instance.
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

    :param x1: The x coordinate.
    :param y1: The y coordinate.
    :param x2: The x coordinate.
    :param y2: The y coordinate.
    :return: A polyline instance.
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

    :param x1: The x coordinate.
    :param y1: The y coordinate.
    :param x2: The x coordinate.
    :param y2: The y coordinate.
    :return: A polygon instance.
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

    :param x: The left x coordinate.
    :param y: The lower y coordinate.
    :param w: The width of the cell.
    :param h: The height of the cell. If h is None, then h = w.
    :return: A polygon instance.
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
    ))


def st_box(
        x: Union[Column, str],
        y: Union[Column, str],
        h: Union[Column, str],
        v: Optional[Union[Column, str]] = None,
) -> Column:
    """Create a polygon rectangle with center at x/y with width = 2*h and height = 2*v.

    :param x: The center x coordinate.
    :param y: The center y coordinate.
    :param h: The horizontal padding of the box.
    :param v: The vertical padding of the box. If v is None, then v = h.
    :return: A polygon instance.
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
        geom: Union[Column, str] = "geom"
) -> Column:
    """Convert a geometry to a WKT string representation.

    :param geom: The geometry.
    :return: The string representation.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stAsText(
        _to_java_column(geom),
    ))


def st_asgeojson(
        geom: Union[Column, str] = "geom"
) -> Column:
    """Convert a geometry to a GeoJSON string representation.

    :param geom: The geometry.
    :return: The string representation.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stAsGeoJSON(
        _to_java_column(geom),
    ))


def st_fromtext(text: Union[Column, str]) -> Column:
    """Create a geometry from a WKT string representation.

    :param text: The WKT string representation.
    :return: The geometry.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stFromText(
        _to_java_column(text),
    ))


def st_lontox(lon: Union[Column, str] = "lon") -> Column:
    """Convert a longitude to an x coordinate in meters.

    :param lon: The longitude.
    :return: The x coordinate in meters.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stLonToX(
        _to_java_column(lon),
    )).alias("x")


def st_lattoy(lat: Union[Column, str] = "lat") -> Column:
    """Convert a latitude to a y coordinate in meters.

    :param lat: The latitude.
    :return: The y coordinate in meters.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stLatToY(
        _to_java_column(lat),
    )).alias("y")


def st_xtolon(x: Union[Column, str] = "x") -> Column:
    """Convert an x coordinate in meters to a longitude.

    :param x: The x coordinate in meters.
    :return: The longitude.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stXToLon(
        _to_java_column(x),
    )).alias("lon")


def st_ytolat(y: Union[Column, str] = "y") -> Column:
    """Convert a y coordinate in meters to a latitude.

    :param y: The y coordinate in meters.
    :return: The latitude.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stYToLat(
        _to_java_column(y),
    )).alias("lat")


def st_lontoq(
        lon: Union[Column, str],
        cell: Union[Column, str, float, int]
) -> Column:
    """Convert a longitude to a q value.

    :param lon: The longitude.
    :param cell: The cell size in meters.
    :return: The q value.
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
    """Convert a latitude to an r value.

    :param lat: The latitude.
    :param cell: The cell size in meters.
    :return: The r value.
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
    """Convert q value to x meters.

    :param q: The q value.
    :param cell: The cell size in meters.
    :param dist: The cell padding in meters.
    :return: meters.
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
    """Convert r value to y meters.

    :param r: The r value.
    :param cell: The cell size in meters.
    :param dist: The padding distance in meters.
    :return: meters.
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


def st_polyline(*points) -> Column:
    """Convert an array of st_point to a polyline.

    :param points: The array of st_point.
    :return: The polyline.
    """
    sc = SparkContext._active_spark_context
    if points:
        arr = array(list(points)) if len(points) > 1 else points[0]
        return Column(sc._jvm.com.esri.spark.GeoFunctions.stPolyline(
            _to_java_column(arr)
        )).alias("geom")
    else:
        raise ValueError("st_polyline expects a list of st_point instances.")


def st_multipoint(*points) -> Column:
    """Convert an array of st_point to a multipoint.

    :param points: The array of st_point.
    :return: The multipoint.
    """
    sc = SparkContext._active_spark_context
    if points:
        arr = array(list(points)) if len(points) > 1 else points[0]
        return Column(sc._jvm.com.esri.spark.GeoFunctions.stMultipoint(
            _to_java_column(arr)
        )).alias("geom")
    else:
        raise ValueError("st_multipoint expects a list of st_point instances.")


def st_polyline2(xy: Union[Column, str]) -> Column:
    """Convert a xy array to a polyline.

    :param xy: The xy array.
    :return: The polyline.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stPolyline2(
        _to_java_column(xy),
    )).alias("geom")


def st_polygon(*points) -> Column:
    """Convert an array of st_point to a polygon.

    :param points: The array of st_point.
    :return: The polygon.
    """
    sc = SparkContext._active_spark_context
    if points:
        arr = array(list(points)) if len(points) > 1 else points[0]
        return Column(sc._jvm.com.esri.spark.GeoFunctions.stPolygon(
            _to_java_column(arr)
        )).alias("geom")
    else:
        raise ValueError("st_polygon expects a list of st_point instances.")


def st_polygon2(xy: Union[Column, str]) -> Column:
    """Convert a xy array to a polygon.

    :param xy: The xy array.
    :return: The polygon.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(sc._jvm.com.esri.spark.GeoFunctions.stPolygon2(
        _to_java_column(xy),
    ))


def st_intersection(
        lhs: Union[Column, str] = "lgeom",
        rhs: Union[Column, str] = "rgeom",
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Compute the intersection of two geometries.

    :param lhs: The left hand side geometry.
    :param rhs: The right hand side geometry.
    :param wkid: The spatial reference ID.
    :return: The intersection.
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

    :param lhs: The left hand side geometry.
    :param rhs: The right hand side geometry.
    :param wkid: The spatial reference ID.
    :return: True if geometries intersect, false otherwise.
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
    """Check if two geometries intersect.

    :param geom: The left hand side geometry.
    :param xmin: The minimum x coordinate of the box.
    :param ymin: The minimum y coordinate of the box.
    :param xmax: The maximum x coordinate of the box.
    :param ymax: The maximum y coordinate of the box.
    :return: True if geometries intersect, false otherwise.
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
        sc._jvm.com.esri.spark.GeoFunctions.stIntersects(
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

    :param lhs: The left hand side geometry.
    :param rhs: The right hand side geometry.
    :param wkid: The spatial reference ID.
    :return: True if geometries overlap, false otherwise.
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

    :param lhs: The left hand side geometry.
    :param rhs: The right hand side geometry.
    :param wkid: The spatial reference ID.
    :return: True if lhs contains rhs, false otherwise.
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

    :param lhs: The left hand side geometry.
    :param rhs: The right hand side geometry.
    :param wkid: The spatial reference ID.
    :return: True if lhs is within rhs, false otherwise.
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

    :param lhs: The left hand side geometry.
    :param rhs: The right hand side geometry.
    :param wkid: The spatial reference ID.
    :return: True if lhs touches rhs, false otherwise.
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

    :param lhs: The left hand side geometry.
    :param rhs: The right hand side geometry.
    :param wkid: The spatial reference ID.
    :return: True if lhs is disjoint from the rhs, false otherwise.
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
    """Calculate the IoU of the lhs geom with rhs geom.

    :param lhs: The left hand side geometry.
    :param rhs: The right hand side geometry.
    :param wkid: The spatial reference ID of the geometries.
    :return: The IoU value.
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
        geom: Union[Column, str] = "geom"
) -> Column:
    """Check if geometry is empty.

    :param geom: The geometry.
    :return: True if geometry is empty, false otherwise.
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

    :param x1: The x coordinate of the first point.
    :param y1: The y coordinate of the first point.
    :param x2: The x coordinate of the second point.
    :param y2: The y coordinate of the second point.
    :return: The Euclidean distance.
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

    :param lhs: The left hand side geometry.
    :param rhs: The right hand side geometry.
    :return: The distance.
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
        cell: Union[Column, str, float],
        dist: Union[Column, str, float] = 0.0,
) -> Column:
    """Compute the qr/envp of a geometry.

    :param geom: The geometry.
    :param cell: The cell size.
    :param dist: The cell padding.
    :return: The qr/envp.
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


def qr_envp_explode(
        geom: Union[Column, str],
        cell: Union[Column, str, float],
        dist: Union[Column, str, float] = 0.0,
) -> Column:
    return explode(qr_envp(geom, cell, dist)).alias("qr", metadata={"cell": cell, "dist": dist})


def qr_clip(
        geom: Union[Column, str],
        cell: Union[Column, str, float],
        dist: Union[Column, str, float] = 0.0,
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Compute the qr/clip of a geometry.

    :param geom: The geometry.
    :param cell: The cell size.
    :param dist: The cell padding.
    :param wkid: The spatial reference ID.
    :return: The qr/clip.
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
        sc._jvm.com.esri.spark.GeoFunctions.qrClip(
            _to_java_column(geom),
            _to_java_column(cell),
            _to_java_column(dist),
            _to_java_column(wkid),
        )
    )


def qr_contains_geom(
        qr: Union[Column, str],
        cell: Union[Column, str, float],
        geom: Union[Column, str],
) -> Column:
    """Check if the geometry is fully inside a QR cell.

    :param qr: The QR code.
    :param cell: The cell size.
    :param geom: The geometry.
    :return: True if qr contains geom, false otherwise.
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


def qr_clip_explode(
        geom: Union[Column, str],
        cell: Union[Column, str, float],
        dist: Union[Column, str, float] = 0.0,
        wkid: Union[Column, str, int] = -1,
) -> Column:
    """Explode the qr/clip of a geometry.

    :param geom: The geometry.
    :param cell: The cell size.
    :param dist: The cell padding.
    :param wkid: The spatial reference ID.
    :return: The exploded qr/clip.
    """
    return explode(
        qr_clip(geom, cell, dist, wkid)
    ).alias("qr", metadata={"cell": cell, "dist": dist, "wkid": wkid})


def qr_list(
        geom: Union[Column, str],
        cell: Union[Column, str, float],
        dist: Union[Column, str, float] = 0.0,
) -> Column:
    """Compute the qr list of a geometry.

    :param geom: The geometry.
    :param cell: The cell size.
    :param dist: The cell padding.
    :return: The qr list.
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


def qr_intersect(
        lhs: Union[Column, str],
        rhs: Union[Column, str],
        cell: Union[Column, int, float],
) -> Column:
    """Check if the qr/envp of two geometries intersect.

    :param lhs: The lhs qr/envp.
    :param rhs: The rhs qr/envp.
    :param cell: The cell size.
    :return: True if qr/envp intersect, false otherwise.
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
    """Compute the qr value for a give x/y coordinate.

    :param x: The x coordinate.
    :param y: The y coordinate.
    :param cell: The cell size.
    :return: The qr value.
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


def st_x(
        geom: Union[Column, str] = "geom",
        index: Union[Column, str, int] = 0,
) -> Column:
    """Get the x coordinate of a geometry at a point index.

    :param geom: The geometry.
    :param index: The point index.
    :return: The x coordinate.
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

    :param geom: The geometry.
    :param index: The point index.
    :return: The y coordinate.
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

    :param x1: The x coordinate of the first point.
    :param y1: The y coordinate of the first point.
    :param x2: The x coordinate of the second point.
    :param y2: The y coordinate of the second point.
    :return: The Manhattan distance.
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
    """Compute the Haversine distance between two points.

    :param lon1: The longitude of the first point.
    :param lat1: The latitude of the first point.
    :param lon2: The longitude of the second point.
    :param lat2: The latitude of the second point.
    :return: The Haversine distance.
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
    """Convert x meters to a q value.

    :param x: The x coordinate in meters.
    :param cell: The cell size in meters.
    :return: The q value.
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
    """Convert y meters to an r value.

    :param y: The y coordinate in meters.
    :param cell: The cell size in meters.
    :return: The r value.
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
        index: Union[Column, str, int] = 0
) -> Column:
    """Get the x/y coordinate of a geometry at a point index.

    :param geom: The geometry.
    :param index: The point index.
    :return: The x/y coordinate.
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

    :param geom: The geometry.
    :return: The centroid as a point.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stCentroid(
            _to_java_column(geom),
        )).alias("centroid")


def st_centroid_xy(
        geom: Union[Column, str] = "geom",
) -> Column:
    """Get the centroid XY of a geometry.

    :param geom: The geometry.
    :return: The centroid as XY.
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

    :param geom: The geometry.
    :param distance: The distance to buffer.
    :param num_vertices: The number of vertices to use.
    :param wkid: The spatial reference ID.
    :return: The buffered geometry.
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
        geom: Union[Column, str] = "geom"
) -> Column:
    """Get the convex hull of a geometry.

    :param geom: The geometry.
    :return: The convexhull.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stConvexHull(
            _to_java_column(geom),
        )).alias("geom")


def st_mercator(
        geom: Union[Column, str] = "geom"
) -> Column:
    """Convert the coordinates from WGS84 to WebMercator.

    :param geom: The geometry.
    :return: The projected geometry.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stMercator(
            _to_java_column(geom),
        )).alias("geom")


def st_wgs84(
        geom: Union[Column, str] = "geom"
) -> Column:
    """Convert the coordinates from WebMercator to WGS84.

    :param geom: The geometry.
    :return: The projected geometry.
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

    :param coll: A collection of geometries.
    :param wkid: The spatial reference ID.
    :return: The union of the collection of geometries.
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


def st_exterior_ring(geom: Union[Column, str]) -> Column:
    """Get the exterior ring of a polygon.

    :param geom: A polygon.
    :return: The exterior ring of a polygon.
    """
    sc = SparkContext._active_spark_context
    assert sc is not None and sc._jvm is not None
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.stExteriorRing(
            _to_java_column(geom),
        )).alias("geom")


def st_extent(
        geom: Union[Column, str] = "geom"
) -> Column:
    """Get the extent (xmin,ymin,xmax,ymax) of a geometry.

    :param geom: A geometry.
    :return: The extent of the geometry.
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
    """Simplify a geometry.

    :param geom: The geometry.
    :param wkid: The spatial reference ID.
    :return: The simplified geometry.
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
    """Repair a geometry.

    :param geom: The geometry.
    :param wkid: The spatial reference ID.
    :return: The repaired geometry.
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
    """Dump the parts of a geometry.

    :param geom: The geometry to dump.
    :return: The parts of the geometry.
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
    """Explode the parts of a geometry.

    :param geom: The geometry to dump.
    :return: The exploded parts of the geometry.
    """
    return explode(st_dump(geom)).alias("geom")


def gdb_polyline(shape: Union[Column, str] = "Shape") -> Column:
    """Convert a GDB shape column to a polyline.

    :param shape: GDB shape column.
    :return: Polyline column.
    """
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.gdbPolyline2(_to_java_column(shape))
    ).alias("geom")


def gdb_polyline2(shape: Union[Column, str] = "Shape") -> Column:
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.gdbPolyline2(_to_java_column(shape))
    ).alias("geom")


def gdb_polygon(shape: Union[Column, str] = "Shape") -> Column:
    """Convert a GDB shape column to a polygon.

    :param shape: GDB shape column.
    :return: Polygon column.
    """
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.gdbPolygon2(_to_java_column(shape))
    ).alias("geom")


def gdb_polygon2(shape: Union[Column, str] = "Shape") -> Column:
    sc = SparkContext._active_spark_context
    return Column(
        sc._jvm.com.esri.spark.GeoFunctions.gdbPolygon2(_to_java_column(shape))
    ).alias("geom")


def join_qr(
        lhs: DataFrame,
        rhs: DataFrame,
        cell: float,
        dist: float = 0.0,
) -> DataFrame:
    """Spatially join dataframes.

    :param lhs: The left hand side dataframe.
    :param rhs: The right hand side dataframe.
    :param cell: The qr cell size.
    :param dist: The qr offset distance. Default is 0.0.
    """
    ldf = lhs.withColumnRenamed("geom", "lgeom").withColumn(
        "lqr", qr_envp_explode("lgeom", cell, dist)
    )
    rdf = rhs.withColumnRenamed("geom", "rgeom").withColumn(
        "rqr", qr_envp_explode("rgeom", cell, dist)
    )
    return (
        ldf
        .join(rdf, ldf.lqr.qr == rdf.rqr.qr)
        .filter(qr_intersect("lqr", "rqr", cell))
        .drop("lqr", "rqr")
    )


def to_feature_table(
        df: DataFrame,
        feature_table_name: str,
        workspace: str = "scratch",
) -> None:
    """Converts a Spark DataFrame to an arcgis feature table.

    :param df: Spark DataFrame.
    :param feature_table_name: Name of the output feature table.
    :param workspace: Workspace where the feature table will be created.
        Can be "memory", "scratch" or a path to a geodatabase.
        Default is "scratch".
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
    """Converts a Spark DataFrame to an arcgis feature class.

    :param df: Spark DataFrame.
    :param feature_class_name: Name of the output feature class.
    :param geom: Name of the geometry column with WKB values. Default is "geom".
    :param sp_ref: A spatial reference. Default is 3857. If negative, get active map SR.
    :param workspace: Workspace where the feature class will be created.
        Can be "memory", "scratch" or a path to a geodatabase.
        Default is "scratch".
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
        fields: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
) -> DataFrame:
    """Converts an arcgis feature class to a Spark DataFrame.

    :param feature_class_name: Name of the feature class.
    :param fields: List of fields to include in the DataFrame. Default is None.
    :param where_clause: SQL where clause. Default is None.
    """
    import arcpy

    if fields is None:
        fields = ["OBJECTID", "SHAPE"]

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
    """
    Dissolve the dataframe by cell while keeping the polygons that are not in a QR and those that are
    fully (by envelope) in a QR in separate lists.

    Args:
        buf: A dataframe with polygons to dissolve.
        cell: The initial cell size.
        dist: The padding distance.
        wkid: The spatial reference identifier for the geometry.
        cell_mul: The cell multiplier.
        breaker: max number of iterations. Acting as a circuit breaker :-)
        geom_name: The name of the geometry column.

    Returns:
        Dataframe with dissolved polygons.
    """

    def _dissolve(df: DataFrame, cell_: float) -> DataFrame:
        """
        Dissolve the polygons in a dataframe given a current cell size.

        Args:
            df: A reference to a dataframe with polygons.
            cell_: A cell size.

        Returns:
            A new dataframe with the dissolved polygons.
        """
        return (
            df
            # Clip each polygon by its overlapping QR envp inflated by dist.
            .withColumn("qr", qr_clip_explode("geom", cell_, dist, wkid))
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
