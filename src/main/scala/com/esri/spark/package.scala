package com.esri

import com.esri.core.geometry.Geometry.Type
import com.esri.core.geometry.{Geometry, OperatorExportToWkb, OperatorImportFromWkb, Point, ShapeExportFlags, ShapeImportFlags}

import java.nio.{ByteBuffer, ByteOrder}

package object spark {

  implicit class GeometryImplicits(val geometry: Geometry) extends AnyVal {
    @inline
    final def bytes: Array[Byte] =
      OperatorExportToWkb.local
        .execute(ShapeExportFlags.ShapeExportDefaults, geometry, null).array()
  }

  implicit class BytesImplicits(val bytes: Array[Byte]) extends AnyVal {
    @inline
    final def geom: Geometry = {
      val p = pointOrNull(bytes)
      if (p != null) p
      else OperatorImportFromWkb.local
        .execute(
          ShapeImportFlags.ShapeImportDefaults,
          Type.Unknown,
          ByteBuffer.wrap(bytes),
          null)
    }
  }

  /**
   * A plain 2D WKB point (21 bytes, type 1, either byte order) decoded directly, or null for
   * anything else. Points dominate per-row parsing (pickups, dropoffs), and the general importer
   * costs a ByteBuffer, a cursor and dispatch per call. NaN coordinates (WKB's empty point) and
   * every other shape go to OperatorImportFromWkb, so the result never differs from it.
   */
  private def pointOrNull(b: Array[Byte]): Point = {
    if (b.length != 21 || (b(0) != 0 && b(0) != 1)) return null
    val bb = ByteBuffer.wrap(b).order(if (b(0) == 1) ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN)
    if (bb.getInt(1) != 1) return null
    val x = bb.getDouble(5)
    val y = bb.getDouble(13)
    if (java.lang.Double.isNaN(x) || java.lang.Double.isNaN(y)) null else new Point(x, y)
  }

}
