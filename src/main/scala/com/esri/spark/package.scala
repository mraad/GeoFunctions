package com.esri

import com.esri.core.geometry.Geometry.Type
import com.esri.core.geometry.{Geometry, OperatorExportToWkb, OperatorImportFromWkb, ShapeExportFlags, ShapeImportFlags}

import java.nio.ByteBuffer

package object spark {

  implicit class GeometryImplicits(val geometry: Geometry) extends AnyVal {
    @inline
    final def bytes: Array[Byte] =
      OperatorExportToWkb.local
        .execute(ShapeExportFlags.ShapeExportDefaults, geometry, null).array()
  }

  implicit class BytesImplicits(val bytes: Array[Byte]) extends AnyVal {
    @inline
    final def geom: Geometry =
      OperatorImportFromWkb.local
        .execute(
          ShapeImportFlags.ShapeImportDefaults,
          Type.Unknown,
          ByteBuffer.wrap(bytes),
          null)
  }

}
