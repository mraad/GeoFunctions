package com.esri.spark

import com.esri.core.geometry.{MultiPath, Point, Polygon}
import org.apache.spark.sql.catalyst.InternalRow

/**
 * Decodes the `Shape` struct of the gdb reader (`struct<xmin, ymin, xmax, ymax, parts, coords>`)
 * into an Esri multipath. `parts` holds the vertex count of each ring/path; `coords` holds the
 * vertices back to back, with as many values per vertex as the layer has dimensions:
 * 2 = (x, y), 3 = (x, y, z) on a Z layer or (x, y, m) on an M layer, 4 = (x, y, z, m).
 * The count is derived from the data, never assumed, so every GDB_* function reads every layer
 * type. Only a 3-value layer is ambiguous; `thirdIsM` (the function's own Z or M flavour)
 * decides it.
 */
private[spark] object GDBShapeObj {

  final def fill(path: MultiPath, row: InternalRow, wantZ: Boolean, wantM: Boolean, thirdIsM: Boolean): Array[Byte] = {
    val parts = row.getArray(4).toIntArray()
    val coords = row.getArray(5).toDoubleArray()
    val vertices = parts.sum
    val stride = if (vertices == 0) 2 else coords.length / vertices
    require(stride >= 2 && stride <= 4 && stride * vertices == coords.length,
      s"Shape coords (${coords.length} values) do not match parts ${parts.mkString("[", ",", "]")}")
    val zAt = if (wantZ && (stride == 4 || (stride == 3 && !thirdIsM))) 2 else -1
    val mAt = if (!wantM) -1 else if (stride == 4) 3 else if (stride == 3 && thirdIsM) 2 else -1
    // Esri rings close implicitly; the stored closing vertex would become a zero-length segment.
    val dropClosing = path.isInstanceOf[Polygon]
    var base = 0
    var p = 0
    while (p < parts.length) {
      val first = base
      var count = parts(p)
      if (dropClosing && count > 1 &&
        coords(base + (count - 1) * stride) == coords(first) &&
        coords(base + (count - 1) * stride + 1) == coords(first + 1)) count -= 1
      var n = 0
      while (n < count) {
        val i = base + n * stride
        val point = new Point(coords(i), coords(i + 1))
        if (zAt >= 0) point.setZ(coords(i + zAt))
        if (mAt >= 0) point.setM(coords(i + mAt))
        if (n == 0) path.startPath(point) else path.lineTo(point)
        n += 1
      }
      base += parts(p) * stride
      p += 1
    }
    path.bytes
  }
}
