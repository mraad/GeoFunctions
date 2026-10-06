package com.esri.spark

import com.esri.core.geometry.{Envelope, Envelope2D, Geometry, OperatorIntersection, Polygon, Polyline, SpatialReference}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

/**
 * The quad-region cell scan shared by QR_GEOM and QR_ENVPGEOM.
 *
 * Walks every cell of the `cell` grid that the geometry can reach, intersects the geometry
 * with the dist-inflated cell, and emits one row per non-degenerate result.
 */
object QRScan extends Serializable {

  private val operator = OperatorIntersection.local

  // Read from SparkEnv, not SparkSession: getActiveSession is empty on executors, which
  // pinned both knobs to their defaults everywhere the scan actually runs.
  private lazy val conf = Option(SparkEnv.get).map(_.conf)

  private lazy val accelerate: Boolean =
    conf.forall(_.getBoolean("spark.esri.accelerate", defaultValue = true))

  /** Cell count above which one geometry's scan fans out over the common ForkJoin pool. */
  private lazy val parallel: Int =
    conf.map(_.getInt("spark.esri.parallel", 64)).getOrElse(64)

  @inline
  private final def qrOf(q: Long, r: Long): Long = (q << 32) | (r & 0xFFFFFFFFL)

  /**
   * Intersect `geom` with cell (q,r) inflated by `dist`, using `cellEnvp` as scratch.
   * Returns null when the cell holds nothing of the geometry.
   */
  private final def clip(geom: Geometry,
                         sr: SpatialReference,
                         q: Long,
                         r: Long,
                         cell: Double,
                         dist: Double,
                         cellEnvp: Envelope
                        )(row: (Long, Envelope, Geometry) => InternalRow): InternalRow = {
    val xmin = q * cell - dist
    val ymin = r * cell - dist
    val chip = cell + dist + dist
    cellEnvp.setCoords(xmin, ymin, xmin + chip, ymin + chip)
    // Accelerating cellEnvp here would be pure overhead - it is a 4-corner rectangle.
    operator.execute(geom, cellEnvp, sr, null) match {
      case polygon: Polygon =>
        if (polygon.getPathCount > 0 && polygon.getPointCount > 2) row(qrOf(q, r), cellEnvp, polygon) else null
      case polyline: Polyline =>
        if (polyline.getPathCount > 0 && polyline.getPointCount > 1) row(qrOf(q, r), cellEnvp, polyline) else null
      case envelope: Envelope =>
        row(qrOf(q, r), cellEnvp, envelope)
      case _ =>
        null
    }
  }

  final def eval(bytes: Array[Byte],
                 cell: Double,
                 dist: Double,
                 wkid: UTF8String
                )(row: (Long, Envelope, Geometry) => InternalRow): ArrayData = {
    val geom = bytes.geom
    val envp = new Envelope2D()
    geom.queryEnvelope2D(envp)
    val range = QRRange(envp, cell, dist)
    if (range.count == 0) return ArrayData.toArrayData(Array.empty[InternalRow])
    val sr = SpatialReferenceObj.create(wkid)

    if (accelerate) {
      operator.accelerateGeometry(geom, sr, Geometry.GeometryAccelerationDegree.enumMild)
    }

    if (range.count > parallel) {
      // Index the cells rather than materializing a Seq of (q,r) tuples up front.
      // Each thread needs its own scratch envelope.
      val nrInt = (range.rmax - range.rmin).toInt
      // A Java parallel stream rather than `.par`: on 2.13 `.par` needs an import that does
      // not exist on 2.12, and this source tree cross-builds both. Each index is written once
      // by one thread, so the shared array needs no synchronization.
      val scratch = new Array[InternalRow](range.count)
      java.util.stream.IntStream.range(0, range.count).parallel().forEach((i: Int) =>
        scratch(i) = clip(geom, sr, range.qmin + i / nrInt, range.rmin + i % nrInt, cell, dist, new Envelope())(row)
      )
      ArrayData.toArrayData(scratch.filter(_ != null))
    } else {
      val arr = new ArrayBuffer[InternalRow](range.count)
      val cellEnvp = new Envelope()
      var q = range.qmin
      while (q < range.qmax) {
        var r = range.rmin
        while (r < range.rmax) {
          val ir = clip(geom, sr, q, r, cell, dist, cellEnvp)(row)
          if (ir != null) arr.append(ir)
          r += 1L
        }
        q += 1L
      }
      ArrayData.toArrayData(arr)
    }
  }
}
