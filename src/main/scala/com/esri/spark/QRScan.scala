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
    require(cell > 0.0, s"cell must be positive, got $cell")
    require(dist >= 0.0, s"dist must be non-negative, got $dist")

    val sr = SpatialReferenceObj.create(wkid)
    val geom = bytes.geom
    val envp = new Envelope2D()
    geom.queryEnvelope2D(envp)

    // Cell (q,r) spans [q*cell-dist, (q+1)*cell+dist] on each axis, so the candidate range
    // is the geometry extent inflated by dist. QRCount/QRList/QREnvp already do this; the
    // scan did not, and silently dropped the boundary cells whenever dist > 0.
    val qmin = ((envp.xmin - dist) / cell).floor.toLong
    val rmin = ((envp.ymin - dist) / cell).floor.toLong
    val qmax = ((envp.xmax + dist) / cell).floor.toLong + 1L
    val rmax = ((envp.ymax + dist) / cell).floor.toLong + 1L
    // qrOf packs q and r into the halves of a Long, so they must each fit in an Int or the
    // key is not round-trippable by QRAsGeom. Bounding the cell count is not enough: a far
    // from the origin geometry can cover few cells and still have a huge q or r.
    require(
      qmin >= Int.MinValue && qmax <= Int.MaxValue &&
        rmin >= Int.MinValue && rmax <= Int.MaxValue,
      s"cell range out of Int bounds: q=[$qmin,$qmax) r=[$rmin,$rmax) with cell=$cell dist=$dist"
    )
    val nq = qmax - qmin
    val nr = rmax - rmin
    // Check the factors before multiplying: nq * nr overflows Long for a small cell over a
    // large extent, and a wrapped positive product would slip past the count check below.
    require(nq > 0L && nr > 0L && nq <= Int.MaxValue && nr <= Int.MaxValue && nq * nr <= Int.MaxValue,
      s"${nq}x${nr} cells for one geometry with cell=$cell dist=$dist - use a larger cell")
    val count = nq * nr

    if (accelerate) {
      operator.accelerateGeometry(geom, sr, Geometry.GeometryAccelerationDegree.enumMild)
    }

    if (count > parallel) {
      // Index the cells rather than materializing a Seq of (q,r) tuples up front.
      // Each thread needs its own scratch envelope.
      val nrInt = nr.toInt
      // A Java parallel stream rather than `.par`: on 2.13 `.par` needs an import that does
      // not exist on 2.12, and this source tree cross-builds both. Each index is written once
      // by one thread, so the shared array needs no synchronization.
      val scratch = new Array[InternalRow](count.toInt)
      java.util.stream.IntStream.range(0, count.toInt).parallel().forEach((i: Int) =>
        scratch(i) = clip(geom, sr, qmin + i / nrInt, rmin + i % nrInt, cell, dist, new Envelope())(row)
      )
      ArrayData.toArrayData(scratch.filter(_ != null).toSeq)
    } else {
      val arr = new ArrayBuffer[InternalRow](count.toInt)
      val cellEnvp = new Envelope()
      var q = qmin
      while (q < qmax) {
        var r = rmin
        while (r < rmax) {
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
