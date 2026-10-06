package com.esri.spark

import com.esri.core.geometry.Envelope2D

/** Validated candidate bounds; upper bounds are exclusive and may be Int.MaxValue + 1. */
private[spark] final case class QRRange(qmin: Long, rmin: Long, qmax: Long, rmax: Long, count: Int)

private[spark] object QRRange {
  def apply(envp: Envelope2D, cell: Double, dist: Double): QRRange = {
    QRIntersectObj.validateCell(cell)
    require(java.lang.Double.isFinite(dist) && dist >= 0.0,
      s"dist must be finite and non-negative, got $dist")
    if (envp.isEmpty) return QRRange(0L, 0L, 0L, 0L, 0)

    // Validate before conversion: Double.toLong saturates, and adding one can wrap.
    val qmin = Math.floor((envp.xmin - dist) / cell)
    val rmin = Math.floor((envp.ymin - dist) / cell)
    val qmax = Math.floor((envp.xmax + dist) / cell)
    val rmax = Math.floor((envp.ymax + dist) / cell)
    require(
      qmin >= Int.MinValue && qmax <= Int.MaxValue &&
        rmin >= Int.MinValue && rmax <= Int.MaxValue,
      s"cell range out of Int bounds: q=[$qmin,$qmax] r=[$rmin,$rmax] with cell=$cell dist=$dist")

    val nq = qmax.toLong - qmin.toLong + 1L
    val nr = rmax.toLong - rmin.toLong + 1L
    // Bound the factors before multiplying so even the count check cannot overflow.
    require(nq > 0L && nr > 0L && nq <= Int.MaxValue && nr <= Int.MaxValue && nq * nr <= Int.MaxValue,
      s"${nq}x${nr} cells for one geometry with cell=$cell dist=$dist - use a larger cell")
    QRRange(qmin.toLong, rmin.toLong, qmax.toLong + 1L, rmax.toLong + 1L, (nq * nr).toInt)
  }
}
