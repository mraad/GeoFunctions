package com.esri.spark

import com.esri.core.geometry.Geometry

/**
 * Parses WKB, reusing a recent parse when the same geometry arrives again.
 *
 * A literal or a scalar subquery reaches an expression as one `byte[]` instance on every row,
 * and a join repeats a build-side geometry as a fresh copy on every matching row, often
 * interleaved with a few others (each trip near a county border meets the same 2-3 counties).
 * The last [[WkbCache.Slots]] geometries are kept, matched by instance first and then by equal
 * bytes, so `ST_WITHIN(pickup, <county>)` parses each county once, not once per row. The byte
 * compare is a vectorized intrinsic that stops at a length mismatch or the first differing
 * byte, so distinct column values cost a few short compares.
 */
final class WkbCache {

  private final class Entry(val bytes: Array[Byte], val geom: Geometry, val reused: Boolean)

  // Per task and single-threaded: each task deserializes its own evaluator, and with it this cache.
  private val slots = new Array[Entry](WkbCache.Slots)
  private var next = 0

  /** The geometry; `onReuse` runs once per cached geometry, on its first reuse. */
  def apply(bytes: Array[Byte], onReuse: Geometry => Unit = _ => ()): Geometry = {
    var i = 0
    while (i < slots.length) {
      val e = slots(i)
      if (e != null && ((e.bytes eq bytes) || java.util.Arrays.equals(e.bytes, bytes))) {
        if (!e.reused) {
          onReuse(e.geom)
          slots(i) = new Entry(e.bytes, e.geom, reused = true)
        }
        return e.geom
      }
      i += 1
    }
    val geom = bytes.geom
    slots(next) = new Entry(bytes, geom, reused = false)
    next = (next + 1) % slots.length
    geom
  }
}

object WkbCache {
  /** Enough for the handful of build-side geometries one streamed row meets in a join. */
  val Slots = 4
}
