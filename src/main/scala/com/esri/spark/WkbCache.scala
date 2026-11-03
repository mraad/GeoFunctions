package com.esri.spark

import com.esri.core.geometry.Geometry

/**
 * Parses WKB, reusing the previous parse while the same geometry keeps arriving.
 *
 * A literal or a scalar subquery reaches an expression as one `byte[]` instance on every row,
 * and a join repeats the build-side geometry as a fresh copy on every matching row. Both are
 * caught: same instance first, then equal bytes. So `ST_INTERSECTS(pickup, <county polygon>)`
 * parses the polygon once, not once per row. The byte compare is a vectorized intrinsic that
 * stops at the first difference (or at a length mismatch), so distinct column values cost
 * almost nothing extra.
 */
final class WkbCache {

  private final class Entry(val bytes: Array[Byte], val geom: Geometry, val reused: Boolean)

  // One immutable snapshot, so a racing reader at worst re-parses; it never pairs the wrong geometry.
  @volatile private var last: Entry = _

  /** The geometry, and whether `onReuse` should run: true exactly once, on the first reuse. */
  def apply(bytes: Array[Byte], onReuse: Geometry => Unit = _ => ()): Geometry = {
    val e = last
    if (e != null && ((e.bytes eq bytes) || java.util.Arrays.equals(e.bytes, bytes))) {
      if (!e.reused) {
        onReuse(e.geom)
        last = new Entry(bytes, e.geom, reused = true)
      }
      e.geom
    } else {
      val geom = bytes.geom
      last = new Entry(bytes, geom, reused = false)
      geom
    }
  }
}
