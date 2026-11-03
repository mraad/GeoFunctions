package com.esri.spark

import com.esri.core.geometry.Geometry

/**
 * Parses WKB, reusing the previous parse while the same array instance keeps arriving.
 *
 * A literal or a scalar subquery result reaches an expression as one `byte[]` instance on
 * every row, so `ST_INTERSECTS(pickup, <county polygon>)` parses the polygon once instead of
 * millions of times. Column values arrive as a fresh array per row and are parsed as before.
 * Relies on Spark never mutating a byte array it has already handed out.
 */
final class WkbCache {

  private final class Entry(val bytes: Array[Byte], val geom: Geometry, val reused: Boolean)

  // One immutable snapshot, so a racing reader at worst re-parses; it never pairs the wrong geometry.
  @volatile private var last: Entry = _

  /** The geometry, and whether `onReuse` should run: true exactly once, on the first reuse. */
  def apply(bytes: Array[Byte], onReuse: Geometry => Unit = _ => ()): Geometry = {
    val e = last
    if (e != null && (e.bytes eq bytes)) {
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
