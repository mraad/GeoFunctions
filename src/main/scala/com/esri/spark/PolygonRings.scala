package com.esri.spark

import com.esri.core.geometry.{Envelope2D, OperatorDisjoint, OperatorWithin, Polygon}

/**
 * Groups a polygon's rings by containment, not by their order. Ring order is not reliable:
 * gdb_polygon* keeps the order stored in the file, so a hole can follow the wrong exterior ring or
 * come first. An island inside a hole is its own exterior ring, nested in the outer one.
 */
private[spark] object PolygonRings {

  private final class Rings(src: Polygon) {
    private val n = src.getPathCount
    private val polygons = new Array[Polygon](n)
    private val envelopes = new Array[Envelope2D](n)

    private def polygon(i: Int): Polygon = {
      if (polygons(i) == null) {
        val p = src.createInstance().asInstanceOf[Polygon]
        p.addPath(src, i, true)
        polygons(i) = p
      }
      polygons(i)
    }

    private def envelope(i: Int): Envelope2D = {
      if (envelopes(i) == null) {
        val e = new Envelope2D()
        polygon(i).queryEnvelope2D(e)
        envelopes(i) = e
      }
      envelopes(i)
    }

    private def area(i: Int): Double = math.abs(src.calculateRingArea2D(i))

    /**
     * Whether ring i lies inside `ring`. Rings can touch but not cross, so the first vertex that
     * is not on the boundary decides; a touching vertex says nothing (a square sharing one corner
     * with a larger square is outside it).
     */
    private def inside(i: Int, ring: Int): Boolean = {
      val outer = polygon(ring)
      var v = src.getPathStart(i)
      val end = src.getPathEnd(i)
      while (v < end) {
        val point = src.getPoint(v)
        if (OperatorDisjoint.local().execute(outer, point, null, null)) return false
        if (OperatorWithin.local().execute(point, outer, null, null)) return true
        v += 1
      }
      false
    }

    /** The smallest of `candidates` that is larger than ring i and lies around it, or -1. */
    def innermostAround(i: Int, candidates: Array[Int]): Int = {
      val box = new Envelope2D()
      polygon(i).queryEnvelope2D(box)
      val own = area(i)
      var best = -1
      var bestArea = Double.PositiveInfinity
      var c = 0
      while (c < candidates.length) {
        val ring = candidates(c)
        val a = area(ring)
        if (ring != i && a > own && a < bestArea && envelope(ring).contains(box) && inside(i, ring)) {
          best = ring
          bestArea = a
        }
        c += 1
      }
      best
    }
  }

  /**
   * The largest ring is always exterior, since a hole is smaller than the ring around it; every
   * ring wound the same way is exterior too. Not Polygon.isExteriorRing: it calls ring 0 exterior
   * whatever its winding, so a hole stored first turns into a solid ring.
   */
  private def isExterior(src: Polygon): Int => Boolean = {
    val n = src.getPathCount
    val areas = Array.tabulate(n)(src.calculateRingArea2D)
    val sign = if (n == 0) 0.0 else math.signum(areas.maxBy(a => math.abs(a)))
    i => areas(i) != 0.0 && math.signum(areas(i)) == sign
  }

  /** Ring indices per part: an exterior ring, then its holes. A hole with no exterior is a part. */
  def parts(src: Polygon): Array[Array[Int]] = {
    val all = (0 until src.getPathCount).toArray
    val (outer, holes) = all.partition(isExterior(src))
    if (holes.isEmpty) all.map(Array(_))
    else if (outer.length == 1) Array(outer ++ holes)
    else {
      val rings = new Rings(src)
      val owner = holes.map(h => h -> rings.innermostAround(h, outer))
      outer.map(e => e +: owner.collect { case (h, o) if o == e => h }) ++
        owner.collect { case (h, -1) => Array(h) }
    }
  }

  /** Exterior rings not inside another exterior ring: the outline of the whole polygon. */
  def outermost(src: Polygon): Array[Int] = {
    val outer = (0 until src.getPathCount).toArray.filter(isExterior(src))
    if (outer.length <= 1) outer
    else {
      val rings = new Rings(src)
      outer.filter(rings.innermostAround(_, outer) < 0)
    }
  }
}
