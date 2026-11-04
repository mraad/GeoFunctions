package com.esri.spark

import com.esri.core.geometry.VertexDescription.Semantics
import com.esri.core.geometry.{Geometry, MultiPath, OperatorImportFromWkt, Polygon, WktImportFlags}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * ST_DUMP splits a geometry into parts. A polygon part is an exterior ring plus its holes; it
 * used to split by ring, so a hole came out as a solid polygon (a 80x80 square with a 20x20 hole
 * dumped to areas 6400 + 400 instead of 6000). ST_EXTERIOR_RING used to keep only the first
 * part's ring.
 * Run with: mvn -DskipTests=false -Dsuites=com.esri.spark.STDumpSpec test
 */
class STDumpSpec extends AnyFlatSpec with Matchers {

  private def wkt(text: String): Array[Byte] =
    OperatorImportFromWkt.local().execute(WktImportFlags.wktImportDefaults, Geometry.Type.Unknown, text, null).bytes

  private def dump(text: String): Seq[Geometry] = {
    val arr = STDumpObj.eval(wkt(text))
    (0 until arr.numElements()).map(i => arr.getBinary(i).geom)
  }

  private def area(g: Geometry): Double = g.calculateArea2D()

  private val donut = "POLYGON ((10 10, 90 10, 90 90, 10 90, 10 10), (40 40, 40 60, 60 60, 60 40, 40 40))"

  "ST_DUMP" should "keep a polygon's hole with its exterior ring" in {
    val parts = dump(donut)
    parts.size shouldBe 1
    parts.head.asInstanceOf[Polygon].getPathCount shouldBe 2
    area(parts.head) shouldBe 6000.0
  }

  it should "split a multipolygon into one polygon per part, each with its own holes" in {
    val parts = dump("MULTIPOLYGON (((10 10, 90 10, 90 90, 10 90, 10 10), (40 40, 40 60, 60 60, 60 40, 40 40))," +
      " ((100 0, 110 0, 110 10, 100 10, 100 0)))")
    parts.map(area) shouldBe Seq(6000.0, 100.0)
    parts.map(_.asInstanceOf[Polygon].getPathCount) shouldBe Seq(2, 1)
  }

  it should "split a polyline into one polyline per path" in {
    val parts = dump("MULTILINESTRING ((0 0, 1 1), (5 5, 6 6, 7 5))")
    parts.map(_.asInstanceOf[MultiPath].getPointCount) shouldBe Seq(2, 3)
  }

  it should "keep Z on every part" in {
    val parts = dump("MULTIPOLYGON Z (((0 0 1, 10 0 2, 10 10 3, 0 0 1)), ((20 0 4, 30 0 5, 30 10 6, 20 0 4)))")
    parts.size shouldBe 2
    parts.foreach(_.hasAttribute(Semantics.Z) shouldBe true)
    parts(1).asInstanceOf[Polygon].getPoint(0).getZ shouldBe 4.0
  }

  it should "return each point of a multipoint and nothing for an empty point" in {
    dump("MULTIPOINT ((1 2), (3 4))").size shouldBe 2
    dump("POINT EMPTY") shouldBe empty
  }

  /** Rings in file order, as gdb_polygon* builds them: outer rings CW, holes CCW. */
  private def rings(paths: Seq[(Double, Double, Double, Boolean)]): Polygon = {
    val p = new Polygon()
    paths.foreach { case (x, y, s, hole) =>
      p.startPath(x, y)
      if (hole) { p.lineTo(x + s, y); p.lineTo(x + s, y + s); p.lineTo(x, y + s) }
      else { p.lineTo(x, y + s); p.lineTo(x + s, y + s); p.lineTo(x + s, y) }
    }
    p
  }

  private def dumpGeom(p: Polygon): Seq[Geometry] = {
    val arr = STDumpObj.eval(p.bytes)
    (0 until arr.numElements()).map(i => arr.getBinary(i).geom)
  }

  it should "give a hole to the ring around it, wherever the file stores it" in {
    val ext1 = (0.0, 0.0, 100.0, false)
    val ext2 = (200.0, 0.0, 100.0, false)
    val hole = (40.0, 40.0, 20.0, true)
    dumpGeom(rings(Seq(ext1, ext2, hole))).map(area).sum shouldBe 19600.0
    dumpGeom(rings(Seq(hole, ext1, ext2))).map(area).sum shouldBe 19600.0
    dumpGeom(rings(Seq(ext1, ext2, hole))).map(area).toSet shouldBe Set(9600.0, 10000.0)
    dumpGeom(rings(Seq(hole, ext1, ext2))).map(area).toSet shouldBe Set(9600.0, 10000.0)
  }

  it should "give a hole touching its ring's edge to that ring" in {
    // The hole's first vertex is on the outer ring's left edge; the second ring is a decoy.
    val parts = dumpGeom(rings(Seq((0.0, 0.0, 100.0, false), (200.0, 0.0, 100.0, false), (0.0, 40.0, 20.0, true))))
    parts.map(area).toSet shouldBe Set(9600.0, 10000.0)
  }

  "ST_EXTERIOR_RING" should "keep the outer ring when an island sits inside a hole" in {
    val g = rings(Seq((0.0, 0.0, 100.0, false), (20.0, 20.0, 60.0, true), (40.0, 40.0, 20.0, false)))
    val out = STExteriorRingObj.eval(g.bytes).geom.asInstanceOf[Polygon]
    out.getPathCount shouldBe 1
    area(out) shouldBe 10000.0
  }

  it should "keep a ring that only touches a larger one at a corner" in {
    // (100,100) is the first vertex of the small ring and a corner of the large one.
    val g = rings(Seq((0.0, 0.0, 100.0, false), (100.0, 100.0, 10.0, false)))
    val out = STExteriorRingObj.eval(g.bytes).geom.asInstanceOf[Polygon]
    out.getPathCount shouldBe 2
    area(out) shouldBe 10100.0
  }

  it should "find the outer rings wherever the file stores them" in {
    val g = rings(Seq((40.0, 40.0, 20.0, true), (0.0, 0.0, 100.0, false), (200.0, 0.0, 100.0, false)))
    area(STExteriorRingObj.eval(g.bytes).geom) shouldBe 20000.0
  }

  it should "return an empty result for an empty polyline" in {
    Seq("LINESTRING EMPTY", "MULTILINESTRING EMPTY", "POLYGON EMPTY").foreach { text =>
      STExteriorRingObj.eval(wkt(text)).geom.isEmpty shouldBe true
    }
  }

  it should "keep every part's exterior ring and drop the holes" in {
    val g = STExteriorRingObj.eval(wkt("MULTIPOLYGON (((10 10, 90 10, 90 90, 10 90, 10 10), " +
      "(40 40, 40 60, 60 60, 60 40, 40 40)), ((100 0, 110 0, 110 10, 100 10, 100 0)))")).geom.asInstanceOf[Polygon]
    g.getPathCount shouldBe 2
    area(g) shouldBe 6500.0
  }
}
