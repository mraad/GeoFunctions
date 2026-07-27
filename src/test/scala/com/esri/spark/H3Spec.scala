package com.esri.spark

import com.esri.core.geometry.{Envelope2D, Polygon}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * H3_LATLNGTOCELL / H3_CELLTOBOUNDARY.
 *
 * There are deliberately no null-input specs: by contract the caller filters nulls before
 * invoking any GeoFunction, so these expressions skip the per-row null check.
 * Run with: mvn -DskipTests=false test
 */
class H3Spec extends AnyFlatSpec with Matchers {

  private val Lat = 39.9863
  private val Lng = -105.0
  private val Res = 7

  private def cell(lat: Double = Lat, lng: Double = Lng, res: Int = Res): Long =
    H3LatLngToCellObj.eval(lat, lng, res)

  "H3LatLngToCell" should "return a cell whose boundary contains the input point" in {
    val boundary = H3CellToBoundaryObj.eval(cell()).geom
    val envp = new Envelope2D()
    boundary.queryEnvelope2D(envp)
    envp.contains(Lng, Lat) shouldBe true
  }

  it should "return finer cells at higher resolutions" in {
    val coarse = H3CellToBoundaryObj.eval(cell(res = 5)).geom.asInstanceOf[Polygon].calculateArea2D()
    val fine = H3CellToBoundaryObj.eval(cell(res = 9)).geom.asInstanceOf[Polygon].calculateArea2D()
    fine should be < coarse
  }

  it should "evaluate through the expression tree" in {
    H3LatLngToCell(Seq(
      Literal(Lat, DoubleType),
      Literal(Lng, DoubleType),
      Literal(Res, IntegerType))).eval(null) shouldBe cell()
  }

  "H3CellToBoundary" should "produce a closed ring of 5 or 6 distinct vertices" in {
    val polygon = H3CellToBoundaryObj.eval(cell()).geom.asInstanceOf[Polygon]
    polygon.getPathCount shouldBe 1
    polygon.getPointCount should (be(5) or be(6)) // pentagon or hexagon
    polygon.calculateArea2D() should be > 0.0
  }

  it should "evaluate through the expression tree" in {
    val bytes = H3CellToBoundary(Seq(Literal(cell(), LongType))).eval(null)
    bytes.asInstanceOf[Array[Byte]].length should be > 0
  }

  it should "reject an invalid cell rather than inventing a polygon" in {
    an[Exception] should be thrownBy H3CellToBoundaryObj.eval(-1L)
  }
}
