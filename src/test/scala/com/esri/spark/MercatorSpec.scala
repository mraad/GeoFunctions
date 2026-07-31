package com.esri.spark

import com.esri.core.geometry.{Envelope, MultiVertexGeometry, Point, Polygon, Polyline}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * ST_MERCATOR / ST_WGS84 must agree between their Point and MultiVertexGeometry branches.
 * The Point branch of ST_MERCATOR was a copy of ST_WGS84's and un-projected instead of
 * projecting, so points came back ~1e10 times too small while polylines were correct.
 * Run with: mvn -DskipTests=false test
 */
class MercatorSpec extends AnyFlatSpec with Matchers {

  /** Web Mercator x of 180 degrees - the half-circumference of the projection. */
  private val XMax = 20037508.342789244

  private def point(x: Double, y: Double): Array[Byte] = {
    val p = new Point(x, y)
    p.bytes
  }

  private def line(x1: Double, y1: Double, x2: Double, y2: Double): Array[Byte] = {
    val l = new Polyline()
    l.startPath(x1, y1)
    l.lineTo(x2, y2)
    l.bytes
  }

  private def xyOf(bytes: Array[Byte]): (Double, Double) = {
    val g = bytes.geom.asInstanceOf[Point]
    (g.getX, g.getY)
  }

  "ST_MERCATOR" should "project a point, not un-project it" in {
    val (x, y) = xyOf(STMercatorObj.eval(point(180.0, 0.0)))
    x shouldBe XMax +- 1e-6
    y shouldBe 0.0 +- 1e-9
  }

  it should "agree with the scalar st_lontox/st_lattoy helpers" in {
    val lon = 23.6543
    val lat = 71.6109
    val (x, y) = xyOf(STMercatorObj.eval(point(lon, lat)))
    x shouldBe STLonToXObj.eval(lon) +- 1e-9
    y shouldBe STLatToYObj.eval(lat) +- 1e-9
  }

  it should "treat a point the same as a one-vertex polyline" in {
    val lon = -1.2894
    val lat = 55.2881
    val (px, py) = xyOf(STMercatorObj.eval(point(lon, lat)))
    val poly = STMercatorObj.eval(line(lon, lat, 0.0, 0.0)).geom.asInstanceOf[Polyline]
    px shouldBe poly.getPoint(0).getX +- 1e-9
    py shouldBe poly.getPoint(0).getY +- 1e-9
  }

  "ST_WGS84" should "invert ST_MERCATOR for a point" in {
    val lon = 20.1895
    val lat = 71.3101
    val (x, y) = xyOf(STWGS84Obj.eval(STMercatorObj.eval(point(lon, lat))))
    x shouldBe lon +- 1e-9
    y shouldBe lat +- 1e-9
  }

  it should "invert ST_MERCATOR for a polyline" in {
    val poly = STWGS84Obj.eval(STMercatorObj.eval(line(10.0, 20.0, 30.0, 40.0)))
      .geom.asInstanceOf[Polyline]
    poly.getPoint(0).getX shouldBe 10.0 +- 1e-9
    poly.getPoint(0).getY shouldBe 20.0 +- 1e-9
    poly.getPoint(1).getX shouldBe 30.0 +- 1e-9
    poly.getPoint(1).getY shouldBe 40.0 +- 1e-9
  }

  // An audit flagged the `case _ => bytes` fall-through as silently returning envelopes
  // unprojected, on the grounds that Envelope extends Geometry and not MultiVertexGeometry.
  // That is true of the class hierarchy but unreachable here: WKB has no envelope type, so
  // an Envelope exports as a Polygon and comes back through the MultiVertexGeometry branch.
  // These two pin that, so nobody adds a dead `case env: Envelope` branch to "fix" it.
  "An Envelope" should "arrive at ST_MERCATOR as a Polygon, never as an Envelope" in {
    val env = new Envelope(-10.0, -20.0, 10.0, 20.0)
    val roundTripped = env.bytes.geom
    roundTripped shouldBe a[Polygon]
    roundTripped shouldBe a[MultiVertexGeometry]
  }

  it should "therefore be projected, not passed through untouched" in {
    val env = new Envelope(-10.0, -20.0, 10.0, 20.0)
    val out = STMercatorObj.eval(env.bytes).geom
    val extent = new Envelope()
    out.queryEnvelope(extent)
    extent.getXMin shouldBe STLonToXObj.eval(-10.0) +- 1e-6
    extent.getXMax shouldBe STLonToXObj.eval(10.0) +- 1e-6
    extent.getYMin shouldBe STLatToYObj.eval(-20.0) +- 1e-6
    extent.getYMax shouldBe STLatToYObj.eval(20.0) +- 1e-6
  }
}
