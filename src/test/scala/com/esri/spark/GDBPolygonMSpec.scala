package com.esri.spark

import com.esri.core.geometry.VertexDescription.Semantics
import com.esri.core.geometry.Polygon
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * GDB_POLYGONM decodes the (x, y, z, m) coord stream the gdb/shp readers hand back.
 * It used to build `new Point(x, y, m)` - the only 3-arg Point ctor sets Z, so the measure
 * was written into Z and the real Z was dropped. Nothing threw; ST_ASTEXT just printed Z.
 * Run with: mvn -DskipTests=false test
 */
class GDBPolygonMSpec extends AnyFlatSpec with Matchers {

  /** The Shape struct is read positionally: parts at index 4, coords at index 5. */
  private def shapeRow(parts: Array[Int], coords: Array[Double]): GenericInternalRow =
    new GenericInternalRow(
      Array[Any](null, null, null, null, ArrayData.toArrayData(parts), ArrayData.toArrayData(coords)))

  /** Two vertices, stride 4: (x, y, z, m). */
  private val row = shapeRow(
    Array(2),
    Array(0.0, 0.0, 100.0, 7.0,
      1.0, 1.0, 200.0, 9.0))

  private def decoded: Polygon = GDBPolygonMObj.eval(row).geom.asInstanceOf[Polygon]

  "GDB_POLYGONM" should "carry the measure in M, not in Z" in {
    val p = decoded
    p.hasAttribute(Semantics.M) shouldBe true
    p.getPoint(0).getM shouldBe 7.0 +- 1e-9
    p.getPoint(1).getM shouldBe 9.0 +- 1e-9
  }

  it should "keep the Z ordinate the coord stream actually carries" in {
    val p = decoded
    p.hasAttribute(Semantics.Z) shouldBe true
    p.getPoint(0).getZ shouldBe 100.0 +- 1e-9
    p.getPoint(1).getZ shouldBe 200.0 +- 1e-9
  }

  it should "not confuse the two - a measure must never equal its own Z" in {
    // The old bug made getZ return the measure. Pin the exact values so a regression that
    // swaps them back cannot pass by coincidence.
    val p = decoded
    p.getPoint(0).getZ should not be p.getPoint(0).getM
    p.getPoint(1).getZ should not be p.getPoint(1).getM
  }

  it should "read x and y from the stride-4 stream" in {
    val p = decoded
    p.getPoint(0).getX shouldBe 0.0 +- 1e-9
    p.getPoint(0).getY shouldBe 0.0 +- 1e-9
    p.getPoint(1).getX shouldBe 1.0 +- 1e-9
    p.getPoint(1).getY shouldBe 1.0 +- 1e-9
  }

  it should "walk every part of a multi-part shape" in {
    val two = shapeRow(
      Array(2, 2),
      Array(0.0, 0.0, 10.0, 1.0,
        1.0, 1.0, 20.0, 2.0,
        5.0, 5.0, 30.0, 3.0,
        6.0, 6.0, 40.0, 4.0))
    val p = GDBPolygonMObj.eval(two).geom.asInstanceOf[Polygon]
    p.getPathCount shouldBe 2
    p.getPointCount shouldBe 4
    p.getPoint(2).getM shouldBe 3.0 +- 1e-9
    p.getPoint(3).getZ shouldBe 40.0 +- 1e-9
  }
}
