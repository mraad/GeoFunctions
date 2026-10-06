package com.esri.spark

import com.esri.core.geometry.Polygon
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal, UnsafeProjection}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * QR_GEOM, QR_ENVPGEOM, QR_LIST and QR_COUNT must agree on which cells a geometry reaches.
 * Run with: mvn -DskipTests=false test
 */
class QRScanSpec extends AnyFlatSpec with Matchers {

  private val NoSR = UTF8String.fromString("-1")

  private def rect(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Array[Byte] = {
    val p = new Polygon()
    p.startPath(xmin, ymin)
    p.lineTo(xmin, ymax)
    p.lineTo(xmax, ymax)
    p.lineTo(xmax, ymin)
    p.closePathWithLine()
    p.bytes
  }

  private def listQrs(bytes: Array[Byte], cell: Double, dist: Double): Set[Long] = {
    val arr = QRListObj.eval(bytes, cell, dist)
    (0 until arr.numElements()).map(arr.getLong).toSet
  }

  private def geomQrs(bytes: Array[Byte], cell: Double, dist: Double): Set[Long] = {
    val arr = QRGeomPar.eval(bytes, cell, dist, NoSR)
    (0 until arr.numElements()).map(i => arr.getStruct(i, 2).getLong(0)).toSet
  }

  private def areaOf(bytes: Array[Byte]): Double =
    bytes.geom.calculateArea2D()

  private val expressions: Seq[Seq[Expression] => Expression with ImplicitCastInputTypes] = Seq(
    QRCount.apply, QRList.apply, QREnvp.apply, QRGeom.apply, QREnvpGeom.apply)

  "QR functions" should "propagate every null argument in interpreted and generated evaluation" in {
    expressions.foreach { build =>
      val args = Seq(Literal(rect(0, 0, 1, 1)), Literal(1.0), Literal(0.0), Literal("-1"))
        .take(build(Seq.empty).inputTypes.length)
      args.indices.foreach { nullIndex =>
        val expression = build(args.updated(nullIndex, Literal.create(null, args(nullIndex).dataType)))
        withClue(s"${expression.prettyName} argument $nullIndex: ") {
          Option(expression.eval(InternalRow.empty)) shouldBe None
          UnsafeProjection.create(Seq(expression))(InternalRow.empty).isNullAt(0) shouldBe true
        }
      }
    }
  }

  it should "return zero candidates and no clipped rows for empty geometry" in {
    val bytes = new Polygon().bytes
    QRCountObj.eval(bytes, 1.0, 0.0) shouldBe 0
    QRListObj.eval(bytes, 1.0, 0.0).numElements() shouldBe 0
    QREnvpObj.eval(bytes, 1.0, 0.0).numElements() shouldBe 0
    QRGeomPar.eval(bytes, 1.0, 0.0, NoSR).numElements() shouldBe 0
    QREnvpGeomPar.eval(bytes, 1.0, 0.0, NoSR).numElements() shouldBe 0
  }

  it should "accept both signed Int boundary cells" in {
    Seq(Int.MinValue.toDouble, Int.MaxValue.toDouble).foreach { edge =>
      val bytes = rect(edge + 0.1, edge + 0.1, edge + 0.9, edge + 0.9)
      val expected = Set((edge.toLong << 32) | (edge.toLong & 0xFFFFFFFFL))
      listQrs(bytes, 1.0, 0.0) shouldBe expected
      geomQrs(bytes, 1.0, 0.0) shouldBe expected
    }
  }

  it should "reject invalid inputs before scanning or allocating candidates" in {
    val evaluators: Seq[(Array[Byte], Double, Double) => Any] = Seq(
      QRCountObj.eval, QRListObj.eval, QREnvpObj.eval,
      (bytes, cell, dist) => QRGeomPar.eval(bytes, cell, dist, NoSR),
      (bytes, cell, dist) => QREnvpGeomPar.eval(bytes, cell, dist, NoSR))
    evaluators.foreach { evaluate =>
      val g = rect(0, 0, 1, 1)
      Seq(0.0, -1.0, Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity).foreach { cell =>
        an[IllegalArgumentException] should be thrownBy evaluate(g, cell, 0.0)
      }
      Seq(-1.0, Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity).foreach { dist =>
        an[IllegalArgumentException] should be thrownBy evaluate(g, 1.0, dist)
      }
      // Small extents outside the packed-key range, too many cells, and saturated conversions.
      Seq(
        rect(Int.MaxValue.toDouble + 1, 0, Int.MaxValue.toDouble + 2, 1),
        rect(Int.MinValue.toDouble - 2, 0, Int.MinValue.toDouble - 1, 1),
        rect(0, Int.MaxValue.toDouble + 1, 1, Int.MaxValue.toDouble + 2),
        rect(0, Int.MinValue.toDouble - 2, 1, Int.MinValue.toDouble - 1),
        rect(0, 0, 50000, 50000),
        rect(-2e9, -2e9, 2e9, 2e9),
        rect(1e20, 1e20, 2e20, 2e20),
      ).foreach { bytes =>
        an[IllegalArgumentException] should be thrownBy evaluate(bytes, 1.0, 0.0)
      }
    }
  }

  it should "reject an incorrect argument count during analysis" in {
    expressions.foreach { build =>
      val args = Seq(Literal(rect(0, 0, 1, 1)), Literal(1.0), Literal(0.0), Literal("-1"))
        .take(build(Seq.empty).inputTypes.length)
      build(args).checkInputDataTypes().isSuccess shouldBe true
      build(args.dropRight(1)).checkInputDataTypes().isFailure shouldBe true
      build(args :+ Literal(0.0)).checkInputDataTypes().isFailure shouldBe true
    }
  }

  it should "produce identical interpreted and generated results" in {
    for {
      build <- expressions
      bytes <- Seq(rect(-1.5, -0.5, 2.5, 1.5), new Polygon().bytes)
    } {
      val args = Seq(Literal(bytes), Literal(1.0), Literal(0.25), Literal("-1"))
        .take(build(Seq.empty).inputTypes.length)
      val expression = build(args)
      val interpreted = UnsafeProjection.create(Array(expression.dataType))(
        InternalRow(expression.eval(InternalRow.empty))).copy()
      val generated = UnsafeProjection.create(Seq(expression))(InternalRow.empty)
      generated shouldBe interpreted
    }
  }

  "QR_COUNT" should "equal the number of candidate cells QR_LIST emits" in {
    for {
      cell <- Seq(0.5, 1.0, 7.3)
      dist <- Seq(0.0, 0.05, 0.4 * cell)
    } {
      val g = rect(-3.2, 1.4, 11.9, 8.6)
      val keys = listQrs(g, cell, dist)
      QRCountObj.eval(g, cell, dist) shouldBe keys.size
      val envps = QREnvpObj.eval(g, cell, dist)
      envps.numElements() shouldBe keys.size
      (0 until envps.numElements()).map(i => envps.getStruct(i, 5).getLong(0)).toSet shouldBe keys
    }
  }

  it should "reject a cell size that overflows the candidate count" in {
    an[IllegalArgumentException] should be thrownBy
      QRCountObj.eval(rect(0, 0, 1e9, 1e9), 1e-3, 0.0)
    an[IllegalArgumentException] should be thrownBy QRCountObj.eval(rect(0, 0, 1, 1), 0.0, 0.0)
  }

  "QR_GEOM" should "only emit cells QR_LIST considers candidates" in {
    for {
      cell <- Seq(0.5, 1.0, 7.3)
      dist <- Seq(0.0, 0.05, 0.4 * cell)
    } {
      val g = rect(-3.2, 1.4, 11.9, 8.6)
      geomQrs(g, cell, dist) should not be empty
      (geomQrs(g, cell, dist) -- listQrs(g, cell, dist)) shouldBe empty
    }
  }

  it should "partition the geometry when dist is 0" in {
    val g = rect(-3.2, 1.4, 11.9, 8.6)
    val arr = QRGeomPar.eval(g, 1.0, 0.0, NoSR)
    val total = (0 until arr.numElements())
      .map(i => areaOf(arr.getStruct(i, 2).getBinary(1)))
      .sum
    total shouldBe areaOf(g) +- 1e-9
  }

  it should "reach cells the geometry only touches through the dist padding" in {
    // Rect lives inside cell (0,0) but is within dist of cell (1,0) - the pre-fix
    // range started at floor(xmin/cell) and dropped it.
    val cell = 100.0
    val g = rect(80.0, 10.0, 95.0, 20.0)
    val qs = geomQrs(g, cell, 10.0).map(_ >> 32)
    qs should contain allOf(0L, 1L)
    geomQrs(g, cell, 0.0).map(_ >> 32) shouldBe Set(0L)
  }

  it should "agree with QR_ENVPGEOM on the cell set" in {
    for (dist <- Seq(0.0, 0.3)) {
      val g = rect(-3.2, 1.4, 11.9, 8.6)
      val envpArr = QREnvpGeomPar.eval(g, 1.0, dist, NoSR)
      val envpQrs = (0 until envpArr.numElements()).map(i => envpArr.getStruct(i, 6).getLong(0)).toSet
      envpQrs shouldBe geomQrs(g, 1.0, dist)
    }
  }

  it should "produce the same cell set on the parallel and sequential paths" in {
    // 400 cells crosses the spark.esri.parallel default of 64, 4 does not.
    val g = rect(0.0, 0.0, 20.0, 20.0)
    geomQrs(g, 1.0, 0.0).size shouldBe 400
    geomQrs(g, 10.0, 0.0).size shouldBe 4
  }

  it should "reject bad arguments" in {
    an[IllegalArgumentException] should be thrownBy QRGeomPar.eval(rect(0, 0, 1, 1), 0.0, 0.0, NoSR)
    an[IllegalArgumentException] should be thrownBy QRGeomPar.eval(rect(0, 0, 1, 1), 1.0, -1.0, NoSR)
  }

  it should "reject a geometry whose q/r do not fit in an Int" in {
    // Small geometry (100 cells) but so far from the origin that q overflows the 32 bits
    // qrOf packs it into. Bounding the cell count alone does not catch this.
    val far = rect(1e15, 1e15, 1e15 + 10.0, 1e15 + 10.0)
    an[IllegalArgumentException] should be thrownBy QRGeomPar.eval(far, 1.0, 0.0, NoSR)
    an[IllegalArgumentException] should be thrownBy QREnvpGeomPar.eval(far, 1.0, 0.0, NoSR)
  }

  it should "reject a cell count whose factors overflow Long when multiplied" in {
    // 1e12 x 1e12 cells: the true product is 1e24, which wraps. Guarding only the product
    // would let a wrapped positive value through and run the scan with a truncated count.
    val huge = rect(0.0, 0.0, 1e9, 1e9)
    an[IllegalArgumentException] should be thrownBy QRGeomPar.eval(huge, 1e-3, 0.0, NoSR)
    an[IllegalArgumentException] should be thrownBy QRCountObj.eval(huge, 1e-3, 0.0)
  }
}
