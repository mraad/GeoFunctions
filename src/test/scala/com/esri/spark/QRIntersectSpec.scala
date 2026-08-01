package com.esri.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, UnsafeProjection}
import org.apache.spark.sql.types.DoubleType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class QRIntersectSpec extends AnyFlatSpec with Matchers {

  private def qr(q: Int, r: Int): Long =
    (q.toLong << 32) | (r.toLong & 0xFFFFFFFFL)

  private def envp(key: Long,
                   xmin: Double,
                   ymin: Double,
                   xmax: Double,
                   ymax: Double
                  ): InternalRow =
    InternalRow(key, xmin, ymin, xmax, ymax)

  "QR_INTERSECT" should "select exactly the lower-left cell of an envelope intersection" in {
    val lhs = new com.esri.core.geometry.Envelope2D(0.0, 0.0, 2.0, 2.0)
    val rhs = new com.esri.core.geometry.Envelope2D(1.0, 0.25, 3.0, 1.5)

    QRIntersectObj.eval(lhs, rhs, 1.0, qr(1, 0)) shouldBe true
    QRIntersectObj.eval(lhs, rhs, 1.0, qr(0, 0)) shouldBe false
    QRIntersectObj.eval(lhs, rhs, 1.0, qr(1, 1)) shouldBe false
  }

  it should "handle negative cells and reject disjoint envelopes" in {
    val lhs = new com.esri.core.geometry.Envelope2D(-2.0, -2.0, 0.0, 0.0)
    val rhs = new com.esri.core.geometry.Envelope2D(-1.25, -0.75, 1.0, 1.0)
    val disjoint = new com.esri.core.geometry.Envelope2D(3.0, 3.0, 4.0, 4.0)

    QRIntersectObj.eval(lhs, rhs, 1.0, qr(-2, -1)) shouldBe true
    QRIntersectObj.eval(lhs, rhs, 1.0, qr(-1, -1)) shouldBe false
    QRIntersectObj.eval(lhs, disjoint, 1.0, qr(0, 0)) shouldBe false
  }

  it should "agree with reference QR packing across varied envelope intersections" in {
    val random = new Random(0x51A7L)
    for (_ <- 0 until 1000) {
      val cell = 0.1 + random.nextDouble() * 50.0
      val lhsXMin = random.nextDouble() * 2000.0 - 1000.0
      val lhsYMin = random.nextDouble() * 2000.0 - 1000.0
      val rhsXMin = lhsXMin + random.nextDouble() * 40.0 - 20.0
      val rhsYMin = lhsYMin + random.nextDouble() * 40.0 - 20.0
      val lhs = new com.esri.core.geometry.Envelope2D(
        lhsXMin, lhsYMin,
        lhsXMin + random.nextDouble() * 30.0,
        lhsYMin + random.nextDouble() * 30.0,
      )
      val rhs = new com.esri.core.geometry.Envelope2D(
        rhsXMin, rhsYMin,
        rhsXMin + random.nextDouble() * 30.0,
        rhsYMin + random.nextDouble() * 30.0,
      )
      val xmin = Math.max(lhs.xmin, rhs.xmin)
      val ymin = Math.max(lhs.ymin, rhs.ymin)
      val xmax = Math.min(lhs.xmax, rhs.xmax)
      val ymax = Math.min(lhs.ymax, rhs.ymax)

      if (xmin <= xmax && ymin <= ymax) {
        val expected = qr(Math.floor(xmin / cell).toInt, Math.floor(ymin / cell).toInt)
        QRIntersectObj.eval(lhs, rhs, cell, expected) shouldBe true
        QRIntersectObj.eval(lhs, rhs, cell, expected ^ (1L << 32)) shouldBe false
      } else {
        QRIntersectObj.eval(lhs, rhs, cell, qr(0, 0)) shouldBe false
      }
    }
  }

  it should "reject rows from different QR groups" in {
    val lhs = Row(qr(0, 0), 0.0, 0.0, 2.0, 2.0)
    val rhs = Row(qr(1, 0), 0.0, 0.0, 2.0, 2.0)
    QRIntersectObj.eval(lhs, rhs, 1.0) shouldBe false

    val lhsInternal = envp(qr(0, 0), 0.0, 0.0, 2.0, 2.0)
    val rhsInternal = envp(qr(1, 0), 0.0, 0.0, 2.0, 2.0)
    QRIntersectObj.eval(lhsInternal, rhsInternal, 1.0) shouldBe false
  }

  it should "reject invalid cell sizes and unpackable coordinate indices" in {
    val lhs = new com.esri.core.geometry.Envelope2D(0.0, 0.0, 2.0, 2.0)
    val rhs = new com.esri.core.geometry.Envelope2D(1.0, 1.0, 3.0, 3.0)
    Seq(0.0, -1.0, Double.NaN, Double.PositiveInfinity).foreach { cell =>
      an[IllegalArgumentException] should be thrownBy
        QRIntersectObj.eval(lhs, rhs, cell, qr(1, 1))
    }
    an[IllegalArgumentException] should be thrownBy
      QRIntersectObj.eval(Int.MaxValue.toDouble + 1.0, 0.0, 1.0)
  }

  it should "propagate null inputs in interpreted and generated evaluation" in {
    val value = envp(qr(0, 0), 0.0, 0.0, 2.0, 2.0)
    val nullExpressions = Seq(
      QRIntersect(Seq(
        Literal.create(null, QREnvpObj.dataType),
        Literal.create(value, QREnvpObj.dataType),
        Literal(1.0),
      )),
      QRIntersect(Seq(
        Literal.create(value, QREnvpObj.dataType),
        Literal.create(null, QREnvpObj.dataType),
        Literal(1.0),
      )),
      QRIntersect(Seq(
        Literal.create(value, QREnvpObj.dataType),
        Literal.create(value, QREnvpObj.dataType),
        Literal.create(null, DoubleType),
      )),
    )

    nullExpressions.foreach { expression =>
      Option(expression.eval(InternalRow.empty)) shouldBe None
      UnsafeProjection.create(Seq(expression))(InternalRow.empty).isNullAt(0) shouldBe true
    }
  }

  it should "reject an incorrect argument count during analysis" in {
    val value = Literal.create(
      envp(qr(0, 0), 0.0, 0.0, 2.0, 2.0),
      QREnvpObj.dataType,
    )
    val args = Seq(value, value, Literal(1.0))

    QRIntersect(args).checkInputDataTypes().isSuccess shouldBe true
    QRIntersect(args.take(2)).checkInputDataTypes().isFailure shouldBe true
    QRIntersect(args :+ Literal(1.0)).checkInputDataTypes().isFailure shouldBe true
  }
}
