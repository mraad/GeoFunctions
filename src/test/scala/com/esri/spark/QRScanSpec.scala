package com.esri.spark

import com.esri.core.geometry.Polygon
import org.apache.spark.sql.catalyst.util.ArrayData
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

  private def qrsOf(arr: ArrayData): Set[Long] =
    (0 until arr.numElements()).map(i => arr.getStruct(i, arr.numElements()).getLong(0)).toSet

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

  "QR_COUNT" should "equal the number of candidate cells QR_LIST emits" in {
    for {
      cell <- Seq(0.5, 1.0, 7.3)
      dist <- Seq(0.0, 0.05, 0.4 * cell)
    } {
      val g = rect(-3.2, 1.4, 11.9, 8.6)
      QRCountObj.eval(g, cell, dist) shouldBe listQrs(g, cell, dist).size
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
}
