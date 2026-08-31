package com.esri.spark

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Guards the narrowed r-scan in ClipLineDistObj against a brute-force scan of the
 * full dist-inflated bounding box. Run with: mvn -DskipTests=false test
 */
class ClipLineDistSpec extends AnyFlatSpec with Matchers {

  private def seg(x1: Double, y1: Double, x2: Double, y2: Double): ArrayData =
    new GenericArrayData(Array(x1, y1, x2, y2))

  private def rows(arr: ArrayData): Seq[(Int, Int, Double)] =
    (0 until arr.numElements()).map { i =>
      val row = arr.getStruct(i, 3)
      (row.getInt(0), row.getInt(1), row.getDouble(2))
    }.sorted

  /** Brute force: scan every cell in the dist-inflated bbox. */
  private def brute(x1: Double, y1: Double, x2: Double, y2: Double,
                    cell: Double, dist: Double): Seq[(Int, Int, Double)] = {
    val qmin = math.floor(((x1 min x2) - dist) / cell).toInt
    val qmax = math.floor(((x1 max x2) + dist) / cell).toInt + 1
    val rmin = math.floor(((y1 min y2) - dist) / cell).toInt
    val rmax = math.floor(((y1 max y2) + dist) / cell).toInt + 1
    val chip = cell + dist + dist
    val out = ArrayBuffer.empty[(Int, Int, Double)]
    for (q <- qmin until qmax; r <- rmin until rmax) {
      val l = Rect(q * cell - dist, r * cell - dist, chip).clip(x1, y1, x2, y2)
      if (l > 0.0) out.append((q, r, l))
    }
    out.toSeq.sorted
  }

  "ClipLineDistObj" should "match a brute-force scan of the inflated bbox" in {
    val rnd = new Random(42)
    for {
      cell <- Seq(0.5, 1.0, 7.3)
      dist <- Seq(0.0, 0.05, 0.4 * cell)
      _ <- 0 until 200
    } {
      val x1 = rnd.nextDouble() * 40.0 - 20.0
      val y1 = rnd.nextDouble() * 40.0 - 20.0
      val x2 = rnd.nextDouble() * 40.0 - 20.0
      val y2 = rnd.nextDouble() * 40.0 - 20.0
      val got = rows(ClipLineDistObj.eval(seg(x1, y1, x2, y2), cell, dist))
      val exp = brute(x1, y1, x2, y2, cell, dist)
      got.map(t => (t._1, t._2)) shouldBe exp.map(t => (t._1, t._2))
      got.zip(exp).foreach { case (g, e) => g._3 shouldBe e._3 +- 1e-9 }
    }
  }

  it should "emit distinct rows (a reused row buffer would duplicate the first)" in {
    val got = rows(ClipLineDistObj.eval(seg(0.0, 0.0, 10.0, 10.0), 1.0, 0.0))
    got.distinct.length shouldBe got.length
    got.length should be >= 10
  }

  it should "split a segment into lengths summing to the segment length when dist is 0" in {
    val (x1, y1, x2, y2) = (-3.7, 2.1, 11.4, -6.8)
    val total = rows(ClipLineDistObj.eval(seg(x1, y1, x2, y2), 0.5, 0.0)).map(_._3).sum
    total shouldBe math.hypot(x2 - x1, y2 - y1) +- 1e-9
  }

  it should "cover cells the segment only reaches through the dist padding" in {
    // Segment sits inside cell 0 but within dist of cell 1 - the pre-fix range missed it.
    val cell = 100.0
    val got = rows(ClipLineDistObj.eval(seg(95.0, 50.0, 98.0, 50.0), cell, 10.0))
    got.map(_._1) should contain allOf(0, 1)
  }

  it should "reject bad arguments" in {
    an[IllegalArgumentException] should be thrownBy
      ClipLineDistObj.eval(seg(0, 0, 1, 1), 0.0, 0.0)
    an[IllegalArgumentException] should be thrownBy
      ClipLineDistObj.eval(seg(0, 0, 1, 1), 1.0, -1.0)
    an[IllegalArgumentException] should be thrownBy
      ClipLineDistObj.eval(new GenericArrayData(Array(0.0, 1.0)), 1.0, 0.0)
  }
}
