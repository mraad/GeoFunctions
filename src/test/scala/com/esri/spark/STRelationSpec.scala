package com.esri.spark

import com.esri.core.geometry.{OperatorDistance, Point, Polygon}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Literal, UnsafeProjection}
import org.apache.spark.sql.types.{BinaryType, DoubleType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * A constant geometry argument is parsed and accelerated once instead of per row. That must
 * never change an answer: every predicate and ST_DWITHIN agree whether each geometry arrives
 * as a literal or a column, interpreted or generated.
 * Run with: mvn -DskipTests=false -Dsuites=com.esri.spark.STRelationSpec test
 */
class STRelationSpec extends AnyFlatSpec with Matchers {

  /** 400-vertex concave star: enough vertices for acceleration to build its index. */
  private val star: Array[Byte] = {
    val p = new Polygon()
    val n = 400
    (0 until n).foreach { i =>
      val a = 2 * math.Pi * i / n
      val r = if (i % 2 == 0) 10.0 else 4.0
      if (i == 0) p.startPath(r * math.cos(a), r * math.sin(a)) else p.lineTo(r * math.cos(a), r * math.sin(a))
    }
    p.bytes
  }

  // A grid across the star, its bbox and beyond, plus the origin and a vertex (boundary).
  private val points: Seq[Array[Byte]] =
    (for (i <- 0 to 64; j <- 0 to 58) yield new Point(-12.0 + i * 0.37, -12.0 + j * 0.41).bytes) :+
      new Point(0, 0).bytes :+ new Point(10, 0).bytes

  private val wkid = UTF8String.fromString("-1")

  private val lhsCol = BoundReference(0, BinaryType, nullable = true)
  private val rhsCol = BoundReference(1, BinaryType, nullable = true)
  private val wkidCol = BoundReference(2, StringType, nullable = true)

  // One projection per expression, so the generated path keeps its WkbCache across rows.
  private val projections = scala.collection.mutable.Map.empty[Expression, UnsafeProjection]

  private def run(e: Expression, lhs: Array[Byte], rhs: Array[Byte], third: Any): (Any, Any) = {
    val row = InternalRow(lhs, rhs, third)
    (e.eval(row), projections.getOrElseUpdate(e, UnsafeProjection.create(Seq(e)))(row).getBoolean(0))
  }

  private val predicates: Seq[(String, Seq[Expression] => Expression)] = Seq(
    "ST_INTERSECTS" -> (c => STIntersects(c)),
    "ST_CONTAINS" -> (c => STContains(c)),
    "ST_WITHIN" -> (c => STWithin(c)),
    "ST_TOUCHES" -> (c => STTouches(c)),
    "ST_OVERLAPS" -> (c => STOverlaps(c)),
    "ST_DISJOINT" -> (c => STDisjoint(c)),
  )

  predicates.foreach { case (name, make) =>
    name should "give the same answer for a constant polygon on either side" in {
      val byColumn = make(Seq(lhsCol, rhsCol, wkidCol))
      val polyLhs = make(Seq(Literal(star), rhsCol, Literal(wkid)))
      val polyRhs = make(Seq(lhsCol, Literal(star), Literal(wkid)))
      points.foreach { pt =>
        val (expectLR, genLR) = run(byColumn, star, pt, wkid)
        val (expectRL, genRL) = run(byColumn, pt, star, wkid)
        genLR shouldBe expectLR
        genRL shouldBe expectRL
        run(polyLhs, null, pt, null) shouldBe ((expectLR, expectLR))
        run(polyRhs, pt, null, null) shouldBe ((expectRL, expectRL))
      }
    }
  }

  "ST_INTERSECTS" should "find points inside and outside the star" in {
    val e = STIntersects(Seq(lhsCol, Literal(star), Literal(wkid)))
    run(e, new Point(0, 0).bytes, null, null)._1 shouldBe true
    run(e, new Point(9, 9).bytes, null, null)._1 shouldBe false
  }

  "WkbCache" should "reuse a parse only for the same array instance" in {
    val cache = new WkbCache
    var reuses = 0
    val a = new Point(1, 2).bytes
    val g = cache(a, _ => reuses += 1)
    cache(a, _ => reuses += 1) should be theSameInstanceAs g
    cache(a, _ => reuses += 1) should be theSameInstanceAs g
    reuses shouldBe 1
    val b = new Point(3, 4).bytes
    cache(b).asInstanceOf[Point].getX shouldBe 3.0
    cache(a.clone()).asInstanceOf[Point].getX shouldBe 1.0
  }

  "ST_DWITHIN" should "equal ST_DISTANCE <= dist, constant or not" in {
    val byColumn = STDWithin(Seq(lhsCol, rhsCol, BoundReference(2, DoubleType, nullable = true)))
    Seq(0.0, 0.5, 2.0, 50.0).foreach { d =>
      val constant = STDWithin(Seq(Literal(star), rhsCol, Literal(d)))
      points.foreach { pt =>
        val expected = OperatorDistance.local.execute(star.geom, pt.geom, null) <= d
        run(byColumn, star, pt, d) shouldBe ((expected, expected))
        run(constant, null, pt, null) shouldBe ((expected, expected))
      }
    }
  }
}
