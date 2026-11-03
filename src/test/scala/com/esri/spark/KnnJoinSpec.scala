package com.esri.spark

import com.esri.core.geometry.{OperatorDistance, Point, Polygon}
import org.apache.spark.sql.types.{BinaryType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * knn_join must return exactly the k smallest Esri distances for every point: brute force over
 * every polygon is the reference. Points inside polygons (distance 0), isolated points far from
 * every polygon and k above the polygon count are included.
 * Run with: mvn -DskipTests=false -Dsuites=com.esri.spark.KnnJoinSpec test
 */
class KnnJoinSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().master("local[2]").appName("KnnJoinSpec")
      .config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally {
      super.afterAll()
    }
  }

  private val rnd = new scala.util.Random(11)

  private def square(x: Double, y: Double, s: Double): Array[Byte] = {
    val p = new Polygon()
    p.startPath(x, y); p.lineTo(x + s, y); p.lineTo(x + s, y + s); p.lineTo(x, y + s)
    p.bytes
  }

  // small squares clustered near the origin and a few far away
  private val polygons: Seq[(Long, Array[Byte])] =
    (0 until 300).map(i => (i.toLong, square(rnd.nextDouble() * 10, rnd.nextDouble() * 10, 0.05 + rnd.nextDouble() * 0.2))) ++
      Seq((1000L, square(150.0, -60.0, 1.0)), (1001L, square(-170.0, 80.0, 0.5)))

  private val points: Seq[(Long, Array[Byte])] =
    (0 until 400).map(i => (i.toLong, new Point(rnd.nextDouble() * 12 - 1, rnd.nextDouble() * 12 - 1).bytes)) ++
      Seq((900L, new Point(-120.0, -45.0).bytes), (901L, new Point(179.0, 89.0).bytes), // isolated
        (902L, null)) ++ // null geometry: no output rows
      polygons.take(3).map { case (k, _) => (2000L + k, new Point(0, 0).bytes) } // duplicates

  private def df(rows: Seq[(Long, Array[Byte])], id: String, geom: String) =
    spark.createDataFrame(spark.sparkContext.parallelize(rows.map { case (k, g) => Row(k, g) }, 4),
      StructType(Seq(StructField(id, LongType), StructField(geom, BinaryType))))

  private def bruteForce(point: Array[Byte], k: Int): Seq[Double] =
    polygons.map { case (_, g) => OperatorDistance.local.execute(point.geom, g.geom, null) }.sorted.take(k)

  Seq(1, 5, 400).foreach { k =>
    "knn_join" should s"return the exact $k nearest distances for every point" in {
      val out = KnnJoinProcessor(df(points, "pid", "pgeom"), df(polygons, "bid", "bgeom"), k, "pgeom", "bgeom")
      out.columns.toSeq shouldBe Seq("pid", "pgeom", "bid", "bgeom", "distance")
      val got = out.collect().groupBy(_.getLong(0)).map { case (pid, rs) => pid -> rs.map(_.getDouble(4)).sorted.toSeq }
      got.keySet should not contain 902L
      points.filter(_._2 != null).foreach { case (pid, g) =>
        withClue(s"point $pid: ") {
          got(pid) shouldBe bruteForce(g, k)
        }
      }
    }
  }

  it should "be exact for lhs geometries with an extent, not only points" in {
    // long thin rectangles: their centres are far from the polygons they nearly touch
    val strips = (0 until 60).map(i => (i.toLong, {
      val p = new Polygon(); val x = rnd.nextDouble() * 10; val y = rnd.nextDouble() * 10
      p.startPath(x - 6, y); p.lineTo(x + 6, y); p.lineTo(x + 6, y + 0.01); p.lineTo(x - 6, y + 0.01); p.bytes
    }))
    val out = KnnJoinProcessor(df(strips, "sid", "sgeom"), df(polygons, "bid", "bgeom"), 5, "sgeom", "bgeom")
    val got = out.collect().groupBy(_.getLong(0)).map { case (sid, rs) => sid -> rs.map(_.getDouble(4)).sorted.toSeq }
    strips.foreach { case (sid, g) => withClue(s"strip $sid: ") { got(sid) shouldBe bruteForce(g, 5) } }
  }

  it should "accept any k, clamped to the number of polygons" in {
    val out = KnnJoinProcessor(df(points.take(3), "pid", "pgeom"), df(polygons, "bid", "bgeom"), Int.MaxValue, "pgeom", "bgeom")
    out.count() shouldBe 3L * polygons.size
  }

  it should "reject a non-positive k and a non-binary geometry column" in {
    an[IllegalArgumentException] should be thrownBy KnnJoinProcessor(df(points, "pid", "pgeom"), df(polygons, "bid", "bgeom"), 0, "pgeom", "bgeom")
    an[IllegalArgumentException] should be thrownBy KnnJoinProcessor(df(points, "pid", "pgeom"), df(polygons, "bid", "bgeom"), 5, "pid", "bgeom")
  }
}
