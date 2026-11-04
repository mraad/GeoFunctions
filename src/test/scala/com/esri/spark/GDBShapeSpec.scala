package com.esri.spark

import com.esri.core.geometry.VertexDescription.Semantics
import com.esri.core.geometry.{MultiPath, Point, Polygon, Polyline}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * The gdb reader puts 2, 3 or 4 values per vertex in `coords`, by layer type: (x, y), (x, y, z),
 * (x, y, m) or (x, y, z, m). GDB_POLYGONM used to assume 4 for every layer, so an M-only layer
 * died with ArrayIndexOutOfBounds; GDB_POLYGON2/Z read garbage from wider layers; and every ring
 * kept its stored closing vertex as a zero-length segment.
 *
 * Shapes.gdb (src/test/resources, made by make_shapes_gdb.py with GDAL) holds a point, a
 * polyline and a polygon in each of the four layouts. The polygon has 3 rings of 5, 5 and 4
 * stored vertices (closing vertex included); vertex k carries z = 100 + k and m = 1000 + k.
 * Run with: mvn -DskipTests=false -Dsuites=com.esri.spark.GDBShapeSpec test
 */
class GDBShapeSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val xy = Seq((0, 0), (0, 10), (10, 10), (10, 0), (2, 2), (4, 2), (4, 4), (2, 4), (20, 0), (20, 5), (25, 5))
  private val ringSizes = Seq(4, 4, 3)

  /** The reader's Shape struct for one layout: closing vertices stored, as the reader emits them. */
  private def shapeRow(z: Boolean, m: Boolean): GenericInternalRow = {
    val coords = Seq.newBuilder[Double]
    var k = 0
    for (size <- ringSizes) {
      val ring = (k until k + size) :+ k
      for (v <- ring) {
        coords += xy(v)._1.toDouble += xy(v)._2.toDouble
        if (z) coords += 100.0 + v
        if (m) coords += 1000.0 + v
      }
      k += size
    }
    new GenericInternalRow(Array[Any](null, null, null, null,
      ArrayData.toArrayData(ringSizes.map(_ + 1).toArray), ArrayData.toArrayData(coords.result().toArray)))
  }

  private val layouts = Seq("xy" -> shapeRow(false, false), "xyz" -> shapeRow(true, false),
    "xym" -> shapeRow(false, true), "xyzm" -> shapeRow(true, true))

  /** Same geometry everywhere; Z and M present exactly when expected, each with its own values. */
  private def check(g: MultiPath, z: Option[Double], m: Option[Double]): Unit = {
    g.getPathCount shouldBe 3
    (0 until 3).map(g.getPathSize) shouldBe ringSizes
    g.getPointCount shouldBe xy.size
    g.hasAttribute(Semantics.Z) shouldBe z.isDefined
    g.hasAttribute(Semantics.M) shouldBe m.isDefined
    xy.indices.foreach { k =>
      val p = g.getPoint(k)
      (p.getX, p.getY) shouldBe ((xy(k)._1.toDouble, xy(k)._2.toDouble))
      z.foreach(base => p.getZ shouldBe base + k)
      m.foreach(base => p.getM shouldBe base + k)
    }
  }

  layouts.foreach { case (name, row) =>
    s"GDB_POLYGON2 on a $name layer" should "keep x and y only" in {
      check(GDBPolygon2Obj.eval(row).geom.asInstanceOf[Polygon], None, None)
    }
  }

  "GDB_POLYGONZ" should "carry Z on xyz and xyzm layers and stay 2D on xy" in {
    check(GDBPolygonZObj.eval(layouts(1)._2).geom.asInstanceOf[Polygon], Some(100), None)
    check(GDBPolygonZObj.eval(layouts(3)._2).geom.asInstanceOf[Polygon], Some(100), None)
    check(GDBPolygonZObj.eval(layouts(0)._2).geom.asInstanceOf[Polygon], None, None)
  }

  "GDB_POLYGONM" should "read an M-only layer (3 values per vertex) as x, y, m" in {
    check(GDBPolygonMObj.eval(layouts(2)._2).geom.asInstanceOf[Polygon], None, Some(1000))
  }

  it should "keep Z and M on an xyzm layer" in {
    check(GDBPolygonMObj.eval(layouts(3)._2).geom.asInstanceOf[Polygon], Some(100), Some(1000))
  }

  it should "stay 2D on an xy layer" in {
    check(GDBPolygonMObj.eval(layouts(0)._2).geom.asInstanceOf[Polygon], None, None)
  }

  "GDB_POLYLINE2" should "keep a path's last vertex even when it meets the first" in {
    val g = GDBPolyline2Obj.eval(layouts(0)._2).geom.asInstanceOf[Polyline]
    (0 until 3).map(g.getPathSize) shouldBe ringSizes.map(_ + 1)
  }

  "GDBShapeObj" should "reject coords that do not match the part sizes" in {
    val bad = new GenericInternalRow(Array[Any](null, null, null, null,
      ArrayData.toArrayData(Array(3)), ArrayData.toArrayData(Array(0.0, 0.0, 1.0, 1.0, 2.0))))
    an[IllegalArgumentException] should be thrownBy GDBPolygon2Obj.eval(bad)
  }

  it should "return an empty polygon for a shape with no vertices" in {
    val empty = new GenericInternalRow(Array[Any](null, null, null, null,
      ArrayData.toArrayData(Array.emptyIntArray), ArrayData.toArrayData(Array.emptyDoubleArray)))
    GDBPolygon2Obj.eval(empty).geom.isEmpty shouldBe true
  }

  // End to end: the real reader on a FileGDB written by GDAL, through the DataFrame API (codegen).
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().master("local[1]").appName("GDBShapeSpec")
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

  private def read(layer: String, f: Column => Column): com.esri.core.geometry.Geometry = {
    val gdb = new java.io.File(getClass.getResource("/Shapes.gdb").toURI).getPath
    val df = spark.read.format("gdb").option("path", gdb).option("name", layer).load()
    val shape = df.schema.fieldNames.find(_.equalsIgnoreCase("shape")).get
    df.select(f(col(shape))).head().getAs[Array[Byte]](0).geom
  }

  private def readPath(layer: String, f: Column => Column): MultiPath = read(layer, f).asInstanceOf[MultiPath]

  "The gdb reader + GDB_POLYGON*" should "decode every polygon layer type GDAL writes" in {
    check(readPath("poly_xy", GeoFunctions.gdbPolygon2), None, None)
    check(readPath("poly_xyz", GeoFunctions.gdbPolygonZ), Some(100), None)
    check(readPath("poly_xym", GeoFunctions.gdbPolygonM), None, Some(1000))
    check(readPath("poly_xyzm", GeoFunctions.gdbPolygonM), Some(100), Some(1000))
    check(readPath("poly_xyzm", GeoFunctions.gdbPolygon2), None, None)
  }

  // line_*: paths (0,0) (5,5) (10,0) and (20,0) (25,5) (30,0) (20,0) - the second closed in x/y.
  private val lineXY = Seq((0, 0), (5, 5), (10, 0), (20, 0), (25, 5), (30, 0), (20, 0))

  private def checkLine(g: MultiPath, z: Option[Double], m: Option[Double]): Unit = {
    g.getPathCount shouldBe 2
    (0 until 2).map(g.getPathSize) shouldBe Seq(3, 4)
    g.hasAttribute(Semantics.Z) shouldBe z.isDefined
    g.hasAttribute(Semantics.M) shouldBe m.isDefined
    lineXY.indices.foreach { k =>
      val p = g.getPoint(k)
      (p.getX, p.getY) shouldBe ((lineXY(k)._1.toDouble, lineXY(k)._2.toDouble))
      z.foreach(base => p.getZ shouldBe base + k)
      m.foreach(base => p.getM shouldBe base + k)
    }
  }

  "The gdb reader + GDB_POLYLINE*" should "decode every polyline layer type GDAL writes" in {
    Seq("xy", "xyz", "xym", "xyzm").foreach(k => checkLine(readPath(s"line_$k", GeoFunctions.gdbPolyline2), None, None))
    checkLine(readPath("line_xy", GeoFunctions.gdbPolylineZ), None, None)
    checkLine(readPath("line_xyz", GeoFunctions.gdbPolylineZ), Some(100), None)
    checkLine(readPath("line_xyzm", GeoFunctions.gdbPolylineZ), Some(100), None)
    checkLine(readPath("line_xy", GeoFunctions.gdbPolylineM), None, None)
    checkLine(readPath("line_xym", GeoFunctions.gdbPolylineM), None, Some(1000))
    checkLine(readPath("line_xyzm", GeoFunctions.gdbPolylineM), Some(100), Some(1000))
  }

  "The gdb reader + GDB_POINT" should "keep exactly the dimensions each point layer has" in {
    Seq("xy" -> (false, false), "xyz" -> (true, false), "xym" -> (false, true), "xyzm" -> (true, true))
      .foreach { case (k, (z, m)) =>
        withClue(s"point_$k: ") {
          val p = read(s"point_$k", GeoFunctions.gdbPoint).asInstanceOf[Point]
          (p.getX, p.getY) shouldBe ((5.0, 7.0))
          p.hasAttribute(Semantics.Z) shouldBe z
          p.hasAttribute(Semantics.M) shouldBe m
          if (z) p.getZ shouldBe 100.0
          if (m) p.getM shouldBe 1000.0
        }
      }
  }

  "GDB_POINT" should "give an empty point for the reader's NaN shape" in {
    val nan = new GenericInternalRow(Array[Any](Double.NaN, Double.NaN, Double.NaN))
    GDBPointObj.eval(nan, 0, 1, 2, -1).geom.isEmpty shouldBe true
  }
}
