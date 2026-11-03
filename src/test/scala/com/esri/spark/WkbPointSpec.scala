package com.esri.spark

import com.esri.core.geometry.Geometry.Type
import com.esri.core.geometry.{Geometry, OperatorImportFromWkb, Point, Polygon, ShapeImportFlags}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.{ByteBuffer, ByteOrder}

/**
 * `bytes.geom` decodes a plain 2D WKB point directly; it must equal OperatorImportFromWkb for
 * every input, including the inputs it hands back to the importer.
 * Run with: mvn -DskipTests=false -Dsuites=com.esri.spark.WkbPointSpec test
 */
class WkbPointSpec extends AnyFlatSpec with Matchers {

  private def esri(b: Array[Byte]): Geometry =
    OperatorImportFromWkb.local.execute(ShapeImportFlags.ShapeImportDefaults, Type.Unknown, ByteBuffer.wrap(b), null)

  private def wkbPoint(x: Double, y: Double, order: ByteOrder, wkbType: Int = 1, z: Option[Double] = None): Array[Byte] = {
    val bb = ByteBuffer.allocate(21 + z.size * 8).order(order)
    bb.put((if (order == ByteOrder.LITTLE_ENDIAN) 1 else 0).toByte).putInt(wkbType).putDouble(x).putDouble(y)
    z.foreach(bb.putDouble)
    bb.array()
  }

  private def same(b: Array[Byte]): Unit = {
    val (fast, ref) = (b.geom, esri(b))
    withClue(s"${b.map("%02x".format(_)).mkString}: ") {
      fast.getType shouldBe ref.getType
      fast.isEmpty shouldBe ref.isEmpty
      (fast, ref) match {
        case (f: Point, r: Point) if !r.isEmpty =>
          java.lang.Double.doubleToRawLongBits(f.getX) shouldBe java.lang.Double.doubleToRawLongBits(r.getX)
          java.lang.Double.doubleToRawLongBits(f.getY) shouldBe java.lang.Double.doubleToRawLongBits(r.getY)
          f.hasZ shouldBe r.hasZ
          f.hasM shouldBe r.hasM
        case _ =>
      }
    }
  }

  "bytes.geom" should "decode 2D points exactly like the Esri importer, in both byte orders" in {
    val rnd = new scala.util.Random(7)
    val values = Seq(0.0, -0.0, 1.0, -111.761, 34.8697, Double.MaxValue, Double.MinPositiveValue, -180.0, 90.0) ++
      Seq.fill(200)((rnd.nextDouble() - 0.5) * 400)
    for (x <- values; y <- values.take(12); order <- Seq(ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN))
      same(wkbPoint(x, y, order))
    same(new Point(-111.7610, 34.8697).bytes)
  }

  it should "leave empty, 3D and non-point WKB to the importer" in {
    same(wkbPoint(Double.NaN, Double.NaN, ByteOrder.LITTLE_ENDIAN)) // WKB empty point
    same(new Point().bytes)
    same(wkbPoint(1.0, 2.0, ByteOrder.LITTLE_ENDIAN, wkbType = 1001, z = Some(3.0))) // POINT Z
    val pz = new Point(1.0, 2.0, 3.0)
    same(pz.bytes)
    val poly = new Polygon(); poly.startPath(0, 0); poly.lineTo(1, 0); poly.lineTo(1, 1)
    same(poly.bytes)
  }
}
