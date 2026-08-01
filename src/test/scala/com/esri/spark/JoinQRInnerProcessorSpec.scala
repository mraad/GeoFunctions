package com.esri.spark

import com.esri.core.geometry.Polygon
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JoinQRInnerProcessorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("JoinQRInnerProcessorSpec")
      .config("spark.ui.enabled", "false")
      // Force the two-row duplicate-suppression fixture through the parallel branch;
      // the one-row null fixture below still covers the sequential branch.
      .config("spark.esri.parallel", "1")
      .getOrCreate()
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

  private def qr(q: Int, r: Int): Long =
    (q.toLong << 32) | (r.toLong & 0xFFFFFFFFL)

  private def qrRow(key: Long,
                    xmin: Double,
                    ymin: Double,
                    xmax: Double,
                    ymax: Double
                   ): Row =
    Row(key, xmin, ymin, xmax, ymax)

  private def rect(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Array[Byte] = {
    val polygon = new Polygon()
    polygon.startPath(xmin, ymin)
    polygon.lineTo(xmin, ymax)
    polygon.lineTo(xmax, ymax)
    polygon.lineTo(xmax, ymin)
    polygon.closePathWithLine()
    polygon.bytes
  }

  private def frame(idName: String,
                    rows: Seq[Row],
                    withGeometry: Boolean = true,
                    qrType: StructType = QREnvpObj.dataType.asInstanceOf[StructType]
                   ): DataFrame = {
    val fields = Seq(
      StructField(idName, StringType, nullable = false),
      StructField("qr", qrType, nullable = true),
    ) ++ (if (withGeometry) Seq(StructField("geom", BinaryType, nullable = true)) else Seq.empty)
    spark.createDataFrame(spark.sparkContext.parallelize(rows), StructType(fields))
  }

  "JoinQRInnerProcessor" should "normalize operation names and suppress replicated pairs" in {
    val bytes = rect(0.25, 0.25, 1.75, 0.75)
    val envp = (0.25, 0.25, 1.75, 0.75)
    val lhs = frame("lid", Seq(
      Row("lhs", qrRow(qr(0, 0), envp._1, envp._2, envp._3, envp._4), bytes),
      Row("lhs", qrRow(qr(1, 0), envp._1, envp._2, envp._3, envp._4), bytes),
    ))
    val rhs = frame("rid", Seq(
      Row("rhs", qrRow(qr(0, 0), envp._1, envp._2, envp._3, envp._4), bytes),
      Row("rhs", qrRow(qr(1, 0), envp._1, envp._2, envp._3, envp._4), bytes),
    ))

    val result = JoinQRInnerProcessor(lhs, rhs, 1.0, oper = " INTERSECTION ")
    result.count() shouldBe 1L
    result.first().getString(0) shouldBe "lhs"
    result.first().getString(2) shouldBe "rhs"
  }

  it should "treat NONE case-insensitively without requiring geometry columns" in {
    val lhs = frame("lid", Seq(Row("lhs", qrRow(qr(0, 0), 0.0, 0.0, 1.0, 1.0))),
      withGeometry = false)
    val rhs = frame("rid", Seq(Row("rhs", qrRow(qr(0, 0), 0.5, 0.5, 1.5, 1.5))),
      withGeometry = false)

    JoinQRInnerProcessor(lhs, rhs, 1.0, oper = " NoNe ").count() shouldBe 1L
  }

  it should "reject unsupported operations and acceleration modes before execution" in {
    val lhs = frame("lid", Seq(Row("lhs", qrRow(qr(0, 0), 0.0, 0.0, 1.0, 1.0))),
      withGeometry = false)
    val rhs = frame("rid", Seq(Row("rhs", qrRow(qr(0, 0), 0.0, 0.0, 1.0, 1.0))),
      withGeometry = false)

    an[IllegalArgumentException] should be thrownBy
      JoinQRInnerProcessor(lhs, rhs, 1.0, oper = "typo")
    an[IllegalArgumentException] should be thrownBy
      JoinQRInnerProcessor(lhs, rhs, 1.0, oper = "disjoint")
    an[IllegalArgumentException] should be thrownBy
      JoinQRInnerProcessor(lhs, rhs, 1.0, acceleration = "fast")
  }

  it should "reject an incompatible QR field before starting a Spark job" in {
    val wrongQrType = StructType(QREnvpObj.dataType.asInstanceOf[StructType].fields.updated(
      0, StructField("qr", StringType, nullable = false)))
    val lhs = frame("lid", Seq.empty, withGeometry = false, qrType = wrongQrType)
    val rhs = frame("rid", Seq.empty, withGeometry = false)

    an[IllegalArgumentException] should be thrownBy
      JoinQRInnerProcessor(lhs, rhs, 1.0)
  }

  it should "skip null QR and geometry values while retaining valid rows" in {
    val goodQr = qrRow(qr(0, 0), 0.0, 0.0, 1.0, 1.0)
    val bytes = rect(0.0, 0.0, 1.0, 1.0)
    val lhs = frame("lid", Seq(
      Row("null-qr", null, bytes),
      Row("null-geom", goodQr, null),
      Row("good", goodQr, bytes),
    ))
    val rhs = frame("rid", Seq(Row("rhs", goodQr, bytes)))

    JoinQRInnerProcessor(lhs, rhs, 1.0, oper = "intersects").count() shouldBe 1L
  }
}
