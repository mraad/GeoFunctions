package com.esri.spark

import com.esri.core.geometry.{Envelope, Envelope2D, MultiPath, OperatorIntersection, Polygon, Polyline}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object QRClipObj extends Serializable {

  final def eval(bytes: Array[Byte], cell: Double, dist: Double): ArrayData = {
    val arr = new ArrayBuffer[InternalRow]()
    val operator = OperatorIntersection.local
    val dist2 = dist + dist
    val envp = new Envelope2D()
    val geom = bytes.geom
    geom.queryEnvelope2D(envp)

    val q_min = (envp.xmin / cell).floor.toLong
    val r_min = (envp.ymin / cell).floor.toLong
    val q_max = (envp.xmax / cell).floor.toLong + 1L
    val r_max = (envp.ymax / cell).floor.toLong + 1L
    val cellEnvp = new Envelope(0.0, 0.0, 1.0, 1.0)

    var r = r_min
    while (r < r_max) {
      val ymin = r * cell - dist
      val ymax = ymin + cell + dist2
      var q = q_min
      while (q < q_max) {
        val xmin = q * cell - dist
        val xmax = xmin + cell + dist2
        cellEnvp.setCoords(xmin, ymin, xmax, ymax)

        def arrAppend(mp: MultiPath, pointCount: Int): Unit = {
          if (mp.getPathCount > 0 && mp.getPointCount > pointCount) {
            val binary = mp.bytes
            val qr = (q << 32) | (r & 0xFFFFFFFFL)
            arr.append(InternalRow(qr, binary))
          }
        }

        operator.execute(geom, cellEnvp, null, null) match {
          // TODO Handle case when point and multipoint.
          case polygon: Polygon => arrAppend(polygon, 2)
          case polyline: Polyline => arrAppend(polyline, 1)
          case _ => //
        }
        q += 1L
      }
      r += 1L
    }
    ArrayData.toArrayData(arr)
  }
}

final case class QRClip(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
    DoubleType,
    DoubleType,
  )

  override def dataType: DataType = ArrayType(
    StructType(Array(
      StructField("qr", LongType, nullable = false),
      StructField("geom", BinaryType, nullable = false)
    )), containsNull = false)

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression) =>
        QRClipObj.eval(
          e1.eval(inputRow).asInstanceOf[Array[Byte]],
          e2.eval(inputRow).asInstanceOf[Double],
          e3.eval(inputRow).asInstanceOf[Double],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value
    val a3 = c3.value

    val obj = QRClipObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2,$a3)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = $objEval;
        """, isNull = FalseLiteral)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
