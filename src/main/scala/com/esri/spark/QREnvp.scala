package com.esri.spark

import com.esri.core.geometry.Envelope2D
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

object QREnvpObj extends Serializable {
  final def dataType: DataType = StructType(Array(
    StructField("qr", LongType, nullable = false),
    StructField("xmin", DoubleType, nullable = false),
    StructField("ymin", DoubleType, nullable = false),
    StructField("xmax", DoubleType, nullable = false),
    StructField("ymax", DoubleType, nullable = false)
  ))

  final def eval(bytes: Array[Byte], cell: Double, dist: Double): ArrayData = {
    val envp = new Envelope2D()
    bytes.geom.queryEnvelope2D(envp)
    val range = QRRange(envp, cell, dist)
    val xmin = envp.xmin - dist
    val ymin = envp.ymin - dist
    val xmax = envp.xmax + dist
    val ymax = envp.ymax + dist

    val arr = new Array[InternalRow](range.count)
    var i = 0
    var r = range.rmin
    while (r < range.rmax) {
      var q = range.qmin
      while (q < range.qmax) {
        val qr = (q << 32) | (r & 0xFFFFFFFFL)
        arr(i) = InternalRow(qr, xmin, ymin, xmax, ymax)
        i += 1
        q += 1
      }
      r += 1
    }
    ArrayData.toArrayData(arr)
  }
}

final case class QREnvp(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
    DoubleType,
    DoubleType,
  )

  override def dataType: DataType = ArrayType(QREnvpObj.dataType, containsNull = false)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length != inputTypes.length) {
      TypeCheckResult.TypeCheckFailure(
        s"QR_ENVP requires exactly ${inputTypes.length} arguments, got ${children.length}")
    } else {
      super.checkInputDataTypes()
    }
  }

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression) =>
        val v1 = e1.eval(inputRow)
        val v2 = e2.eval(inputRow)
        val v3 = e3.eval(inputRow)
        if (v1 == null || v2 == null || v3 == null) null
        else {
          QREnvpObj.eval(
            v1.asInstanceOf[Array[Byte]],
            v2.asInstanceOf[Double],
            v3.asInstanceOf[Double],
          )
        }
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

    val obj = QREnvpObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2,$a3)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        boolean ${ev.isNull} = ${c1.isNull} || ${c2.isNull} || ${c3.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = null;
        if (!${ev.isNull}) {
          ${ev.value} = $objEval;
        }
        """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
