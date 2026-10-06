package com.esri.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object QRGeomPar extends Serializable {
  final def eval(bytes: Array[Byte],
                 cell: Double,
                 dist: Double,
                 wkid: UTF8String
                ): ArrayData =
    QRScan.eval(bytes, cell, dist, wkid)((qr, _, geom) => InternalRow(qr, geom.bytes))
}


final case class QRGeom(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType, // geom
    DoubleType, // cell
    DoubleType, // dist
    StringType, // wkid
  )

  override def dataType: DataType = ArrayType(
    StructType(Array(
      StructField("qr", LongType, nullable = false),
      StructField("geom", BinaryType, nullable = false)
    )), containsNull = false)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length != inputTypes.length) {
      TypeCheckResult.TypeCheckFailure(
        s"QR_GEOM requires exactly ${inputTypes.length} arguments, got ${children.length}")
    } else {
      super.checkInputDataTypes()
    }
  }

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression, e4: Expression) =>
        val v1 = e1.eval(inputRow)
        val v2 = e2.eval(inputRow)
        val v3 = e3.eval(inputRow)
        val v4 = e4.eval(inputRow)
        if (v1 == null || v2 == null || v3 == null || v4 == null) null
        else {
          QRGeomPar.eval(
            v1.asInstanceOf[Array[Byte]],
            v2.asInstanceOf[Double],
            v3.asInstanceOf[Double],
            v4.asInstanceOf[UTF8String],
          )
        }
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children(2).genCode(ctx)
    val c4 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value
    val a3 = c3.value
    val a4 = c4.value

    val obj = QRGeomPar.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2,$a3,$a4)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        ${c4.code}
        boolean ${ev.isNull} = ${c1.isNull} || ${c2.isNull} || ${c3.isNull} || ${c4.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = null;
        if (!${ev.isNull}) {
          ${ev.value} = $objEval;
        }
        """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
