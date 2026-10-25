package com.esri.spark

import com.esri.core.geometry.{OperatorExportToWkt, ShapeExportFlags}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


object STAsTextObj extends Serializable {
  final def eval(bytes: Array[Byte]): UTF8String = {
    bytes match {
      case Array() =>
        UTF8String.fromString("EMPTY")
      case _ =>
        UTF8String.fromString(OperatorExportToWkt
          .local
          .execute(
            ShapeExportFlags.ShapeExportDefaults,
            bytes.geom,
            null))
    }
  }
}

final case class STAsText(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
  )

  override def dataType: DataType = StringType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression) =>
        val v1 = e1.eval(inputRow)
        if (v1 == null) null
        else {
          STAsTextObj.eval(
            v1.asInstanceOf[Array[Byte]],
          )
        }
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)

    val a1 = c1.value

    val obj = STAsTextObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1)"
    ev.copy(code =
      code"""
        ${c1.code}
        boolean ${ev.isNull} = ${c1.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (!${ev.isNull}) {
          ${ev.value} = $objEval;
        }
        """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
