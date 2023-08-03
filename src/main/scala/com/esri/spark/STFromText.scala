package com.esri.spark

import com.esri.core.geometry.{Geometry, OperatorImportFromWkt, ShapeImportFlags}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.{Success, Try}


object STFromTextObj extends Serializable {
  final def eval(text: UTF8String): Array[Byte] = {
    Try(OperatorImportFromWkt
      .local
      .execute(
        ShapeImportFlags.ShapeImportDefaults,
        Geometry.Type.Unknown,
        text.toString,
        null)
    ) match {
      case Success(g) =>
        g.bytes
      case _ =>
        Array.empty
    }
  }
}

final case class STFromText(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    StringType,
  )

  override def dataType: DataType = BinaryType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression) =>
        STFromTextObj.eval(
          e1.eval(inputRow).asInstanceOf[UTF8String],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)

    val a1 = c1.value

    val obj = STFromTextObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = $objEval;
        """, isNull = FalseLiteral)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
