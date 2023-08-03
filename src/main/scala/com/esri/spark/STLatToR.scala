package com.esri.spark

import com.esri.webmercator._
import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

object STLatToRObj extends Serializable {
  final def eval(d: Double, c: Double): Int = {
    FastMath.floor(d.toMercatorY() / c).toInt
  }
}

final case class STLatToR(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    DoubleType,
    DoubleType,
  )

  override def dataType: DataType = IntegerType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression) =>
        STLatToRObj.eval(
          e1.eval(inputRow).asInstanceOf[Double],
          e2.eval(inputRow).asInstanceOf[Double],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value

    val obj = STLatToRObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = $objEval;
        """, isNull = FalseLiteral)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
