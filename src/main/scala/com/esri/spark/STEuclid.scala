package com.esri.spark

import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object STEuclidObj extends Serializable {
  final def eval(x1: Double,
                 y1: Double,
                 x2: Double,
                 y2: Double,
                ): Double = {
    val dx = x2 - x1
    val dy = y2 - y1
    FastMath.sqrt(dx * dx + dy * dy)
  }
}

final case class STEuclid(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    DoubleType,
    DoubleType,
    DoubleType,
    DoubleType,
  )

  override def dataType: DataType = DoubleType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression, e4: Expression) =>
        val v1 = e1.eval(inputRow)
        val v2 = e2.eval(inputRow)
        val v3 = e3.eval(inputRow)
        val v4 = e4.eval(inputRow)
        if (v1 == null || v2 == null || v3 == null || v4 == null) null
        else {
          STEuclidObj.eval(
            v1.asInstanceOf[Double],
            v2.asInstanceOf[Double],
            v3.asInstanceOf[Double],
            v4.asInstanceOf[Double],
          )
        }
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children(2).genCode(ctx)
    val c4 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value
    val a3 = c3.value
    val a4 = c4.value

    val obj = STEuclidObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2,$a3,$a4)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        ${c4.code}
        boolean ${ev.isNull} = ${c1.isNull} || ${c2.isNull} || ${c3.isNull} || ${c4.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (!${ev.isNull}) {
          ${ev.value} = $objEval;
        }
        """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
