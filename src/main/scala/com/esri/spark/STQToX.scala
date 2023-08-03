package com.esri.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

object STQToXObj extends Serializable {
  final def eval(q: Int, c: Double, o: Double): Double = {
    q * c + o
  }
}

final case class STQToX(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    IntegerType,
    DoubleType,
    DoubleType,
  )

  override def dataType: DataType = DoubleType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression) =>
        STQToXObj.eval(
          e1.eval(inputRow).asInstanceOf[Int],
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

    val obj = STQToXObj.getClass.getName.stripSuffix("$")
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
