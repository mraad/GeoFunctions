package com.esri.spark

import com.esri.core.geometry.{MultiVertexGeometry, Point}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object STXObj extends Serializable {
  final def eval(bytes: Array[Byte], index: Int): Double = {
    bytes.geom match {
      case p: Point =>
        if (p.isEmpty) Double.NaN else p.getX
      case mvg: MultiVertexGeometry =>
        if (mvg.isEmpty) {
          Double.NaN
        } else {
          val i = if (index < 0) mvg.getPointCount + index else index
          mvg.getXY(i).x
        }
      case _ => Double.NaN
    }
  }
}

final case class STX(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(BinaryType, IntegerType)

  override def dataType: DataType = DoubleType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression) =>
        STXObj.eval(
          e1.eval(inputRow).asInstanceOf[Array[Byte]],
          e2.eval(inputRow).asInstanceOf[Int],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value

    val obj = STXObj.getClass.getName.stripSuffix("$")
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
