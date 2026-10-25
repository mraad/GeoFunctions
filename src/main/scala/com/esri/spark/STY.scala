package com.esri.spark

import com.esri.core.geometry.{MultiVertexGeometry, Point}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object STYObj extends Serializable {
  final def eval(bytes: Array[Byte], index: Int): Double = {
    bytes.geom match {
      case p: Point =>
        if (p.isEmpty) Double.NaN else p.getY
      case mvg: MultiVertexGeometry =>
        if (mvg.isEmpty) {
          Double.NaN
        } else {
          val i = if (index < 0) mvg.getPointCount + index else index
          mvg.getXY(i).y
        }
      case _ =>
        Double.NaN
    }
  }
}

final case class STY(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(BinaryType, IntegerType)

  override def dataType: DataType = DoubleType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression) =>
        val v1 = e1.eval(inputRow)
        val v2 = e2.eval(inputRow)
        if (v1 == null || v2 == null) null
        else {
          STYObj.eval(
            v1.asInstanceOf[Array[Byte]],
            v2.asInstanceOf[Int],
          )
        }
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value

    val obj = STYObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        boolean ${ev.isNull} = ${c1.isNull} || ${c2.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (!${ev.isNull}) {
          ${ev.value} = $objEval;
        }
        """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
