package com.esri.spark

import com.esri.core.geometry.Polygon
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object STCellObj extends Serializable {
  final def eval(x0: Double,
                 y0: Double,
                 ww: Double,
                 hh: Double,
                ): Array[Byte] = {
    val x1 = x0 + ww
    val y1 = y0 + hh
    val geom = new Polygon()
    geom.startPath(x0, y0)
    geom.lineTo(x0, y1)
    geom.lineTo(x1, y1)
    geom.lineTo(x1, y0)
    geom.closePathWithLine()
    geom.bytes
  }
}

final case class STCell(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    DoubleType,
    DoubleType,
    DoubleType,
    DoubleType,
  )

  override def dataType: DataType = BinaryType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression, e4: Expression) =>
        STCellObj.eval(
          e1.eval(inputRow).asInstanceOf[Double],
          e2.eval(inputRow).asInstanceOf[Double],
          e3.eval(inputRow).asInstanceOf[Double],
          e4.eval(inputRow).asInstanceOf[Double],
        )
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

    val obj = STCellObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2,$a3,$a4)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        ${c4.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = $objEval;
        """, isNull = FalseLiteral)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
