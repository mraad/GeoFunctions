package com.esri.spark

import com.esri.core.geometry.{OperatorCentroid2D, Point}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object STCentroidObj extends Serializable {
  final def eval(bytes: Array[Byte]): Array[Byte] = {
    val geom = bytes.geom
    val point = new Point(OperatorCentroid2D.local.execute(geom, null))
    point.bytes
  }
}

final case class STCentroid(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
  )

  override def dataType: DataType = BinaryType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression) =>
        STCentroidObj.eval(
          e1.eval(inputRow).asInstanceOf[Array[Byte]],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)

    val a1 = c1.value

    val obj = STCentroidObj.getClass.getName.stripSuffix("$")
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
