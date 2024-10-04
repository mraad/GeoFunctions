package com.esri.spark

import com.esri.core.geometry.{MultiVertexGeometry, OperatorCentroid2D, Point}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object STCentroidXYObj extends Serializable {
  final def eval(bytes: Array[Byte]
                ): InternalRow = {
    bytes.geom match {
      case p: Point =>
        if (p.isEmpty)
          InternalRow(Double.NaN, Double.NaN)
        else
          InternalRow(p.getX, p.getY)
      case mvg: MultiVertexGeometry =>
        if (mvg.isEmpty) {
          InternalRow(Double.NaN, Double.NaN)
        } else {
          val p = OperatorCentroid2D.local.execute(mvg, null)
          InternalRow(p.x, p.y)
        }
      case _ =>
        InternalRow(Double.NaN, Double.NaN)
    }
  }
}

final case class STCentroidXY(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def dataType: DataType = StructType(Array(
    StructField("x", DoubleType, nullable = false),
    StructField("y", DoubleType, nullable = false)
  ))

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression) =>
        STCentroidXYObj.eval(
          e1.eval(inputRow).asInstanceOf[Array[Byte]],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)

    val a1 = c1.value

    val obj = STCentroidXYObj.getClass.getName.stripSuffix("$")
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
