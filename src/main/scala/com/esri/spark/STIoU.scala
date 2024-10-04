package com.esri.spark

import com.esri.core.geometry.{OperatorIntersection, OperatorUnion, Polygon}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object STIoUObj extends Serializable {
  final def eval(lhs: Array[Byte],
                 rhs: Array[Byte],
                 wkid: UTF8String,
                ): Double = {
    (lhs.geom, rhs.geom) match {
      case (l: Polygon, r: Polygon) =>
        val sr = SpatialReferenceObj.create(wkid)
        val i = OperatorIntersection.local.execute(l, r, sr, null).calculateArea2D()
        val u = OperatorUnion.local.execute(l, r, sr, null).calculateArea2D()
        if (u > 0.0) {
          i / u
        } else {
          0.0
        }
      case _ =>
        0.0
    }
  }
}

final case class STIoU(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
    BinaryType,
  )

  override def dataType: DataType = DoubleType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(
      e1: Expression,
      e2: Expression,
      e3: Expression,
      ) =>
        STIoUObj.eval(
          e1.eval(inputRow).asInstanceOf[Array[Byte]],
          e2.eval(inputRow).asInstanceOf[Array[Byte]],
          e3.eval(inputRow).asInstanceOf[UTF8String],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children(2).genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value
    val a3 = c3.value

    val obj = STIoUObj.getClass.getName.stripSuffix("$")
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
