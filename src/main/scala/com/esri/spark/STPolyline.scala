package com.esri.spark

import com.esri.core.geometry.{Point, Polyline}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._


object STPolylineObj extends Serializable {
  final def eval(arr: ArrayData): Array[Byte] = {
    val mp = new Polyline()
    val n = arr.numElements()
    var i = 0
    while (i < n) {
      arr.getBinary(i).geom match {
        case p: Point =>
          i match {
            case 0 => mp.startPath(p)
            case _ => mp.lineTo(p)
          }
        case _ =>
      }
      i += 1
    }
    mp.bytes
  }
}

final case class STPolyline(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    ArrayType(BinaryType, containsNull = false),
  )

  override def dataType: DataType = BinaryType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression) =>
        STPolylineObj.eval(
          e1.eval(inputRow).asInstanceOf[ArrayData],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)

    val a1 = c1.value

    val obj = STPolylineObj.getClass.getName.stripSuffix("$")
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
