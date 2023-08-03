package com.esri.spark

import com.esri.core.geometry.Polygon
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._


object STPolygon2Obj extends Serializable {
  final def eval(arrData: ArrayData): Array[Byte] = {
    val polygon = new Polygon()
    val doubles = arrData.toDoubleArray()
    var i = 0
    var j = 1
    val n = doubles.length
    while (i < n) {
      val x = doubles(i)
      val y = doubles(j)
      i match {
        case 0 => polygon.startPath(x, y)
        case _ => polygon.lineTo(x, y)
      }
      i += 2
      j += 2
    }
    polygon.bytes
  }
}

final case class STPolygon2(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    ArrayType(DoubleType, containsNull = false),
  )

  override def dataType: DataType = BinaryType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression) =>
        STPolygon2Obj.eval(
          e1.eval(inputRow).asInstanceOf[ArrayData],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)

    val a1 = c1.value

    val obj = STPolygon2Obj.getClass.getName.stripSuffix("$")
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
