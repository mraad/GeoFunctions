package com.esri.spark

import com.esri.core.geometry.{Geometry, OperatorUnion}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


object STUnionColObj extends Serializable {
  final def eval(arr: ArrayData,
                 wkid: UTF8String,
                ): Array[Byte] = {
    arr.numElements() match {
      case 0 =>
        Array.emptyByteArray
      case 1 =>
        arr.getBinary(0)
      case _ =>
        val cur = ArrayDataGeometryCursor(arr)
        val res = OperatorUnion.local.execute(
          cur,
          SpatialReferenceObj.create(wkid),
          null)
        res.next match {
          case g: Geometry => g.bytes
          case _ => Array.emptyByteArray
        }
    }
  }
}

final case class STUnionCol(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    ArrayType(BinaryType, containsNull = false),
    StringType,
  )

  override def dataType: DataType = BinaryType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression) =>
        STUnionColObj.eval(
          e1.eval(inputRow).asInstanceOf[ArrayData],
          e2.eval(inputRow).asInstanceOf[UTF8String],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value

    val obj = STUnionColObj.getClass.getName.stripSuffix("$")
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
