package com.esri.spark

import com.esri.core.geometry.Polyline
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object GDBPolyline2Obj extends Serializable {
  final def eval(row: InternalRow): Array[Byte] = {
    val polyline = new Polyline()
    val parts = row.getArray(4).toIntArray()
    val coords = row.getArray(5).toDoubleArray()
    var i = 0
    var j = 1
    var p = 0
    while (p < parts.length) {
      val part = parts(p)
      var n = 0
      while (n < part) {
        val x = coords(i)
        val y = coords(j)
        if (n == 0) {
          polyline.startPath(x, y)
        } else {
          polyline.lineTo(x, y)
        }
        i += 2
        j += 2
        n += 1
      }
      p += 1
    }
    polyline.bytes
  }
}

final case class GDBPolyline2(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[StructType.type] = Seq(
    StructType,
  )

  override def dataType: DataType = BinaryType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression) =>
        GDBPolyline2Obj.eval(
          e1.eval(inputRow).asInstanceOf[InternalRow],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)

    val a1 = c1.value

    val obj = GDBPolyline2Obj.getClass.getName.stripSuffix("$")
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
