package com.esri.spark

import com.esri.core.geometry.{Point, Polygon}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object GDBPolygonMObj extends Serializable {
  final def eval(row: InternalRow): Array[Byte] = {
    val mp = new Polygon()
    val parts = row.getArray(4).toIntArray
    val coords = row.getArray(5).toDoubleArray

    var x = 0
    var y = 1
    var z = 2
    var m = 3
    parts.foreach(nPoint => {
      var n = 0
      while (n < nPoint) {
        n match {
          case 0 => mp.startPath(new Point(coords(x), coords(y), coords(m)))
          case _ => mp.lineTo(new Point(coords(x), coords(y), coords(m)))
        }
        x += 4
        y += 4
        z += 4
        m += 4
        n += 1
      }
    })

    mp.bytes
  }
}

final case class GDBPolygonM(children: Seq[Expression])
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
        GDBPolygonMObj.eval(
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

    val obj = GDBPolygonMObj.getClass.getName.stripSuffix("$")
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
