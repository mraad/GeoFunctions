package com.esri.spark

import com.esri.core.geometry.Polygon
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

object H3CellToBoundaryObj extends Serializable {
  final def eval(cell: Long): Array[Byte] = {
    // Index the java List directly: asScala.zipWithIndex allocated a wrapper, a tuple per
    // vertex and a boxed Int per vertex, on every row, to find out which vertex was first.
    val boundary = H3Instance.h3.cellToBoundary(cell)
    val n = boundary.size()
    val polygon = new Polygon()
    if (n > 0) {
      val head = boundary.get(0)
      polygon.startPath(head.lng, head.lat)
      var i = 1
      while (i < n) {
        val coord = boundary.get(i)
        polygon.lineTo(coord.lng, coord.lat)
        i += 1
      }
      // No closePathWithLine: Polygon rings are implicitly closed and WKB export emits the
      // repeated first vertex already.
    }
    polygon.bytes
  }
}

final case class H3CellToBoundary(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    LongType,
  )

  override def dataType: DataType = BinaryType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression) =>
        H3CellToBoundaryObj.eval(e1.eval(inputRow).asInstanceOf[Long])
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)

    val a1 = c1.value

    val obj = H3CellToBoundaryObj.getClass.getName.stripSuffix("$")
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
