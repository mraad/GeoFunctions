package com.esri.spark

import com.esri.core.geometry.Polygon
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._

object H3CellToBoundaryObj extends Serializable {
  final def eval(
                  cell: Long,
                ): Array[Byte] = {
    val polygon = new Polygon()
    H3Instance.h3
      .cellToBoundary(cell)
      .asScala.zipWithIndex.foreach {
        case (coord, i) => i match {
          case 0 => polygon.startPath(coord.lng, coord.lat)
          case _ => polygon.lineTo(coord.lng, coord.lat)
        }
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
      case Seq(
      e1: Expression,
      ) =>
        H3CellToBoundaryObj.eval(
          e1.eval(inputRow).asInstanceOf[Long],
        )
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
