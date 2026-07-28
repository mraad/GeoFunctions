package com.esri.spark

import com.esri.core.geometry.{MultiVertexGeometry, Point, Point2D}
import com.esri.webmercator._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object STMercatorObj extends Serializable {
  final def eval(bytes: Array[Byte]): Array[Byte] = {
    bytes.geom match {
      case pt: Point =>
        // toMercatorX/Y, not toLongitude/toLatitude - this branch was a copy of STWGS84's
        // and un-projected points instead of projecting them.
        pt.setX(pt.getX.toMercatorX)
        pt.setY(pt.getY.toMercatorY)
        pt.bytes
      case mv: MultiVertexGeometry =>
        val point2D = new Point2D()
        val n = mv.getPointCount
        var i = 0
        while (i < n) {
          mv.getXY(i, point2D)
          point2D.setCoords(point2D.x.toMercatorX, point2D.y.toMercatorY)
          mv.setXY(i, point2D)
          i += 1
        }
        mv.bytes
      case _ =>
        bytes
    }

  }
}

final case class STMercator(children: Seq[Expression])
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
        STMercatorObj.eval(
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

    val obj = STMercatorObj.getClass.getName.stripSuffix("$")
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
