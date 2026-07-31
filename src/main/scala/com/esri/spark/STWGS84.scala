package com.esri.spark

import com.esri.core.geometry.{MultiVertexGeometry, Point, Point2D}
import com.esri.webmercator._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object STWGS84Obj extends Serializable {
  final def eval(bytes: Array[Byte]): Array[Byte] = {
    bytes.geom match {
      case pt: Point =>
        pt.setX(pt.getX.toLongitude)
        pt.setY(pt.getY.toLatitude)
        pt.bytes
      case mv: MultiVertexGeometry =>
        val point2D = new Point2D()
        val n = mv.getPointCount
        var i = 0
        while (i < n) {
          mv.getXY(i, point2D)
          point2D.setCoords(point2D.x.toLongitude(), point2D.y.toLatitude())
          mv.setXY(i, point2D)
          i += 1
        }
        mv.bytes
      case _ =>
        // See the note in STMercator - an Envelope arrives here as a Polygon.
        bytes
    }

  }
}

final case class STWGS84(children: Seq[Expression])
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
        STWGS84Obj.eval(
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

    val obj = STWGS84Obj.getClass.getName.stripSuffix("$")
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
