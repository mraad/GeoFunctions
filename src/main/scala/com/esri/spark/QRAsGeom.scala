package com.esri.spark

import com.esri.core.geometry.Polygon
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

object QRAsGeomObj extends Serializable {

  final def eval(qr: Long,
                 cell: Double,
                 dist: Double,
                ): Array[Byte] = {
    val q = qr >> 32
    val xmin = q * cell - dist
    val xmax = xmin + cell + dist + dist

    val r = qr & 0xFFFFFFFFL
    val v = -(r & 0x80000000L) << 32 >> 31 | r
    val ymin = v * cell - dist
    val ymax = ymin + cell + dist + dist
    val geom = new Polygon()

    geom.startPath(xmin, ymin)
    geom.lineTo(xmin, ymax)
    geom.lineTo(xmax, ymax)
    geom.lineTo(xmax, ymin)
    geom.closePathWithLine()
    geom.bytes
  }
}

final case class QRAsGeom(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    LongType,
    DoubleType,
    DoubleType,
  )

  override def dataType: DataType = BinaryType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression) =>
        QRAsGeomObj.eval(
          e1.eval(inputRow).asInstanceOf[Long],
          e2.eval(inputRow).asInstanceOf[Double],
          e3.eval(inputRow).asInstanceOf[Double],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value
    val a3 = c3.value

    val obj = QRAsGeomObj.getClass.getName.stripSuffix("$")
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
