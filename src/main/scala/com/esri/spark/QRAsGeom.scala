package com.esri.spark

import com.esri.core.geometry.Polygon
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
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
        val v1 = e1.eval(inputRow)
        val v2 = e2.eval(inputRow)
        val v3 = e3.eval(inputRow)
        if (v1 == null || v2 == null || v3 == null) null
        else QRAsGeomObj.eval(
          v1.asInstanceOf[Long],
          v2.asInstanceOf[Double],
          v3.asInstanceOf[Double],
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

    val obj = QRAsGeomObj.getClass.getName.stripSuffix("$")
    val jt = CodeGenerator.javaType(dataType)
    val dv = CodeGenerator.defaultValue(dataType)
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        boolean ${ev.isNull} = ${c1.isNull} || ${c2.isNull} || ${c3.isNull};
        $jt ${ev.value} = ${ev.isNull}
            ? $dv
            : $obj.eval(${c1.value}, ${c2.value}, ${c3.value});
        """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
