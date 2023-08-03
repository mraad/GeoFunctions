package com.esri.spark

import com.esri.core.geometry.Envelope2D
import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

object QRIntersectObj extends Serializable {

  @inline // Leave inline - make sure -opt:l:inline is enabled as a compiler arg
  final def eval(x: Double, y: Double, cell: Double): Long = {
    val q = FastMath.floor(x / cell).toLong << 32
    val r = FastMath.floor(y / cell).toLong & 0xFFFFFFFFL
    q | r
  }

  final def eval(lhs: Envelope2D, rhs: Envelope2D, cell: Double, qr: Long): Boolean = {
    val x5 = FastMath.max(lhs.xmin, rhs.xmin)
    val y5 = FastMath.max(lhs.ymin, rhs.ymin)

    val x6 = FastMath.min(lhs.xmax, rhs.xmax)
    val y6 = FastMath.min(lhs.ymax, rhs.ymax)

    if (x5 <= x6 && y5 <= y6) {
      qr == eval(x5, y5, cell)
    } else {
      false
    }
  }

  final def eval(lhs: Row, rhs: Row, cell: Double): Boolean = {
    val x1 = lhs.getDouble(1)
    val y1 = lhs.getDouble(2)
    val x2 = lhs.getDouble(3)
    val y2 = lhs.getDouble(4)

    val x3 = rhs.getDouble(1)
    val y3 = rhs.getDouble(2)
    val x4 = rhs.getDouble(3)
    val y4 = rhs.getDouble(4)

    val x5 = FastMath.max(x1, x3)
    val y5 = FastMath.max(y1, y3)

    val x6 = FastMath.min(x2, x4)
    val y6 = FastMath.min(y2, y4)

    if (x5 <= x6 && y5 <= y6) {
      lhs.getLong(0) == eval(x5, y5, cell)
    } else {
      false
    }
  }

  final def eval(lhs: InternalRow, rhs: InternalRow, cell: Double): Boolean = {
    val x1 = lhs.getDouble(1)
    val y1 = lhs.getDouble(2)
    val x2 = lhs.getDouble(3)
    val y2 = lhs.getDouble(4)

    val x3 = rhs.getDouble(1)
    val y3 = rhs.getDouble(2)
    val x4 = rhs.getDouble(3)
    val y4 = rhs.getDouble(4)

    val x5 = FastMath.max(x1, x3)
    val y5 = FastMath.max(y1, y3)

    val x6 = FastMath.min(x2, x4)
    val y6 = FastMath.min(y2, y4)

    if (x5 <= x6 && y5 <= y6) {
      lhs.getLong(0) == eval(x5, y5, cell)
    } else {
      false
    }
  }
}

final case class QRIntersect(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    QREnvpObj.dataType,
    QREnvpObj.dataType,
    DoubleType,
  )

  override def dataType: DataType = BooleanType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression) =>
        QRIntersectObj.eval(
          e1.eval(inputRow).asInstanceOf[InternalRow],
          e2.eval(inputRow).asInstanceOf[InternalRow],
          e3.eval(inputRow).asInstanceOf[Double],
        )
      case _ => false
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value
    val a3 = c3.value

    val obj = QRIntersectObj.getClass.getName.stripSuffix("$")
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
