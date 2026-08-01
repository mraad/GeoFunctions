package com.esri.spark

import com.esri.core.geometry.Envelope2D
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

object QRIntersectObj extends Serializable {

  @inline
  private[spark] final def validateCell(cell: Double): Unit =
    require(java.lang.Double.isFinite(cell) && cell > 0.0,
      s"cell must be finite and positive, got $cell")

  @inline
  private final def qrAt(x: Double, y: Double, cell: Double): Long = {
    val q = Math.floor(x / cell)
    val r = Math.floor(y / cell)
    require(
      q >= Int.MinValue && q <= Int.MaxValue &&
        r >= Int.MinValue && r <= Int.MaxValue,
      s"cell index out of Int bounds: q=$q r=$r with cell=$cell"
    )
    (q.toLong << 32) | (r.toLong & 0xFFFFFFFFL)
  }

  /** True when x/y lies in the cell represented by qr.
   *
   * Comparing the decoded indices avoids rebuilding a packed Long and also makes an
   * out-of-Int coordinate fail closed instead of aliasing a valid QR through truncation.
   */
  @inline
  private final def isCanonicalCell(x: Double,
                                    y: Double,
                                    cell: Double,
                                    qr: Long
                                   ): Boolean =
    Math.floor(x / cell) == (qr >> 32).toDouble &&
      Math.floor(y / cell) == qr.toInt.toDouble

  @inline
  private final def evalBounds(lhsXMin: Double,
                               lhsYMin: Double,
                               lhsXMax: Double,
                               lhsYMax: Double,
                               rhsXMin: Double,
                               rhsYMin: Double,
                               rhsXMax: Double,
                               rhsYMax: Double,
                               cell: Double,
                               qr: Long
                              ): Boolean = {
    // Stage the axes so x-disjoint candidates do no y or cell-index work. The positive
    // overlap tests deliberately reject NaN bounds as well as disjoint envelopes.
    val xmin = Math.max(lhsXMin, rhsXMin)
    val xmax = Math.min(lhsXMax, rhsXMax)
    if (!(xmin <= xmax)) {
      false
    } else {
      val ymin = Math.max(lhsYMin, rhsYMin)
      val ymax = Math.min(lhsYMax, rhsYMax)
      ymin <= ymax && isCanonicalCell(xmin, ymin, cell, qr)
    }
  }

  @inline // Leave inline - make sure -opt:l:inline is enabled as a compiler arg
  final def eval(x: Double, y: Double, cell: Double): Long = {
    validateCell(cell)
    qrAt(x, y, cell)
  }

  final def eval(lhs: Envelope2D, rhs: Envelope2D, cell: Double, qr: Long): Boolean = {
    validateCell(cell)
    evalUnchecked(lhs, rhs, cell, qr)
  }

  /** Join-only entry point after the invariant cell size has been validated once. */
  @inline
  private[spark] final def evalUnchecked(lhs: Envelope2D,
                                         rhs: Envelope2D,
                                         cell: Double,
                                         qr: Long
                                        ): Boolean =
    evalBounds(
      lhs.xmin, lhs.ymin, lhs.xmax, lhs.ymax,
      rhs.xmin, rhs.ymin, rhs.xmax, rhs.ymax,
      cell, qr,
    )

  final def eval(lhs: Row, rhs: Row, cell: Double): Boolean = {
    validateCell(cell)
    val lhsQR = lhs.getLong(0)
    lhsQR == rhs.getLong(0) && evalBounds(
      lhs.getDouble(1), lhs.getDouble(2), lhs.getDouble(3), lhs.getDouble(4),
      rhs.getDouble(1), rhs.getDouble(2), rhs.getDouble(3), rhs.getDouble(4),
      cell, lhsQR,
    )
  }

  final def eval(lhs: InternalRow, rhs: InternalRow, cell: Double): Boolean = {
    validateCell(cell)
    val lhsQR = lhs.getLong(0)
    lhsQR == rhs.getLong(0) && evalBounds(
      lhs.getDouble(1), lhs.getDouble(2), lhs.getDouble(3), lhs.getDouble(4),
      rhs.getDouble(1), rhs.getDouble(2), rhs.getDouble(3), rhs.getDouble(4),
      cell, lhsQR,
    )
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

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length != inputTypes.length) {
      TypeCheckResult.TypeCheckFailure(
        s"QR_INTERSECT requires exactly ${inputTypes.length} arguments, got ${children.length}")
    } else {
      super.checkInputDataTypes()
    }
  }

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression) =>
        val v1 = e1.eval(inputRow)
        val v2 = e2.eval(inputRow)
        val v3 = e3.eval(inputRow)
        if (v1 == null || v2 == null || v3 == null) null
        else {
          QRIntersectObj.eval(
            v1.asInstanceOf[InternalRow],
            v2.asInstanceOf[InternalRow],
            v3.asInstanceOf[Double],
          )
        }
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children(2).genCode(ctx)

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
        boolean ${ev.isNull} = ${c1.isNull} || ${c2.isNull} || ${c3.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = false;
        if (!${ev.isNull}) {
          ${ev.value} = $objEval;
        }
        """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
