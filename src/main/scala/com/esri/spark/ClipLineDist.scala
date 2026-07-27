package com.esri.spark

import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, TernaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object ClipLineDistObj extends Serializable {
  final def eval(arrData: ArrayData, cell: Double, dist: Double): ArrayData = {
    require(cell > 0.0, s"cell must be positive, got $cell")
    require(dist >= 0.0, s"dist must be non-negative, got $dist")
    if (arrData.numElements() != 4) {
      throw new IllegalArgumentException(
        s"expected array of 4 doubles (x1,y1,x2,y2), got ${arrData.numElements()}: ${arrData.toDoubleArray().mkString(",")}"
      )
    }
    val x1 = arrData.getDouble(0)
    val y1 = arrData.getDouble(1)
    val x2 = arrData.getDouble(2)
    val y2 = arrData.getDouble(3)

    // Cell (q,r) spans [q*cell-dist, (q+1)*cell+dist] on each axis, so the candidate
    // range is the segment extent inflated by dist - same convention as QRList/QREnvp.
    val qminL = FastMath.floor(((x1 min x2) - dist) / cell).toLong
    val qmaxL = FastMath.floor(((x1 max x2) + dist) / cell).toLong + 1L
    val rminL = FastMath.floor(((y1 min y2) - dist) / cell).toLong
    val rmaxL = FastMath.floor(((y1 max y2) + dist) / cell).toLong + 1L
    require(
      qminL >= Int.MinValue && qmaxL <= Int.MaxValue &&
        rminL >= Int.MinValue && rmaxL <= Int.MaxValue,
      s"cell range out of Int bounds: q=[$qminL,$qmaxL) r=[$rminL,$rmaxL)"
    )
    val qmin = qminL.toInt
    val qmax = qmaxL.toInt
    val rmin = rminL.toInt
    val rmax = rmaxL.toInt
    val chip = cell + dist + dist
    val dx = x2 - x1
    val dy = y2 - y1
    val arr = new ArrayBuffer[InternalRow]()
    var q = qmin
    while (q < qmax) {
      val xmin = q * cell - dist
      // Clip the r scan to the segment's y-extent inside this column's x-slab.
      // Scanning the full bbox is O(dq*dr) and most of those cells miss the segment.
      var rlo = rmin
      var rhi = rmax
      if (dx != 0.0) {
        val t0 = (xmin - x1) / dx
        val t1 = (xmin + chip - x1) / dx
        val tlo = 0.0 max (t0 min t1)
        val thi = 1.0 min (t0 max t1)
        if (tlo > thi) {
          rhi = rlo // x-slab misses the segment entirely
        } else {
          val ya = y1 + tlo * dy
          val yb = y1 + thi * dy
          rlo = rmin max FastMath.floor(((ya min yb) - dist) / cell).toInt
          rhi = rmax min (FastMath.floor(((ya max yb) + dist) / cell).toInt + 1)
        }
      }
      var r = rlo
      while (r < rhi) {
        val ymin = r * cell - dist
        val l = Rect(xmin, ymin, chip).clip(x1, y1, x2, y2)
        if (l > 0.0) {
          arr.append(InternalRow(q, r, l))
        }
        r += 1
      }
      q += 1
    }
    ArrayData.toArrayData(arr)
  }
}

case class ClipLineDist(first: Expression, second: Expression, third: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[DataType] = Seq(
    ArrayType(DoubleType, containsNull = false),
    DoubleType,
    DoubleType,
  )

  override def dataType: DataType = ArrayType(StructType(Array(
    StructField("q", IntegerType, nullable = false),
    StructField("r", IntegerType, nullable = false),
    StructField("l", DoubleType, nullable = false),
  )), containsNull = false)

  override protected def nullSafeEval(i1: Any, i2: Any, i3: Any): Any =
    ClipLineDistObj.eval(
      i1.asInstanceOf[ArrayData],
      i2.asInstanceOf[Double],
      i3.asInstanceOf[Double],
    )

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (i1, i2, i3) => s"com.esri.spark.ClipLineDistObj.eval($i1,$i2,$i3)")

  protected def withNewChildrenInternal(e1: Expression, e2: Expression, e3: Expression): Expression =
    copy(e1, e2, e3)
}
