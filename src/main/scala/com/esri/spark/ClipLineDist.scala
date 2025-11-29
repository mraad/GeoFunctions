package com.esri.spark

import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, TernaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object ClipLineDistObj extends Serializable {
  final def apply(arrData: ArrayData, cell: Double, dist: Double): ArrayData = {
    val arr = new ArrayBuffer[InternalRow]()
    arrData.toDoubleArray() match {
      case Array(x1, y1, x2, y2) =>
        val qmin = FastMath.floor((x1 min x2) / cell).toInt
        val qmax = FastMath.floor((x1 max x2) / cell).toInt + 1L
        val rmin = FastMath.floor((y1 min y2) / cell).toInt
        val rmax = FastMath.floor((y1 max y2) / cell).toInt + 1L
        val chip = cell + dist + dist
        var q = qmin
        while (q < qmax) {
          val xmin = q * cell - dist
          var r = rmin
          while (r < rmax) {
            val ymin = r * cell - dist
            val l = Rect(xmin, ymin, chip).clip(x1, y1, x2, y2)
            if (l > 0.0) {
              val writer = new UnsafeRowWriter(3)
              writer.resetRowWriter()
              writer.write(0, q)
              writer.write(1, r)
              writer.write(2, l)
              arr.append(writer.getRow)
            }
            r += 1
          }
          q += 1
        }
      case _ =>
    }
    ArrayData.toArrayData(arr)
  }

}

case class ClipLineDist(first: Expression, second: Expression, third: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  // override val children: Seq[Expression] = Seq(first, second, third)

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
    ClipLineDistObj(
      i1.asInstanceOf[ArrayData],
      i2.asInstanceOf[Double],
      i3.asInstanceOf[Double],
    )

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (i1, i2, i3) => s"com.esri.spark.ClipLineDistObj.apply($i1,$i2,$i3)")

  protected def withNewChildrenInternal(e1: Expression, e2: Expression, e3: Expression): Expression =
    copy(e1, e2, e3)
}
