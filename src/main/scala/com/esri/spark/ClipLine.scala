package com.esri.spark

import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object ClipLineObj extends Serializable {
  final def apply(arrData: ArrayData,
                  cell: Double
                 ): ArrayData = {
    val arr = new ArrayBuffer[InternalRow]()
    arrData.toDoubleArray() match {
      case Array(x1, y1, x2, y2) =>
        val qmin = FastMath.floor((x1 min x2) / cell).toInt
        val qmax = FastMath.floor((x1 max x2) / cell).toInt + 1L
        val rmin = FastMath.floor((y1 min y2) / cell).toInt
        val rmax = FastMath.floor((y1 max y2) / cell).toInt + 1L
        var q = qmin
        while (q < qmax) {
          val xmin = q * cell
          var r = rmin
          while (r < rmax) {
            val ymin = r * cell
            val l = Rect(xmin, ymin, cell).clip(x1, y1, x2, y2)
            if (l > 0.0) {
              val writer = new UnsafeRowWriter(3)
              writer.resetRowWriter()
              writer.write(0, q)
              writer.write(1, r)
              writer.write(2, l)
              val row = writer.getRow
              arr.append(row)
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

case class ClipLine(left: Expression,
                    right: Expression
                   )
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[DataType] = Seq(
    ArrayType(DoubleType, containsNull = false), // x1, y1, x2, y2
    DoubleType)

  override def dataType: DataType = ArrayType(StructType(Array(
    StructField("q", IntegerType, nullable = false),
    StructField("r", IntegerType, nullable = false),
    StructField("l", DoubleType, nullable = false),
  )), containsNull = false)

  override protected def nullSafeEval(i1: Any,
                                      i2: Any
                                     ): Any =
    ClipLineObj(
      i1.asInstanceOf[ArrayData],
      i2.asInstanceOf[Double])

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode =
    defineCodeGen(ctx, ev, (i1, i2) => s"com.esri.spark.ClipLineObj.apply($i1,$i2)")

  protected def withNewChildrenInternal(new1: Expression,
                                        new2: Expression
                                       ): Expression =
    copy(new1, new2)
}
