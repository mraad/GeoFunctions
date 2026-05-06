package com.esri.spark

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

object ClipLineObj extends Serializable {
  final def eval(arrData: ArrayData,
                 cell: Double
                ): ArrayData =
    ClipLineDistObj.eval(arrData, cell, 0.0)
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
    ClipLineObj.eval(
      i1.asInstanceOf[ArrayData],
      i2.asInstanceOf[Double])

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode =
    defineCodeGen(ctx, ev, (i1, i2) => s"com.esri.spark.ClipLineObj.eval($i1,$i2)")

  protected def withNewChildrenInternal(new1: Expression,
                                        new2: Expression
                                       ): Expression =
    copy(new1, new2)
}
