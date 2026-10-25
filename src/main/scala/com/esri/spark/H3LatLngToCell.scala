package com.esri.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

object H3LatLngToCellObj extends Serializable {
  // res is Int, not java.lang.Integer: codegen hands us a primitive and H3Core takes one,
  // so the boxed type only added a round trip.
  final def eval(lat: Double,
                 lon: Double,
                 res: Int,
                ): Long = {
    H3Instance.h3.latLngToCell(lat, lon, res)
  }
}

final case class H3LatLngToCell(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    DoubleType,
    DoubleType,
    IntegerType,
  )

  override def dataType: DataType = LongType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression) =>
        val v1 = e1.eval(inputRow)
        val v2 = e2.eval(inputRow)
        val v3 = e3.eval(inputRow)
        if (v1 == null || v2 == null || v3 == null) null
        else {
          H3LatLngToCellObj.eval(
            v1.asInstanceOf[Double],
            v2.asInstanceOf[Double],
            v3.asInstanceOf[Int],
          )
        }
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children(2).genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value
    val a3 = c3.value

    val obj = H3LatLngToCellObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2,$a3)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        boolean ${ev.isNull} = ${c1.isNull} || ${c2.isNull} || ${c3.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (!${ev.isNull}) {
          ${ev.value} = $objEval;
        }
        """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
