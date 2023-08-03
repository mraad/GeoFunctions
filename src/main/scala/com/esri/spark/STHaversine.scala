package com.esri.spark

import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object STHaversineObj extends Serializable {
  final def eval(lon1: Double,
                 lat1: Double,
                 lon2: Double,
                 lat2: Double,
                ): Double = {
    val dLat = FastMath.toRadians(lat2 - lat1)
    val dLon = FastMath.toRadians(lon2 - lon1)
    val sinLat2 = FastMath.sin(dLat * 0.5)
    val sinLon2 = FastMath.sin(dLon * 0.5)
    val a = sinLat2 * sinLat2 +
      sinLon2 * sinLon2 *
        FastMath.cos(FastMath.toRadians(lat1)) * FastMath.cos(FastMath.toRadians(lat2))
    12756274.0 * FastMath.atan2(FastMath.sqrt(a), FastMath.sqrt(1.0 - a))
  }
}

final case class STHaversine(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    DoubleType,
    DoubleType,
    DoubleType,
    DoubleType,
  )

  override def dataType: DataType = DoubleType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression, e4: Expression) =>
        STHaversineObj.eval(
          e1.eval(inputRow).asInstanceOf[Double],
          e2.eval(inputRow).asInstanceOf[Double],
          e3.eval(inputRow).asInstanceOf[Double],
          e4.eval(inputRow).asInstanceOf[Double],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children(2).genCode(ctx)
    val c4 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value
    val a3 = c3.value
    val a4 = c4.value

    val obj = STHaversineObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2,$a3,$a4)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        ${c4.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = $objEval;
        """, isNull = FalseLiteral)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
