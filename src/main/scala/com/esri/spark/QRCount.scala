package com.esri.spark

import com.esri.core.geometry.Envelope2D
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

object QRCountObj extends Serializable {

  final def eval(bytes: Array[Byte],
                 cell: Double,
                 dist: Double
                ): Int = {
    require(cell > 0.0, s"cell must be positive, got $cell")
    require(dist >= 0.0, s"dist must be non-negative, got $dist")
    val geom = bytes.geom

    val envp = new Envelope2D()
    geom.queryEnvelope2D(envp)

    val xmin = envp.xmin - dist
    val ymin = envp.ymin - dist
    val xmax = envp.xmax + dist
    val ymax = envp.ymax + dist

    // Long math throughout: a small cell over a large extent overflows Int in the floor
    // conversions and again in the product, which used to surface as a negative count.
    val qmin = (xmin / cell).floor.toLong
    val rmin = (ymin / cell).floor.toLong
    val qmax = (xmax / cell).floor.toLong + 1L
    val rmax = (ymax / cell).floor.toLong + 1L

    val count = (qmax - qmin) * (rmax - rmin)
    require(count > 0L && count <= Int.MaxValue,
      s"$count cells for one geometry with cell=$cell dist=$dist - use a larger cell")
    count.toInt
  }
}

final case class QRCount(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
    DoubleType,
    DoubleType,
  )

  override def dataType: DataType = IntegerType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression) =>
        QRCountObj.eval(
          e1.eval(inputRow).asInstanceOf[Array[Byte]],
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

    val obj = QRCountObj.getClass.getName.stripSuffix("$")
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
