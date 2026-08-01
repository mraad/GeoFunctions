package com.esri.spark

import com.esri.core.geometry.Envelope2D
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

object QRListObj extends Serializable {

  final def eval(bytes: Array[Byte],
                 cell: Double,
                 dist: Double
                ): ArrayData = {
    require(java.lang.Double.isFinite(cell) && cell > 0.0,
      s"cell must be finite and positive, got $cell")
    require(java.lang.Double.isFinite(dist) && dist >= 0.0,
      s"dist must be finite and non-negative, got $dist")

    val envp = new Envelope2D()
    bytes.geom.queryEnvelope2D(envp)
    if (envp.isEmpty) {
      return UnsafeArrayData.fromPrimitiveArray(Array.empty[Long])
    }

    val xmin = envp.xmin - dist
    val ymin = envp.ymin - dist
    val xmax = envp.xmax + dist
    val ymax = envp.ymax + dist

    // Validate the floored doubles before converting them. Double.toLong saturates and
    // adding one to Long.MaxValue wraps, which can otherwise turn an invalid extent into
    // an empty scan. QR keys only retain the low 32 bits of q and r, so both indices must
    // fit in an Int to remain round-trippable by QR_ASGEOM.
    val qminD = (xmin / cell).floor
    val rminD = (ymin / cell).floor
    val qmaxD = (xmax / cell).floor
    val rmaxD = (ymax / cell).floor
    require(
      qminD >= Int.MinValue && qmaxD <= Int.MaxValue &&
        rminD >= Int.MinValue && rmaxD <= Int.MaxValue,
      s"cell range out of Int bounds: q=[$qminD,$qmaxD] r=[$rminD,$rmaxD] with cell=$cell dist=$dist"
    )

    val qmin = qminD.toLong
    val rmin = rminD.toLong
    val qmax = qmaxD.toLong + 1L
    val rmax = rmaxD.toLong + 1L
    val nq = qmax - qmin
    val nr = rmax - rmin
    // Check each factor before multiplying so the product cannot overflow Long. The
    // result must fit in an Int because both JVM arrays and Spark ArrayData are Int-sized.
    require(nq > 0L && nr > 0L && nq <= Int.MaxValue && nr <= Int.MaxValue && nq * nr <= Int.MaxValue,
      s"${nq}x${nr} cells for one geometry with cell=$cell dist=$dist - use a larger cell")

    val arr = new Array[Long]((nq * nr).toInt)
    var i = 0
    var q = qmin
    while (q < qmax) {
      val q32 = q << 32
      var r = rmin
      while (r < rmax) {
        arr(i) = q32 | (r & 0xFFFFFFFFL)
        i += 1
        r += 1L
      }
      q += 1L
    }
    UnsafeArrayData.fromPrimitiveArray(arr)
  }
}

final case class QRList(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
    DoubleType,
    DoubleType,
  )

  override def dataType: DataType = ArrayType(LongType, containsNull = false)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length != inputTypes.length) {
      TypeCheckResult.TypeCheckFailure(
        s"QR_LIST requires exactly ${inputTypes.length} arguments, got ${children.length}")
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
          QRListObj.eval(
            v1.asInstanceOf[Array[Byte]],
            v2.asInstanceOf[Double],
            v3.asInstanceOf[Double],
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
    val c3 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value
    val a3 = c3.value

    val obj = QRListObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2,$a3)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        boolean ${ev.isNull} = ${c1.isNull} || ${c2.isNull} || ${c3.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = null;
        if (!${ev.isNull}) {
          ${ev.value} = $objEval;
        }
        """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
