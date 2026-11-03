package com.esri.spark

import com.esri.core.geometry.{Envelope2D, OperatorDistance}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

/**
 * `ST_DISTANCE(lhs, rhs) <= dist`, planar. A geometry repeated on every row (literal or scalar
 * subquery) is parsed once through [[WkbCache]], and pairs whose envelopes are already farther
 * apart than `dist` skip the exact distance.
 */
final class STDWithinEval extends Serializable {

  @transient private lazy val lhsCache = new WkbCache

  @transient private lazy val rhsCache = new WkbCache

  def eval(lhs: Array[Byte], rhs: Array[Byte], dist: Double): Boolean = {
    val l = lhsCache(lhs)
    val r = rhsCache(rhs)
    val le = new Envelope2D()
    val re = new Envelope2D()
    l.queryEnvelope2D(le)
    r.queryEnvelope2D(re)
    // Envelope distance is a lower bound on the exact distance, so the short-circuit never changes the answer.
    le.distance(re) <= dist && OperatorDistance.local.execute(l, r, null) <= dist
  }
}

final case class STDWithin(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
    BinaryType,
    DoubleType,
  )

  override def dataType: DataType = BooleanType

  @transient private lazy val within = new STDWithinEval

  override def eval(inputRow: InternalRow): Any = {
    val v1 = children.head.eval(inputRow)
    val v2 = children(1).eval(inputRow)
    val v3 = children(2).eval(inputRow)
    if (v1 == null || v2 == null || v3 == null) null
    else {
      within.eval(
        v1.asInstanceOf[Array[Byte]],
        v2.asInstanceOf[Array[Byte]],
        v3.asInstanceOf[Double],
      )
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children(2).genCode(ctx)

    val obj = ctx.addReferenceObj("dwithin", new STDWithinEval, classOf[STDWithinEval].getName)
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        boolean ${ev.isNull} = ${c1.isNull} || ${c2.isNull} || ${c3.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (!${ev.isNull}) {
          ${ev.value} = $obj.eval(${c1.value}, ${c2.value}, ${c3.value});
        }
        """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
