package com.esri.spark

import com.esri.core.geometry.Geometry.GeometryAccelerationDegree
import com.esri.core.geometry.{Envelope2D, OperatorWithin, Point}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * `ST_WITHIN(points(i), polygon)` for every element, with the polygon read once.
 *
 * Per-row `ST_WITHIN(point, polygon)` copies and parses the polygon on every row (Spark's
 * `getBinary` hands each row its own copy): a point-in-polygon join over 156k zones moved
 * ~1 TB of polygon bytes at SpatialBench SF1. Grouping a polygon's candidate points into one
 * array and testing them here copies, parses and accelerates the polygon once. Points outside
 * the polygon's envelope are rejected without the exact test (they cannot be within it).
 */
object STWithinMaskObj extends Serializable {

  /** Accelerate the polygon only when enough points share it to repay the index build. */
  private val AccelerateAbove = 16

  final def eval(points: ArrayData, polygon: Array[Byte], wkid: UTF8String): ArrayData = {
    val n = points.numElements()
    val out = new Array[Any](n)
    val poly = polygon.geom
    val sr = SpatialReferenceObj.create(wkid)
    val op = OperatorWithin.local
    if (n > AccelerateAbove && STRelationEval.accelerate && op.canAccelerateGeometry(poly)) {
      op.accelerateGeometry(poly, sr, GeometryAccelerationDegree.enumMedium)
    }
    val env = new Envelope2D()
    poly.queryEnvelope2D(env)
    var i = 0
    while (i < n) {
      out(i) = if (points.isNullAt(i)) null else {
        points.getBinary(i).geom match {
          case p: Point if !p.isEmpty && !env.isEmpty && !env.contains(p.getX, p.getY) => false
          case g => op.execute(g, poly, sr, null)
        }
      }
      i += 1
    }
    new GenericArrayData(out)
  }
}

final case class STWithinMask(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  // One type per child even for a bad arity, so the analyzer reaches checkInputDataTypes (as in STRelation).
  override def inputTypes: Seq[DataType] =
    children.indices.map(i => if (i == 0) ArrayType(BinaryType) else if (i == 1) BinaryType else StringType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length != 3) {
      TypeCheckResult.TypeCheckFailure(
        s"ST_WITHIN_MASK requires 3 arguments (points, polygon, wkid), got ${children.length}")
    } else {
      super.checkInputDataTypes()
    }
  }

  override def dataType: DataType = ArrayType(BooleanType, containsNull = true)

  override def eval(inputRow: InternalRow): Any = {
    val v1 = children.head.eval(inputRow)
    val v2 = children(1).eval(inputRow)
    val v3 = children(2).eval(inputRow)
    if (v1 == null || v2 == null || v3 == null) null
    else {
      STWithinMaskObj.eval(
        v1.asInstanceOf[ArrayData],
        v2.asInstanceOf[Array[Byte]],
        v3.asInstanceOf[UTF8String],
      )
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children(2).genCode(ctx)

    val obj = STWithinMaskObj.getClass.getName.stripSuffix("$")
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
