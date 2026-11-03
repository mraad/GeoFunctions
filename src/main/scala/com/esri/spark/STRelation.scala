package com.esri.spark

import com.esri.core.geometry.Geometry.GeometryAccelerationDegree
import com.esri.core.geometry.{Geometry, Operator, OperatorFactoryLocal, OperatorSimpleRelation, SpatialReference}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Evaluates one simple relation (intersects, contains, ...) over WKB arguments.
 *
 * A geometry repeated on every row (literal or scalar subquery) is parsed once through
 * [[WkbCache]] and accelerated on its first reuse, so a point-in-big-polygon filter costs a
 * point parse per row. Only the operator type is serialized; the caches start empty wherever
 * the instance is deserialized.
 */
final class STRelationEval(opType: Operator.Type) extends Serializable {

  @transient private lazy val operator: OperatorSimpleRelation =
    OperatorFactoryLocal.getInstance().getOperator(opType).asInstanceOf[OperatorSimpleRelation]

  @transient private lazy val lhsCache = new WkbCache

  @transient private lazy val rhsCache = new WkbCache

  private def accelerate(sr: SpatialReference)(geom: Geometry): Unit = {
    if (STRelationEval.accelerate && operator.canAccelerateGeometry(geom)) {
      operator.accelerateGeometry(geom, sr, GeometryAccelerationDegree.enumMedium)
    }
  }

  def eval(lhs: Array[Byte], rhs: Array[Byte], wkid: UTF8String): Boolean = {
    val sr = SpatialReferenceObj.create(wkid)
    operator.execute(lhsCache(lhs, accelerate(sr)), rhsCache(rhs, accelerate(sr)), sr, null)
  }
}

object STRelationEval {
  // SparkEnv, not SparkSession: getActiveSession is empty on executors (same as QRScan).
  private lazy val accelerate: Boolean =
    Option(SparkEnv.get).forall(_.conf.getBoolean("spark.esri.accelerate", defaultValue = true))
}

/** `(lhs: WKB, rhs: WKB, wkid: String) => Boolean` for one `OperatorSimpleRelation`. */
abstract class STRelation extends Expression with ImplicitCastInputTypes {

  protected def opType: Operator.Type

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
    BinaryType,
    StringType,
  )

  override def dataType: DataType = BooleanType

  @transient private lazy val relation = new STRelationEval(opType)

  override def eval(inputRow: InternalRow): Any = {
    val v1 = children.head.eval(inputRow)
    val v2 = children(1).eval(inputRow)
    val v3 = children(2).eval(inputRow)
    if (v1 == null || v2 == null || v3 == null) null
    else {
      relation.eval(
        v1.asInstanceOf[Array[Byte]],
        v2.asInstanceOf[Array[Byte]],
        v3.asInstanceOf[UTF8String],
      )
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children(2).genCode(ctx)

    // References ship inside the task closure, so each task deserializes its own caches.
    val rel = ctx.addReferenceObj("relation", new STRelationEval(opType), classOf[STRelationEval].getName)
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        boolean ${ev.isNull} = ${c1.isNull} || ${c2.isNull} || ${c3.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (!${ev.isNull}) {
          ${ev.value} = $rel.eval(${c1.value}, ${c2.value}, ${c3.value});
        }
        """)
  }
}
