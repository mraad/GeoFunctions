package com.esri.spark

import com.esri.core.geometry.Envelope2D
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

object QRContainsGeomObj extends Serializable {
  final def eval(qr: Long,
                 cell: Double,
                 bytes: Array[Byte]
                ): Boolean = {
    val envp2D = new Envelope2D()
    bytes.geom.queryEnvelope2D(envp2D)
    val q1 = (envp2D.xmin / cell).floor.toLong
    val r1 = (envp2D.ymin / cell).floor.toLong
    val q2 = (envp2D.xmax / cell).floor.toLong
    val r2 = (envp2D.ymax / cell).floor.toLong
    val qr1 = (q1 << 32) | (r1 & 0xFFFFFFFFL)
    val qr2 = (q2 << 32) | (r2 & 0xFFFFFFFFL)
    qr1 == qr && qr2 == qr
  }

}

final case class QRContainsGeom(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    LongType, DoubleType, BinaryType,
  )

  override def dataType: DataType = BooleanType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(
      e1: Expression,
      e2: Expression,
      e3: Expression,
      ) =>
        QRContainsGeomObj.eval(
          e1.eval(inputRow).asInstanceOf[Long],
          e2.eval(inputRow).asInstanceOf[Double],
          e3.eval(inputRow).asInstanceOf[Array[Byte]],
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

    val obj = QRContainsGeomObj.getClass.getName.stripSuffix("$")
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
