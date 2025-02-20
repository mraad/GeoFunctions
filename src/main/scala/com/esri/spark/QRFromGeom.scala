package com.esri.spark

import com.esri.core.geometry.Envelope2D
import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

object QRFromGeomObj extends Serializable {

  final def eval(bytes: Array[Byte],
                 cell: Double
                ): Long = {
    val envp = new Envelope2D()
    bytes.geom.queryEnvelope2D(envp)
    val q = FastMath.floor(envp.xmin / cell).toLong << 32
    val r = FastMath.floor(envp.ymin / cell).toLong & 0xFFFFFFFFL
    q | r
  }
}

final case class QRFromGeom(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
    DoubleType,
  )

  override def dataType: DataType = LongType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(
      exprH: Expression,
      exprL: Expression
      ) =>
        QRFromGeomObj.eval(
          exprH.eval(inputRow).asInstanceOf[Array[Byte]],
          exprL.eval(inputRow).asInstanceOf[Double],
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val codeH = children.head.genCode(ctx)
    val codeL = children.last.genCode(ctx)

    val valueH = codeH.value
    val valueL = codeL.value

    val obj = QRFromGeomObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($valueH,$valueL)"
    ev.copy(code =
      code"""
        ${codeH.code}
        ${codeL.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = $objEval;
        """, isNull = FalseLiteral)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
