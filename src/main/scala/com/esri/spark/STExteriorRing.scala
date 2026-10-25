package com.esri.spark

import com.esri.core.geometry.MultiPath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object STExteriorRingObj extends Serializable {
  final def eval(
                  bytes: Array[Byte]
                ): Array[Byte] = {
    bytes.geom match {
      case src: MultiPath =>
        val instance = src.createInstance()
        instance match {
          case dst: MultiPath =>
            dst.addPath(src, 0, true)
          case _ =>
        }
        instance.bytes
      case _ =>
        Array.emptyByteArray
    }

  }
}

final case class STExteriorRing(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
  )

  override def dataType: DataType = BinaryType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression) =>
        val v1 = e1.eval(inputRow)
        if (v1 == null) null
        else {
          STExteriorRingObj.eval(
            v1.asInstanceOf[Array[Byte]],
          )
        }
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)

    val a1 = c1.value

    val obj = STExteriorRingObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1)"
    ev.copy(code =
      code"""
        ${c1.code}
        boolean ${ev.isNull} = ${c1.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (!${ev.isNull}) {
          ${ev.value} = $objEval;
        }
        """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
