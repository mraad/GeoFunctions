package com.esri.spark

import com.esri.core.geometry.Envelope2D
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._


object STIntersectsBoxObj extends Serializable {
  final def eval(bytes: Array[Byte],
                 xmin: Double,
                 ymin: Double,
                 xmax: Double,
                 ymax: Double
                ): Boolean = {
    val geom = bytes.geom
    val envp2D = new Envelope2D()
    geom.queryEnvelope2D(envp2D)
    (if (envp2D.xmin <= xmin) {
      envp2D.xmax >= xmin
    }
    else {
      xmax >= envp2D.xmin
    }) &&
      (if (envp2D.ymin <= ymin) {
        envp2D.ymax >= ymin
      }
      else {
        ymax >= envp2D.ymin
      })
  }
}

final case class STIntersectsBox(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType, // Geom
    DoubleType, // xmin
    DoubleType, // ymin
    DoubleType, // xmax
    DoubleType // ymax
  )

  override def dataType: DataType = BooleanType

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression, e4: Expression, e5: Expression) =>
        STIntersectsBoxObj.eval(
          e1.eval(inputRow).asInstanceOf[Array[Byte]],
          e2.eval(inputRow).asInstanceOf[Double],
          e3.eval(inputRow).asInstanceOf[Double],
          e4.eval(inputRow).asInstanceOf[Double],
          e5.eval(inputRow).asInstanceOf[Double]
        )
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val c2 = children(1).genCode(ctx)
    val c3 = children(2).genCode(ctx)
    val c4 = children(3).genCode(ctx)
    val c5 = children(4).genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value
    val a3 = c3.value
    val a4 = c4.value
    val a5 = c5.value

    val obj = STIntersectsBoxObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2,$a3,$a4,$a5)"
    // s to code, ctx to CodeGenerator, false to FalseLiteral
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        ${c4.code}
        ${c5.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = $objEval;
        """, isNull = FalseLiteral)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
