package com.esri.spark

import com.esri.core.geometry.{MultiPath, MultiPoint, Point, Polygon}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer


object STDumpObj extends Serializable {
  final def eval(
                  bytes: Array[Byte]
                ): ArrayData = {
    val arr = new ArrayBuffer[Array[Byte]]()
    bytes.geom match {
      case pt: Point =>
        if (!pt.isEmpty) {
          arr append bytes
        }
      case mp: MultiPoint =>
        val pt = new Point()
        val pc = mp.getPointCount
        var pi = 0
        while (pi < pc) {
          mp.getPointByVal(pi, pt)
          if (!pt.isEmpty) {
            arr append pt.bytes
          }
          pi += 1
        }
      case src: Polygon =>
        // A polygon part is an exterior ring plus its holes; a hole on its own would come out as
        // a solid polygon.
        PolygonRings.parts(src).foreach { rings =>
          val part = src.createInstance().asInstanceOf[Polygon]
          rings.foreach(part.addPath(src, _, true))
          arr append part.bytes
        }
      case src: MultiPath =>
        var i = 0
        while (i < src.getPathCount) {
          val part = src.createInstance().asInstanceOf[MultiPath]
          part.addPath(src, i, true)
          arr append part.bytes
          i += 1
        }
      case _ =>
    }
    ArrayData.toArrayData(arr)
  }
}

final case class STDump(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
  )

  override def dataType: DataType = ArrayType(BinaryType, containsNull = false)

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression) =>
        val v1 = e1.eval(inputRow)
        if (v1 == null) null
        else {
          STDumpObj.eval(
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

    val obj = STDumpObj.getClass.getName.stripSuffix("$")
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
