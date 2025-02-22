package com.esri.spark

import com.esri.core.geometry.{MultiPath, MultiPoint, Point}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._


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
      case src: MultiPath =>
        val pathCount = src.getPathCount
        //        var pathIndex = 0
        //        while (pathIndex < pathCount) {
        //          val mpp = src.createInstance().asInstanceOf[MultiPath]
        //          mpp.insertPath(0, src, pathIndex, true)
        //          arr append mpp.bytes
        //          pathIndex += 1
        //        }

        //        val results = (0 until pathCount)
        //          .par
        //          .map { pathIndex =>
        //            val mpp = src.createInstance().asInstanceOf[MultiPath]
        //            mpp.insertPath(0, src, pathIndex, true)
        //            mpp.bytes
        //          }
        //        arr ++= results.toIterator

        // TODO - Check pathCount and if it is less than parallel, then use parallel = pathCount
        val futures = (0 until pathCount).map { pathIndex =>
          Future {
            val mpp = src.createInstance().asInstanceOf[MultiPath]
            mpp.insertPath(0, src, pathIndex, true)
            mpp.bytes
          }
        }
        arr ++= Await.result(Future.sequence(futures), duration.Duration.Inf)
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
        STDumpObj.eval(
          e1.eval(inputRow).asInstanceOf[Array[Byte]],
        )
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
        ${CodeGenerator.javaType(dataType)} ${ev.value} = $objEval;
        """, isNull = FalseLiteral)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
