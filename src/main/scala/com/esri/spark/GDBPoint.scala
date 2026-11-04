package com.esri.spark

import com.esri.core.geometry.Point
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

/**
 * The gdb reader returns a point layer's Shape as a named struct: (x, y), (x, y, z), (x, y, m)
 * or (x, y, z, m), with NaN coordinates for an empty point. The field names say which is which,
 * so one function covers all four layouts. Indices are -1 for an absent field.
 */
object GDBPointObj extends Serializable {
  final def eval(row: InternalRow, xi: Int, yi: Int, zi: Int, mi: Int): Array[Byte] = {
    require(xi >= 0 && yi >= 0, "GDB_POINT needs a struct with x and y fields")
    val x = row.getDouble(xi)
    val y = row.getDouble(yi)
    val point = if (x.isNaN || y.isNaN) new Point() else new Point(x, y)
    if (!point.isEmpty) {
      if (zi >= 0) point.setZ(row.getDouble(zi))
      if (mi >= 0) point.setM(row.getDouble(mi))
    }
    point.bytes
  }
}

final case class GDBPoint(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[StructType.type] = Seq(
    StructType,
  )

  override def dataType: DataType = BinaryType

  private def index(name: String): Int = children.head.dataType match {
    case s: StructType => s.fieldNames.indexWhere(_.equalsIgnoreCase(name))
    case _ => -1
  }

  private lazy val indices = (index("x"), index("y"), index("z"), index("m"))

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression) =>
        val v1 = e1.eval(inputRow)
        if (v1 == null) null
        else {
          val (xi, yi, zi, mi) = indices
          GDBPointObj.eval(v1.asInstanceOf[InternalRow], xi, yi, zi, mi)
        }
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode
                                  ): ExprCode = {
    val c1 = children.head.genCode(ctx)
    val (xi, yi, zi, mi) = indices
    val obj = GDBPointObj.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval(${c1.value}, $xi, $yi, $zi, $mi)"
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
