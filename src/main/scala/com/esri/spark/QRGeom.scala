package com.esri.spark

import com.esri.core.geometry.{Envelope, Envelope2D, Geometry, MultiPath, OperatorIntersection, Polygon, Polyline, SpatialReference}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// import scala.collection.mutable.ArrayBuffer

object QRGeomPar extends Serializable {
  private val operator = OperatorIntersection.local

  private final def accelerateGeometryNoop(geom: Geometry,
                                           sr: SpatialReference,
                                          ): Unit = {
    // No-op
  }

  private final def accelerateGeometryMild(geom: Geometry,
                                           sr: SpatialReference,
                                          ): Unit = {
    operator.accelerateGeometry(geom, sr, Geometry.GeometryAccelerationDegree.enumMild)
  }

  private val accelerateGeometry: (Geometry, SpatialReference) ⇒ Unit = {
    val accelerate = SparkSession
      .getActiveSession
      .forall(_.sparkContext.getConf.getBoolean("spark.esri.accelerate", defaultValue = true))

    if (accelerate) {
      accelerateGeometryMild
    } else {
      accelerateGeometryNoop
    }
  }

  private val parallel: Int = SparkSession
    .getActiveSession
    .map(_.sparkContext.getConf.getInt("spark.esri.parallel", 64))
    .getOrElse(64)

  final def eval(bytes: Array[Byte],
                 cell: Double,
                 dist: Double,
                 wkid: UTF8String
                ): ArrayData = {
    val sr = SpatialReferenceObj.create(wkid)

    val envp = new Envelope2D()
    val geom = bytes.geom
    geom.queryEnvelope2D(envp)

    val qmin = (envp.xmin / cell).floor.toLong
    val rmin = (envp.ymin / cell).floor.toLong
    val qmax = (envp.xmax / cell).floor.toLong + 1L
    val rmax = (envp.ymax / cell).floor.toLong + 1L
    val qRange = qmin until qmax
    val rRange = rmin until rmax

    accelerateGeometry(geom, sr)

    def processQR(q: Long,
                  r: Long
                 ): Option[InternalRow] = {
      val qr = (q << 32) | (r & 0xFFFFFFFFL)
      val xmin = q * cell
      val ymin = r * cell
      val xmax = xmin + cell
      val ymax = ymin + cell

      val cellEnvp = new Envelope(xmin - dist, ymin - dist, xmax + dist, ymax + dist)
      accelerateGeometry(cellEnvp, sr)

      @inline
      def optionalRow(mp: MultiPath,
                      pointCount: Int
                     ): Option[InternalRow] = {
        if (mp.getPathCount > 0 && mp.getPointCount > pointCount) {
          Some(InternalRow(qr, mp.bytes))
        } else {
          None
        }
      }

      operator.execute(geom, cellEnvp, sr, null) match {
        case polygon: Polygon =>
          optionalRow(polygon, 2)
        case polyline: Polyline =>
          optionalRow(polyline, 1)
        case envelope: Envelope ⇒
          Some(InternalRow(qr, envelope.bytes))
        case _ =>
          None
      }
    }

    val rows = {
      val qrCount = (qmax - qmin) * (rmax - rmin)
      if (qrCount > parallel) {
        qRange
          .flatMap(q ⇒
            rRange
              .map(r ⇒ (q, r))
          )
          .par
          .flatMap { case (q, r) =>
            processQR(q, r)
          }
          .seq
      } else {
        qRange
          .flatMap(q ⇒
            rRange
              .flatMap(r ⇒ processQR(q, r))
          )
      }
    }
    ArrayData.toArrayData(rows)
  }
}


//object QRClipObj extends Serializable {
//  private val operator = OperatorIntersection.local
//
//  final def eval(bytes: Array[Byte],
//                 cell: Double,
//                 dist: Double,
//                 wkid: UTF8String,
//                ): ArrayData = {
//    val sr = SpatialReferenceObj.create(wkid)
//    val arr = new ArrayBuffer[InternalRow]()
//    val dist2 = dist + dist
//    val envp = new Envelope2D()
//    val geom = bytes.geom
//    geom.queryEnvelope2D(envp)
//
//    val q_min = (envp.xmin / cell).floor.toLong
//    val r_min = (envp.ymin / cell).floor.toLong
//    val q_max = (envp.xmax / cell).floor.toLong + 1L
//    val r_max = (envp.ymax / cell).floor.toLong + 1L
//    val cellEnvp = new Envelope(0.0, 0.0, 1.0, 1.0)
//
//    var r = r_min
//    while (r < r_max) {
//      val ymin = r * cell - dist
//      val ymax = ymin + cell + dist2
//      var q = q_min
//      while (q < q_max) {
//        val xmin = q * cell - dist
//        val xmax = xmin + cell + dist2
//        cellEnvp.setCoords(xmin, ymin, xmax, ymax)
//
//        def arrAppend(mp: MultiPath,
//                      pointCount: Int
//                     ): Unit = {
//          if (mp.getPathCount > 0 && mp.getPointCount > pointCount) {
//            val qr = (q << 32) | (r & 0xFFFFFFFFL)
//            arr.append(InternalRow(qr, mp.bytes))
//          }
//        }
//
//        operator.execute(
//          geom,
//          cellEnvp,
//          sr,
//          null
//        ) match {
//          // TODO Handle case when point and multipoint.
//          case polygon: Polygon => arrAppend(polygon, 2)
//          case polyline: Polyline => arrAppend(polyline, 1)
//          case _ => //
//        }
//        q += 1L
//      }
//      r += 1L
//    }
//    ArrayData.toArrayData(arr)
//  }
//}

final case class QRGeom(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  override def inputTypes: Seq[DataType] = Seq(
    BinaryType,
    DoubleType,
    DoubleType,
    StringType,
  )

  override def dataType: DataType = ArrayType(
    StructType(Array(
      StructField("qr", LongType, nullable = false),
      StructField("geom", BinaryType, nullable = false)
    )), containsNull = false)

  override def eval(inputRow: InternalRow): Any = {
    children match {
      case Seq(e1: Expression, e2: Expression, e3: Expression, e4: Expression) =>
        QRGeomPar.eval(
          e1.eval(inputRow).asInstanceOf[Array[Byte]],
          e2.eval(inputRow).asInstanceOf[Double],
          e3.eval(inputRow).asInstanceOf[Double],
          e4.eval(inputRow).asInstanceOf[UTF8String],
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
    val c4 = children.last.genCode(ctx)

    val a1 = c1.value
    val a2 = c2.value
    val a3 = c3.value
    val a4 = c4.value

    val obj = QRGeomPar.getClass.getName.stripSuffix("$")
    val objEval = s"$obj.eval($a1,$a2,$a3,$a4)"
    ev.copy(code =
      code"""
        ${c1.code}
        ${c2.code}
        ${c3.code}
        ${c4.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = $objEval;
        """, isNull = FalseLiteral)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
