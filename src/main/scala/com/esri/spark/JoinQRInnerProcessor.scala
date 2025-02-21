package com.esri.spark

import com.esri.core.geometry.Geometry.GeometryAccelerationDegree
import com.esri.core.geometry.{Envelope2D, Geometry, OperatorContains, OperatorCrosses, OperatorDisjoint, OperatorEquals, OperatorIntersects, OperatorOverlaps, OperatorSimpleRelation, OperatorTouches, OperatorWithin}
import com.github.plokhotnyuk.rtree2d.core._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.LoggerFactory


final case class JoinQRInnerArgs(cell: Double,
                                 qL: Int,
                                 qR: Int,
                                 geomL: Int = -1,
                                 geomR: Int = -1,
                                 oper: String = "none",
                                 wkid: String = "-1",
                                 acceleration: String = "mild",
                                )

final case class JoinQRInnerElem(ext: Envelope2D,
                                 seq: scala.Seq[Any],
                                )

final case class JoinQRInnerGeom(ext: Envelope2D,
                                 seq: scala.Seq[Any],
                                 geom: Geometry,
                                )

object JoinQRInnerProcessor extends Serializable {
  final def apply(lhs: Dataset[Row],
                  rhs: Dataset[Row],
                  cell: Double,
                  qrField: String = "qr",
                  oper: String = "none",
                  wkid: String = "-1",
                  lhsGeom: String = "geom",
                  rhsGeom: String = "geom",
                  acceleration: String = "mild",
                 ): Dataset[Row] = {
    lazy val logger = LoggerFactory.getLogger(getClass)

    val spark = lhs.sparkSession // SparkSession.builder().getOrCreate()
    import spark.implicits._
    // Look for qr in the lhs and rhs df.
    val qrL = lhs.schema.fieldIndex(qrField)
    val qrR = rhs.schema.fieldIndex(qrField)
    val geomL = oper match {
      case "none" => -1
      case _ => lhs.schema.fieldIndex(lhsGeom)
    }
    val geomR = oper match {
      case "none" => -1
      case _ ⇒ rhs.schema.fieldIndex(rhsGeom)
    }

    require(qrL != -1, s"Missing `$qrField` field in LHS dataframe.")
    require(qrR != -1, s"Missing `$qrField` field in RHS dataframe.")
    if (oper != "none") {
      require(geomL != -1, s"Missing `$lhsGeom` field in LHS dataframe.")
      require(geomR != -1, s"Missing `$rhsGeom` field in RHS dataframe.")
    }

    // Broadcast the need variables.
    val bv = spark.sparkContext.broadcast(
      JoinQRInnerArgs(cell, qrL, qrR, geomL, geomR, oper, wkid, acceleration)
    )

    // Define resulting schema.
    val lhsFields = lhs.schema.filter(_.name != qrField)
    val rhsFields = rhs.schema.filter(_.name != qrField)
    val schema = StructType(lhsFields ++ rhsFields)

    val lhsGroup = lhs
      .groupByKey(row => {
        row.getStruct(bv.value.qL).getLong(0)
      })

    val rhsGroup = rhs
      .groupByKey(row => {
        row.getStruct(bv.value.qR).getLong(0)
      })

    lhsGroup
      .cogroup(rhsGroup)((qrGroup, lhsIter, rhsIter) => {
        try {
          if (lhsIter.isEmpty || rhsIter.isEmpty) {
            Iterator.empty
          } else {
            bv.value.oper.toLowerCase match {
              case "contains" =>
                operLocal(qrGroup, lhsIter, rhsIter, bv, OperatorContains.local)
              case "crosses" =>
                operLocal(qrGroup, lhsIter, rhsIter, bv, OperatorCrosses.local)
              case "disjoint" =>
                operLocal(qrGroup, lhsIter, rhsIter, bv, OperatorDisjoint.local)
              case "equals" =>
                operLocal(qrGroup, lhsIter, rhsIter, bv, OperatorEquals.local)
              case "intersects" =>
                operLocal(qrGroup, lhsIter, rhsIter, bv, OperatorIntersects.local)
              case "overlaps" =>
                operLocal(qrGroup, lhsIter, rhsIter, bv, OperatorOverlaps.local)
              case "touches" =>
                operLocal(qrGroup, lhsIter, rhsIter, bv, OperatorTouches.local)
              case "within" =>
                operLocal(qrGroup, lhsIter, rhsIter, bv, OperatorWithin.local)
              case _ =>
                operNone(qrGroup, lhsIter, rhsIter, bv)
            }
          }
        } catch {
          case t: Throwable =>
            logger.error(t.getMessage, t)
            Iterator.empty
        }
      })(ExpressionEncoder(schema))
  }

  private def operNone(qrGroup: Long,
                       lhsIter: Iterator[Row],
                       rhsIter: Iterator[Row],
                       bv: Broadcast[JoinQRInnerArgs]
                      ) = {
    val iter = rhsIter
      .map {
        row => {
          val qr = row.getStruct(bv.value.qR)
          val ext = new Envelope2D(qr.getDouble(1), qr.getDouble(2), qr.getDouble(3), qr.getDouble(4))
          val seq = row
            .toSeq
            .zipWithIndex
            .withFilter { case (_, i) => i != bv.value.qR }
            .map { case (f, _) => f }
          RTreeEntry(
            ext.xmin.toFloat,
            ext.ymin.toFloat,
            ext.xmax.toFloat,
            ext.ymax.toFloat,
            JoinQRInnerElem(ext, seq))
        }
      }
      .toIterable
    val rtree = RTree(iter)
    lhsIter
      //.toIterable
      //.par
      .flatMap { lhsRow =>
        val lhsQR = lhsRow.getStruct(bv.value.qL)
        val ext = new Envelope2D(lhsQR.getDouble(1), lhsQR.getDouble(2), lhsQR.getDouble(3), lhsQR.getDouble(4))
        rtree.searchAll(
            ext.xmin.toFloat,
            ext.ymin.toFloat,
            ext.xmax.toFloat,
            ext.ymax.toFloat)
          .withFilter(entry => QRIntersectObj.eval(ext, entry.value.ext, bv.value.cell, qrGroup))
          .map(entry => {
            val lhsRowSeq = lhsRow
              .toSeq
              .zipWithIndex
              .withFilter { case (_, i) => i != bv.value.qL }
              .map { case (f, _) => f }
            Row.fromSeq(lhsRowSeq ++ entry.value.seq)
          })
      }
    // .toIterator
  }

  private def operLocal(qrGroup: Long,
                        lhsIter: Iterator[Row],
                        rhsIter: Iterator[Row],
                        bv: Broadcast[JoinQRInnerArgs],
                        operLocal: OperatorSimpleRelation
                       ): Iterator[Row] = {
    val spRef = SpatialReferenceObj.create(bv.value.wkid)
    val accel = bv.value.acceleration.toLowerCase match {
      case "medium" =>
        GeometryAccelerationDegree.enumMedium
      case "hot" =>
        GeometryAccelerationDegree.enumHot
      case _ =>
        GeometryAccelerationDegree.enumMild
    }
    val iter = rhsIter
      .toIterable
      .par
      .map {
        row => {
          val geom = row.getAs[Array[Byte]](bv.value.geomR).geom
          operLocal.accelerateGeometry(geom, spRef, accel)
          val qr = row.getStruct(bv.value.qR)
          val ext = new Envelope2D(qr.getDouble(1), qr.getDouble(2), qr.getDouble(3), qr.getDouble(4))
          val seq = row
            .toSeq
            .zipWithIndex
            // Skip the qr field.
            .withFilter { case (_, i) => i != bv.value.qR }
            .map { case (f, _) => f }
          RTreeEntry(
            ext.xmin.toFloat,
            ext.ymin.toFloat,
            ext.xmax.toFloat,
            ext.ymax.toFloat,
            JoinQRInnerGeom(ext, seq, geom))
        }
      }
      .seq
    val rtree = RTree(iter)
    lhsIter
      .toIterable
      .par
      .flatMap { lhsRow =>
        val geom = lhsRow.getAs[Array[Byte]](bv.value.geomR).geom
        val lhsQR = lhsRow.getStruct(bv.value.qL)
        val ext = new Envelope2D(lhsQR.getDouble(1), lhsQR.getDouble(2), lhsQR.getDouble(3), lhsQR.getDouble(4))
        rtree.searchAll(
            ext.xmin.toFloat,
            ext.ymin.toFloat,
            ext.xmax.toFloat,
            ext.ymax.toFloat)
          .withFilter(entry => QRIntersectObj.eval(ext, entry.value.ext, bv.value.cell, qrGroup))
          .withFilter(entry => operLocal.execute(geom, entry.value.geom, spRef, null))
          .map(entry => {
            val lhsRowSeq = lhsRow
              .toSeq
              .zipWithIndex
              .withFilter { case (_, i) => i != bv.value.qL }
              .map { case (f, _) => f }
            Row.fromSeq(lhsRowSeq ++ entry.value.seq)
          })
      }
      .toIterator
  }
}
