package com.esri.spark

import com.esri.core.geometry.Geometry.GeometryAccelerationDegree
import com.esri.core.geometry.{Envelope2D, Geometry, OperatorContains, OperatorCrosses, OperatorEquals, OperatorIntersects, OperatorOverlaps, OperatorSimpleRelation, OperatorTouches, OperatorWithin}
import com.github.plokhotnyuk.rtree2d.core._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{BinaryType, DataType, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.LoggerFactory

import java.util.Locale


final case class JoinQRInnerArgs(cell: Double,
                                 qL: Int,
                                 qR: Int,
                                 geomL: Int = -1,
                                 geomR: Int = -1,
                                 oper: String = "none",
                                 wkid: String = "-1",
                                 acceleration: String = "mild",
                                 parallelThreshold: Int = 64,
                                )

final case class JoinQRInnerElem(ext: Envelope2D,
                                 values: Array[Any],
                                )

final case class JoinQRInnerGeom(ext: Envelope2D,
                                 values: Array[Any],
                                 geom: Geometry,
                                )

object JoinQRInnerProcessor extends Serializable {

  private val supportedOperations = Set(
    "none", "contains", "crosses", "equals", "intersects", "overlaps", "touches", "within")
  private val supportedAcceleration = Set("mild", "medium", "hot")

  private[spark] def normalizeOperation(operation: String): String = {
    val normalized = Option(operation).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("") match {
      case "intersection" => "intersects"
      case value => value
    }
    require(normalized != "disjoint",
      "operation 'disjoint' is not supported by a QR overlap join")
    require(supportedOperations.contains(normalized),
      s"unsupported spatial operation '$normalized'; expected one of ${supportedOperations.toSeq.sorted.mkString(", ")}")
    normalized
  }

  private[spark] def normalizeAcceleration(acceleration: String): String = {
    val normalized = Option(acceleration).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
    require(supportedAcceleration.contains(normalized),
      s"unsupported acceleration '$normalized'; expected one of ${supportedAcceleration.toSeq.sorted.mkString(", ")}")
    normalized
  }

  private def requiredFieldIndex(schema: StructType, name: String, side: String): Int = {
    require(name != null && name.nonEmpty, s"$side field name must be non-empty")
    val indices = schema.fields.indices.filter(index => schema(index).name == name)
    require(indices.nonEmpty, s"Missing `$name` field in $side dataframe.")
    require(indices.length == 1, s"Ambiguous `$name` field in $side dataframe.")
    indices.head
  }

  private def requireFieldType(schema: StructType,
                               index: Int,
                               expected: DataType,
                               side: String
                              ): Unit = {
    val field = schema(index)
    require(field.dataType.catalogString == expected.catalogString,
      s"`${field.name}` in $side dataframe must be ${expected.sql}, got ${field.dataType.sql}")
  }

  /** Drop rows that cannot participate in this null-intolerant join. */
  private def quoted(name: String): String =
    s"`${name.replace("`", "``")}`"

  private def usableRows(df: Dataset[Row],
                         qrField: String,
                         geomField: Option[String]
                        ): Dataset[Row] = {
    val qr = df.col(quoted(qrField))
    val qrFields = Seq("qr", "xmin", "ymin", "xmax", "ymax")
    val qrValid = qrFields
      .map(name => qr.getField(name).isNotNull)
      .foldLeft(qr.isNotNull)(_ && _)
    geomField match {
      case Some(name) => df.filter(qrValid && df.col(quoted(name)).isNotNull)
      case None => df.filter(qrValid)
    }
  }

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
    QRIntersectObj.validateCell(cell)
    val operation = normalizeOperation(oper)
    val accelerationMode = normalizeAcceleration(acceleration)
    lazy val logger = LoggerFactory.getLogger(getClass)

    val spark = lhs.sparkSession // SparkSession.builder().getOrCreate()
    require(spark.sparkContext == rhs.sparkSession.sparkContext,
      "LHS and RHS dataframes must use the same SparkContext")
    import spark.implicits._
    val qrL = requiredFieldIndex(lhs.schema, qrField, "LHS")
    val qrR = requiredFieldIndex(rhs.schema, qrField, "RHS")
    requireFieldType(lhs.schema, qrL, QREnvpObj.dataType, "LHS")
    requireFieldType(rhs.schema, qrR, QREnvpObj.dataType, "RHS")

    val geomL = operation match {
      case "none" => -1
      case _ => requiredFieldIndex(lhs.schema, lhsGeom, "LHS")
    }
    val geomR = operation match {
      case "none" => -1
      case _ => requiredFieldIndex(rhs.schema, rhsGeom, "RHS")
    }
    val spatialReference = Option(wkid).map(_.trim).getOrElse("")
    if (operation != "none") {
      requireFieldType(lhs.schema, geomL, BinaryType, "LHS")
      requireFieldType(rhs.schema, geomR, BinaryType, "RHS")
      require(spatialReference.nonEmpty, "wkid must be non-empty for an exact spatial operation")
      // Validate once on the driver instead of failing an executor after the shuffle.
      SpatialReferenceObj.create(spatialReference)
    }

    // This tiny immutable value belongs in the serialized closure. Broadcasting it creates
    // lifecycle overhead and an executor lookup in every row/group without reducing payload.
    val parallelThreshold = math.max(
      1, spark.sparkContext.getConf.getInt("spark.esri.parallel", 64))
    val args = JoinQRInnerArgs(
      cell, qrL, qrR, geomL, geomR, operation, spatialReference,
      accelerationMode, parallelThreshold)

    // Define resulting schema.
    val lhsFields = lhs.schema.fields.zipWithIndex.collect { case (field, index) if index != qrL => field }
    val rhsFields = rhs.schema.fields.zipWithIndex.collect { case (field, index) if index != qrR => field }
    val schema = StructType(lhsFields ++ rhsFields)

    val geomFieldL = if (geomL < 0) None else Some(lhsGeom)
    val geomFieldR = if (geomR < 0) None else Some(rhsGeom)
    val lhsGroup = usableRows(lhs, qrField, geomFieldL)
      .groupByKey(row => {
        row.getStruct(args.qL).getLong(0)
      })

    val rhsGroup = usableRows(rhs, qrField, geomFieldR)
      .groupByKey(row => {
        row.getStruct(args.qR).getLong(0)
      })

    lhsGroup
      .cogroup(rhsGroup)((qrGroup, lhsIter, rhsIter) => {
        try {
          if (lhsIter.isEmpty || rhsIter.isEmpty) {
            Iterator.empty
          } else {
            args.oper match {
              case "contains" =>
                operLocal(qrGroup, lhsIter, rhsIter, args, OperatorContains.local)
              case "crosses" =>
                operLocal(qrGroup, lhsIter, rhsIter, args, OperatorCrosses.local)
              case "equals" =>
                operLocal(qrGroup, lhsIter, rhsIter, args, OperatorEquals.local)
              case "intersects" =>
                operLocal(qrGroup, lhsIter, rhsIter, args, OperatorIntersects.local)
              case "overlaps" =>
                operLocal(qrGroup, lhsIter, rhsIter, args, OperatorOverlaps.local)
              case "touches" =>
                operLocal(qrGroup, lhsIter, rhsIter, args, OperatorTouches.local)
              case "within" =>
                operLocal(qrGroup, lhsIter, rhsIter, args, OperatorWithin.local)
              case "none" =>
                operNone(qrGroup, lhsIter, rhsIter, args)
              case value =>
                throw new IllegalStateException(s"unexpected spatial operation '$value'")
            }
          }
        } catch {
          // Do not swallow: returning an empty iterator turns a failed partition into a
          // silently short join result that looks like a successful run.
          case t: Throwable =>
            logger.error(s"QR join failed for group $qrGroup: ${t.getMessage}", t)
            throw t
        }
      })(ExpressionEncoder(schema))
  }

  /** Row fields with the QR field dropped, copied exactly once. */
  @inline
  private def dropField(row: Row, index: Int): Array[Any] = {
    val values = new Array[Any](row.length - 1)
    var source = 0
    var target = 0
    while (source < row.length) {
      if (source != index) {
        values(target) = row.get(source)
        target += 1
      }
      source += 1
    }
    values
  }

  @inline
  private def joinedRow(lhs: Array[Any], rhs: Array[Any]): Row = {
    val values = new Array[Any](lhs.length + rhs.length)
    System.arraycopy(lhs, 0, values, 0, lhs.length)
    System.arraycopy(rhs, 0, values, lhs.length, rhs.length)
    new GenericRow(values)
  }

  @inline
  private def envelope(row: Row, qrIndex: Int): Envelope2D = {
    val qr = row.getStruct(qrIndex)
    new Envelope2D(qr.getDouble(1), qr.getDouble(2), qr.getDouble(3), qr.getDouble(4))
  }

  private def operNone(qrGroup: Long,
                       lhsIter: Iterator[Row],
                       rhsIter: Iterator[Row],
                       args: JoinQRInnerArgs
                      ): Iterator[Row] = {
    val entries = rhsIter
      .map { row =>
        val ext = envelope(row, args.qR)
        RTreeEntry(
          ext.xmin.toFloat,
          ext.ymin.toFloat,
          ext.xmax.toFloat,
          ext.ymax.toFloat,
          JoinQRInnerElem(ext, dropField(row, args.qR)))
      }
      .toVector
    val rtree = RTree(entries)
    lhsIter
      .flatMap { lhsRow =>
        val ext = envelope(lhsRow, args.qL)
        // Hoisted: this is per lhs row, not per matched pair.
        val lhsValues = dropField(lhsRow, args.qL)
        rtree.searchAll(
            ext.xmin.toFloat,
            ext.ymin.toFloat,
            ext.xmax.toFloat,
            ext.ymax.toFloat)
          .withFilter(entry => QRIntersectObj.evalUnchecked(ext, entry.value.ext, args.cell, qrGroup))
          .map(entry => joinedRow(lhsValues, entry.value.values))
      }
  }

  private def operLocal(qrGroup: Long,
                        lhsIter: Iterator[Row],
                        rhsIter: Iterator[Row],
                        args: JoinQRInnerArgs,
                        operator: OperatorSimpleRelation
                       ): Iterator[Row] = {
    val spRef = SpatialReferenceObj.create(args.wkid)
    val accel = args.acceleration match {
      case "medium" =>
        GeometryAccelerationDegree.enumMedium
      case "hot" =>
        GeometryAccelerationDegree.enumHot
      case _ =>
        GeometryAccelerationDegree.enumMild
    }
    val rhsRows = rhsIter.toVector
    def rhsEntry(row: Row): RTreeEntry[JoinQRInnerGeom] = {
      val geom = row.getAs[Array[Byte]](args.geomR).geom
      operator.accelerateGeometry(geom, spRef, accel)
      val ext = envelope(row, args.qR)
      RTreeEntry(
        ext.xmin.toFloat,
        ext.ymin.toFloat,
        ext.xmax.toFloat,
        ext.ymax.toFloat,
        JoinQRInnerGeom(ext, dropField(row, args.qR), geom))
    }
    // Java parallel streams rather than `.par` throughout: on 2.13 `.par` needs an import
    // that does not exist on 2.12, and this source tree cross-builds both. Every index is
    // written once by one thread, so the shared arrays need no synchronization.
    val entries =
      if (rhsRows.length > args.parallelThreshold) {
        val scratch = new Array[RTreeEntry[JoinQRInnerGeom]](rhsRows.length)
        java.util.stream.IntStream.range(0, rhsRows.length).parallel().forEach((i: Int) =>
          scratch(i) = rhsEntry(rhsRows(i))
        )
        scratch.toSeq
      }
      else rhsRows.map(rhsEntry)
    val rtree = RTree(entries)

    def matches(lhsRow: Row): Iterator[Row] = {
      val ext = envelope(lhsRow, args.qL)
      // Hoisted: this is per lhs row, not per matched pair.
      val lhsValues = dropField(lhsRow, args.qL)
      val candidates = rtree.searchAll(
          ext.xmin.toFloat,
          ext.ymin.toFloat,
          ext.xmax.toFloat,
          ext.ymax.toFloat)
        .filter(entry => QRIntersectObj.evalUnchecked(ext, entry.value.ext, args.cell, qrGroup))
      if (candidates.isEmpty) {
        Iterator.empty
      } else {
        val geom = lhsRow.getAs[Array[Byte]](args.geomL).geom
        // Relation accelerators may be consumed from either operand. Build the LHS
        // accelerator lazily so rows rejected by the R-tree/canonical filter pay no cost.
        operator.accelerateGeometry(geom, spRef, accel)
        candidates.iterator
          .withFilter(entry => operator.execute(geom, entry.value.geom, spRef, null))
          .map(entry => joinedRow(lhsValues, entry.value.values))
      }
    }

    val lhsRows = lhsIter.toVector
    if (lhsRows.length > args.parallelThreshold) {
      val scratch = new Array[Vector[Row]](lhsRows.length)
      java.util.stream.IntStream.range(0, lhsRows.length).parallel().forEach((i: Int) =>
        scratch(i) = matches(lhsRows(i)).toVector
      )
      scratch.iterator.flatten
    } else {
      lhsRows.iterator.flatMap(matches)
    }
  }
}
