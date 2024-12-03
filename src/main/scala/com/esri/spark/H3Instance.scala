package com.esri.spark

import com.esri.core.geometry.Polygon
import com.uber.h3core.H3Core
import org.apache.spark.sql.catalyst.InternalRow

import scala.jdk.CollectionConverters._

object H3Instance extends Serializable {
  @transient lazy final val h3: H3Core = H3Core.newInstance()

  final def cellToLatLng(l: Long): InternalRow = {
    val coord = h3.cellToLatLng(l)
    InternalRow(coord.lat, coord.lng)
  }

  def cellToBoundary(l: Long): Array[Byte] = {
    val polygon = new Polygon()
    h3
      .cellToBoundary(l)
      .asScala.zipWithIndex.foreach {
        case (coord, i) => i match {
          case 0 => polygon.startPath(coord.lng, coord.lat)
          case _ => polygon.lineTo(coord.lng, coord.lat)
        }
      }
    polygon.bytes
  }

}
