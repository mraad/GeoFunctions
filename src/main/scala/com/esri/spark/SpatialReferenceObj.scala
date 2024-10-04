package com.esri.spark

import com.esri.core.geometry.{SpatialReference, VertexDescription}
import org.apache.spark.unsafe.types.UTF8String

import java.io.Serializable
import scala.collection.concurrent.TrieMap

object SpatialReferenceObj extends Serializable {
  final val sr3857 = SpatialReference.create(3857)
  final val sr4326 = SpatialReference.create(4326)
  final val sr102008 = SpatialReference.create(102008)
  private final val srMap: TrieMap[String, SpatialReference] = TrieMap.empty[String, SpatialReference] +=
    "4326" -> sr4326 +=
    "EPSG:4326" -> sr4326 +=
    "3857" -> sr3857 +=
    "EPSG:3857" -> sr3857 +=
    "102008" -> sr102008 +=
    "EPSG:102008" -> sr102008

  final def create(wkid: UTF8String): SpatialReference = {
    wkid.toString match {
      case "-1" =>
        null
      case text =>
        srMap.getOrElse(text, if (Character.isDigit(text(0))) SpatialReference.create(text.toInt) else SpatialReference.create(text))
    }
  }
}