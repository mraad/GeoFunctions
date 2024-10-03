package com.esri.spark

import com.esri.core.geometry.{Geometry, GeometryCursor}
import org.apache.spark.sql.catalyst.util.ArrayData

final class ArrayDataGeometryCursor(arr: ArrayData) extends GeometryCursor {
  private val _elemNum = arr.numElements()
  private var _elemIdx = 0

  override def next(): Geometry = {
    if (_elemIdx < _elemNum) {
      val geom = arr.getBinary(_elemIdx).geom
      _elemIdx += 1
      geom
    } else {
      null
    }
  }

  override def getGeometryID: Int = _elemIdx - 1
}

object ArrayDataGeometryCursor extends Serializable {
  def apply(arr: ArrayData): ArrayDataGeometryCursor = new ArrayDataGeometryCursor(arr)
}
