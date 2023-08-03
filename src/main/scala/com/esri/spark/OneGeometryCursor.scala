package com.esri.spark

import com.esri.core.geometry.{Geometry, GeometryCursor}

final class OneGeometryCursor(geom: Geometry) extends GeometryCursor with Serializable {
  private var gid = -1

  override def next(): Geometry = if (gid == -1) {
    gid = 0
    geom
  } else {
    null
  }

  override def getGeometryID: Int = gid
}

object OneGeometryCursor extends Serializable {
  def apply(geom: Geometry) = new OneGeometryCursor(geom)
}
