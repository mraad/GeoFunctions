package com.esri.spark

import org.apache.commons.math3.util.FastMath

case class Rect(xmin: Double, ymin: Double, cell: Double) {
  private val xmax = xmin + cell
  private val ymax = ymin + cell

  @inline
  private final def outcode(px: Double, py: Double): Int = {
    var oc = 0
    if (px < xmin) oc |= 1
    if (px > xmax) oc |= 4
    if (py < ymin) oc |= 2
    if (py > ymax) oc |= 8
    oc
  }

  def clip(_x1: Double, _y1: Double, _x2: Double, _y2: Double): Double = {
    var clipped = true
    var x1 = _x1
    var y1 = _y1
    var x2 = _x2
    var y2 = _y2
    var f1 = outcode(x1, y1)
    var f2 = outcode(x2, y2)
    while (clipped && ((f1 | f2) != 0)) {
      if ((f1 & f2) != 0) {
        clipped = false
      }
      else {
        val dx = x2 - x1
        val dy = y2 - y1
        if (f1 != 0) {
          if ((f1 & 1) == 1 && dx != 0.0) {
            y1 = y1 + (xmin - x1) * dy / dx
            x1 = xmin
          } else if ((f1 & 4) == 4 && dx != 0.0) {
            y1 = y1 + (xmax - x1) * dy / dx
            x1 = xmax
          } else if ((f1 & 8) == 8 && dy != 0.0) {
            x1 = x1 + (ymax - y1) * dx / dy
            y1 = ymax
          } else if ((f1 & 2) == 2 && dy != 0.0) {
            x1 = x1 + (ymin - y1) * dx / dy
            y1 = ymin
          }
          f1 = outcode(x1, y1)
        } else if (f2 != 0) {
          if ((f2 & 1) == 1 && dx != 0.0) {
            y2 = y2 + (xmin - x2) * dy / dx
            x2 = xmin
          } else if ((f2 & 4) == 4 && dx != 0.0) {
            y2 = y2 + (xmax - x2) * dy / dx
            x2 = xmax
          } else if ((f2 & 8) == 8 && dy != 0.0) {
            x2 = x2 + (ymax - y2) * dx / dy
            y2 = ymax
          } else if ((f2 & 2) == 2 && dy != 0.0) {
            x2 = x2 + (ymin - y2) * dx / dy
            y2 = ymin
          }
          f2 = outcode(x2, y2)
        }
      }
    }
    if (clipped) {
      val dx = x2 - x1
      val dy = y2 - y1
      FastMath.sqrt(dx * dx + dy * dy)
    } else {
      0.0
    }
  }
}
