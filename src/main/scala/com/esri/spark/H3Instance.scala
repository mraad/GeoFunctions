package com.esri.spark

import com.uber.h3core.H3Core

/** Holds the one H3Core per JVM. Loading it pulls in a native library, so never per row. */
object H3Instance extends Serializable {
  @transient lazy final val h3: H3Core = H3Core.newInstance()
}
