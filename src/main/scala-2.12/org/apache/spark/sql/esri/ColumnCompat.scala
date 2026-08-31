package org.apache.spark.sql.esri

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * The Spark 3.x half of the Column bridge. Spark 3.x `Column` wraps a Catalyst `Expression`
 * directly, so the constructor takes one and `expr` hands it back - only the construction
 * side needs a name here, and `expr` resolves natively.
 *
 * See the Spark 4.0 twin under `src/main/scala-2.13` for why this file is split by version
 * and why it sits in `org.apache.spark.sql` rather than `com.esri.spark`.
 * `build-helper-maven-plugin` puts exactly one of the two on the compile source path,
 * keyed off `${scala.compact}`.
 */
object ColumnCompat {
  def toColumn(e: Expression): Column = new Column(e)
}
