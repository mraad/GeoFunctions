package org.apache.spark.sql.esri

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.ExpressionUtils

/**
 * The Spark 4.0 half of the Column bridge. Spark 4.0 moved `Column` into `spark-sql-api`,
 * where it wraps a `ColumnNode` instead of a Catalyst `Expression`: the `Expression`
 * constructor and the `expr` accessor are both gone from the public type.
 *
 * `classic.ExpressionUtils` is the replacement, and every route to it is closed to an
 * outside package - `ExpressionUtils` is `private[spark]`, `object Column` is `private[spark]`
 * (so `ClassicConversions.ColumnConstructorExt` cannot be applied either), and
 * `ExpressionColumnNode` is `private[sql]`. Hence the package: this file lives inside
 * `org.apache.spark.sql` so those qualifiers admit it. `com.esri.spark` cannot see any of them.
 *
 * The Spark 3.x twin lives under `src/main/scala-2.12`; `build-helper-maven-plugin` puts
 * exactly one of the two on the compile source path, keyed off `${scala.compact}`.
 */
object ColumnCompat {
  def toColumn(e: Expression): Column = ExpressionUtils.column(e)

  /** Reinstates `column.expr`, which GeoFunctions calls on every wrapper argument. */
  implicit class ColumnExpr(private val c: Column) extends AnyVal {
    def expr: Expression = ExpressionUtils.expression(c)
  }
}
