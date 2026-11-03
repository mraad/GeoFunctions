package com.esri.spark

import com.esri.core.geometry.Operator
import org.apache.spark.sql.catalyst.expressions.Expression

final case class STIntersects(children: Seq[Expression]) extends STRelation {

  override protected def opType: Operator.Type = Operator.Type.Intersects

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
