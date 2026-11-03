package com.esri.spark

import com.esri.core.geometry.Operator
import org.apache.spark.sql.catalyst.expressions.Expression

final case class STTouches(children: Seq[Expression]) extends STRelation {

  override protected def opType: Operator.Type = Operator.Type.Touches

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
