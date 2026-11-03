package com.esri.spark

import com.esri.core.geometry.Operator
import org.apache.spark.sql.catalyst.expressions.Expression

final case class STDisjoint(children: Seq[Expression]) extends STRelation {

  override protected def opType: Operator.Type = Operator.Type.Disjoint

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren)
}
