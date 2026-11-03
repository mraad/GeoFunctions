package com.esri.spark

import com.esri.core.geometry.{Envelope2D, Geometry, OperatorDistance}
import com.github.plokhotnyuk.rtree2d.core.EuclideanPlane._
import com.github.plokhotnyuk.rtree2d.core._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{BinaryType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

/**
 * k-nearest-neighbour join: for every lhs row, the k rhs rows whose geometries are nearest
 * (planar distance), one output row per pair with the distance appended.
 *
 * The rhs side must be small enough to collect: it is broadcast, and each task indexes it in an
 * R-tree. rtree2d stores floats, so two things keep the answer exact: each entry box is rounded
 * outward (it always contains the true envelope), and the search radius handed back to the tree
 * is the k-th exact (double) distance plus a slack that covers float rounding of the query point.
 * The tree can therefore only prune entries that are truly farther; the final k come from exact
 * Esri distances. Ties at the k-th distance keep the rhs row that comes first.
 */
object KnnJoinProcessor extends Serializable {

  private final case class Entry(index: Int, geom: Geometry)

  /** Box that contains [xmin, xmax] x [ymin, ymax] after the conversion to float. */
  private def outward(e: Envelope2D): (Float, Float, Float, Float) = {
    def down(v: Double): Float = { val f = v.toFloat; if (f > v) Math.nextDown(f) else f }
    def up(v: Double): Float = { val f = v.toFloat; if (f < v) Math.nextUp(f) else f }
    (down(e.xmin), down(e.ymin), up(e.xmax), up(e.ymax))
  }

  /** Distance error from rounding the query point and node boxes to float. */
  private def slack(x: Double, y: Double): Float =
    (1e-5 * (1.0 + math.max(math.abs(x), math.abs(y)))).toFloat

  private[spark] final class Index(rhs: Array[Array[Byte]]) {
    private var indexed = 0
    private val tree: RTree[Entry] = {
      val entries = rhs.indices.flatMap { i =>
        val g = rhs(i).geom
        val env = new Envelope2D()
        g.queryEnvelope2D(env)
        if (env.isEmpty) None
        else {
          val (x0, y0, x1, y1) = outward(env)
          Some(RTreeEntry(x0, y0, x1, y1, Entry(i, g)))
        }
      }
      indexed = entries.size
      RTree(entries)
    }

    /**
     * (rhs index, distance) of the k nearest, nearest first; fewer when rhs has fewer. The tree
     * measures from the lhs envelope centre; a lhs geometry with an extent (line, polygon) can be
     * up to half its envelope diagonal closer to an entry than the centre is, so that half
     * diagonal joins the slack (it is 0 for a point).
     */
    def nearest(geom: Geometry, kRequested: Int): Array[(Int, Double)] = {
      val env = new Envelope2D()
      geom.queryEnvelope2D(env)
      if (env.isEmpty || tree.isEmpty) return Array.empty
      val k = math.min(kRequested, indexed)
      val x = env.getCenterX
      val y = env.getCenterY
      val pad = slack(x, y) + (0.5 * math.hypot(env.getWidth, env.getHeight)).toFloat
      // max-heap on (distance, index): the root is the current k-th best
      val heap = new java.util.PriorityQueue[(Double, Int)](k + 1, (a: (Double, Int), b: (Double, Int)) => {
        val c = java.lang.Double.compare(b._1, a._1)
        if (c != 0) c else Integer.compare(b._2, a._2)
      })
      def offer(e: Entry): Unit = {
        val d = OperatorDistance.local.execute(geom, e.geom, null)
        val top = heap.peek()
        if (heap.size < k) heap.add((d, e.index))
        else if (d < top._1 || (d == top._1 && e.index < top._2)) { heap.poll(); heap.add((d, e.index)) }
      }
      def bound: Float = if (heap.size < k) Float.PositiveInfinity else heap.peek()._1.toFloat + pad
      // Seed with the k nearest by box distance (no Esri call): k real candidates, so their k-th
      // exact distance bounds the answer and the exact pass below visits only entries inside it.
      val seed = tree.nearestK(x.toFloat, y.toFloat, k).map(_.value)
      seed.foreach(offer)
      val seeded = seed.map(_.index).toSet
      tree.nearest(x.toFloat, y.toFloat, bound)((_: Float, e: RTreeEntry[Entry]) => {
        if (!seeded.contains(e.value.index)) offer(e.value)
        bound
      })
      heap.toArray(new Array[(Double, Int)](0)).sortBy(p => (p._1, p._2)).map(p => (p._2, p._1))
    }
  }

  private def fieldIndex(schema: StructType, name: String, side: String): Int = {
    val i = schema.fieldNames.indexOf(name)
    require(i >= 0, s"Missing `$name` field in $side dataframe.")
    require(schema(i).dataType == BinaryType, s"`$name` in $side dataframe must be binary, got ${schema(i).dataType.sql}")
    i
  }

  final def apply(lhs: Dataset[Row],
                  rhs: Dataset[Row],
                  k: Int,
                  lhsGeom: String = "geom",
                  rhsGeom: String = "geom",
                  distance: String = "distance",
                 ): Dataset[Row] = {
    require(k > 0, s"k must be positive, got $k")
    val gl = fieldIndex(lhs.schema, lhsGeom, "LHS")
    val gr = fieldIndex(rhs.schema, rhsGeom, "RHS")
    val rhsRows = rhs.collect().filter(r => !r.isNullAt(gr))
    val sc = lhs.sparkSession.sparkContext
    val rhsValues = sc.broadcast(rhsRows.map(r => r.toSeq.toArray))
    val rhsGeoms = sc.broadcast(rhsRows.map(_.getAs[Array[Byte]](gr)))
    val schema = StructType(lhs.schema.fields ++ rhs.schema.fields :+ StructField(distance, DoubleType, nullable = false))
    lhs.mapPartitions { rows =>
      lazy val index = new Index(rhsGeoms.value)
      val values = rhsValues.value
      rows.flatMap { row =>
        if (row.isNullAt(gl)) Iterator.empty
        else {
          val left = row.toSeq.toArray
          index.nearest(row.getAs[Array[Byte]](gl).geom, k).iterator.map { case (i, d) =>
            new GenericRow((left ++ values(i)) :+ d).asInstanceOf[Row]
          }
        }
      }
    }(ExpressionEncoder(schema))
  }
}
