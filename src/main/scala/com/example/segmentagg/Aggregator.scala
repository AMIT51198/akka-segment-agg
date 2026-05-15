package com.example.segmentagg

import scala.collection.mutable

/** Stateless aggregation: ColumnBatch → PartialAggregate, with merge support. */
object Aggregator {

  private[segmentagg] final case class AccState(var impressions: Long, var revenue: Double)

  final class PartialAggregate private[Aggregator] (
      private val map: mutable.LongMap[AccState]
  ) {
    def merge(other: PartialAggregate): PartialAggregate = {
      other.map.foreach { case (id, incoming) =>
        map.get(id) match {
          case Some(existing) =>
            existing.impressions += incoming.impressions
            existing.revenue += incoming.revenue
          case None =>
            map.put(id, AccState(incoming.impressions, incoming.revenue))
        }
      }
      this
    }

    def toRows: Vector[AggregateRow] =
      map.iterator
        .map { case (id, s) => AggregateRow(id, s.impressions, s.revenue) }
        .toVector
        .sortBy(_.segmentId)

    def size: Int = map.size
  }

  def emptyAggregate: PartialAggregate = new PartialAggregate(mutable.LongMap.empty)

  def aggregateBatch(batch: ColumnBatch): PartialAggregate = {
    val map = mutable.LongMap.empty[AccState]
    var i = 0
    while (i < batch.size) {
      val id  = batch.segmentIds(i)
      val imp = batch.impressions(i)
      val rev = batch.revenues(i)
      map.get(id) match {
        case Some(s) => s.impressions += imp; s.revenue += rev
        case None    => map.put(id, AccState(imp, rev))
      }
      i += 1
    }
    new PartialAggregate(map)
  }
}

