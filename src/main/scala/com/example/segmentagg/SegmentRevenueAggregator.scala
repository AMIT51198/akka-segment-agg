package com.example.segmentagg

import scala.collection.mutable

final case class AggregateRow(segmentId: Long, totalImpressions: Long, totalRevenue: Double)

object SegmentRevenueAggregator {

  final case class AggregateState(var totalImpressions: Long, var totalRevenue: Double)

  final class PartialAggregate private[SegmentRevenueAggregator] (private val values: mutable.LongMap[AggregateState]) {
    def merge(other: PartialAggregate): PartialAggregate = {
      other.values.foreach { case (segmentId, incoming) =>
        values.get(segmentId) match {
          case Some(existing) =>
            existing.totalImpressions += incoming.totalImpressions
            existing.totalRevenue += incoming.totalRevenue
          case None =>
            values.put(segmentId, AggregateState(incoming.totalImpressions, incoming.totalRevenue))
        }
      }
      this
    }

    def rows: Vector[AggregateRow] = {
      values.iterator
        .map { case (segmentId, state) =>
          AggregateRow(segmentId, state.totalImpressions, state.totalRevenue)
        }
        .toVector
        .sortBy(_.segmentId)
    }

    def size: Int = values.size
  }

  object PartialAggregate {
    def empty: PartialAggregate = new PartialAggregate(mutable.LongMap.empty[AggregateState])
  }

  def aggregateBatch(batch: ColumnBatch): PartialAggregate = {
    val states = mutable.LongMap.empty[AggregateState]

    var i = 0
    while (i < batch.size) {
      val segmentId = batch.segmentIds(i)
      val impressions = batch.impressions(i)
      val revenue = batch.revenues(i)

      states.get(segmentId) match {
        case Some(existing) =>
          existing.totalImpressions += impressions
          existing.totalRevenue += revenue
        case None =>
          states.put(segmentId, AggregateState(impressions, revenue))
      }

      i += 1
    }

    new PartialAggregate(states)
  }
}
