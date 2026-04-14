package com.example.segmentagg

import scala.collection.mutable

import com.example.segmentagg.model.AggregateRow

/**
 * Stateless aggregation logic for segment revenue data.
 *
 * Aggregates [[ColumnBatch]] data into a [[PartialAggregate]] and merges partials.
 * The mutable [[AggregateState]] is confined to the aggregation scope and
 * never leaks outside [[PartialAggregate]].
 */
object SegmentRevenueAggregator {

  /**
   * Internal accumulator — mutable for performance, encapsulated
   * within [[PartialAggregate]] so external code never observes mutation.
   */
  private[segmentagg] final case class AggregateState(
      var totalImpressions: Long,
      var totalRevenue: Double
  ) {
    def mergeFrom(other: AggregateState): Unit = {
      totalImpressions += other.totalImpressions
      totalRevenue += other.totalRevenue
    }
  }

  /**
   * An encapsulated partial aggregation result that can be merged with others.
   *
   * The mutable internal map is never exposed; consumers obtain
   * an immutable [[Vector]] of [[AggregateRow]] via [[rows]].
   */
  final class PartialAggregate private[SegmentRevenueAggregator] (
      private val values: mutable.LongMap[AggregateState]
  ) {

    def merge(other: PartialAggregate): PartialAggregate = {
      other.values.foreach { case (segmentId, incoming) =>
        values.get(segmentId) match {
          case Some(existing) => existing.mergeFrom(incoming)
          case None           => values.put(segmentId, AggregateState(incoming.totalImpressions, incoming.totalRevenue))
        }
      }
      this
    }

    /** Materialise the aggregate as an immutable sorted vector. */
    def rows: Vector[AggregateRow] =
      values.iterator
        .map { case (segmentId, state) =>
          AggregateRow(segmentId, state.totalImpressions, state.totalRevenue)
        }
        .toVector
        .sortBy(_.segmentId)

    def size: Int = values.size
  }

  object PartialAggregate {
    def empty: PartialAggregate = new PartialAggregate(mutable.LongMap.empty[AggregateState])
  }

  /** Aggregate a single columnar batch into a partial result. */
  def aggregateBatch(batch: ColumnBatch): PartialAggregate = {
    val states = mutable.LongMap.empty[AggregateState]
    var i = 0
    while (i < batch.size) {
      val segmentId   = batch.segmentIds(i)
      val impressions = batch.impressions(i)
      val revenue     = batch.revenues(i)

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

