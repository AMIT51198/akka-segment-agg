package com.example.segmentagg.model

/**
 * A fully aggregated output row: one entry per distinct `segment_id`
 * with the summed impressions and revenue across all input events.
 */
final case class AggregateRow(
    segmentId: Long,
    totalImpressions: Long,
    totalRevenue: Double
)

