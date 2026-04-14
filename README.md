# Akka Segment Aggregation

A single-node streaming pipeline that executes the following aggregation over a large Parquet dataset:

```sql
SELECT
    segment_id,
    SUM(impressions) AS total_impressions,
    SUM(revenue)     AS total_revenue
FROM fact_events
GROUP BY segment_id;
```

The pipeline reads columnar Parquet data, decodes only the three required columns, partially aggregates each row group in parallel, and merges the results into a final grouped output written to CSV.

---

## Generating the Input Data

Use [DuckDB](https://duckdb.org/) to generate a synthetic `fact_events` table and export it to Parquet.

```sql
-- Generate 500M rows with high-cardinality, hash-distributed values.
-- segment_id:  up to 10M unique values, randomly distributed (no sequential pattern).
-- impressions: random integer 1–1000 (defeats RLE/dictionary encoding).
-- revenue:     near-unique double (full floating-point entropy, defeats compression).
CREATE TABLE fact_events AS
SELECT
    ABS(hash(i)     % 10000000)                                          AS segment_id,
    (ABS(hash(i+1)  % 1000) + 1)                                        AS impressions,
    ABS(hash(i+2)   % 999999) * 0.00000100001
        + ABS(hash(i+3) % 9973) * 0.001                                 AS revenue
FROM range(500000000) t(i);

-- Export to partitioned Parquet files.
COPY fact_events
TO '/path/to/fact_events_parquet'
(FORMAT parquet, ROW_GROUP_SIZE 65536);
```

> The data is deliberately designed to be hard to compress: random segment IDs break sort-based
> optimisations, a wide impression range defeats RLE, and near-unique doubles defeat dictionary encoding.
> This gives a realistic worst-case benchmark for the aggregation pipeline.

---

## Pipeline Architecture

```
Parquet directory
       │
       ▼
ParquetBatchSource          (readParallelism=N concurrent file readers)
       │  one ColumnBatch per row group
       ▼
filter(non-empty batches)
       │
       ▼
mapAsyncUnordered           (aggregateParallelism=M concurrent workers)
       │  each worker: decode columns → PartialAggregate (LongMap)
       ▼
Sink.fold                   (single thread, sequential merge)
       │  merges all PartialAggregates into one
       ▼
reportResults               (materialise rows, log timing & latency metrics)
       │
       ▼
ResultWriter                (CsvResultWriter or NoOpResultWriter for benchmarks)
```

### Key components

| Component | Package | Role |
|---|---|---|
| `ParquetBatchSource` | `segmentagg` | Resolves Parquet files, emits one `ColumnBatch` per row group |
| `ColumnBatch` | `segmentagg` | Holds decoded column arrays (`segmentIds`, `impressions`, `revenues`) + timing |
| `SegmentRevenueAggregator` | `segmentagg` | Aggregates a `ColumnBatch` into a `PartialAggregate`; merges partials |
| `AggregationPipeline` | `segmentagg` | Wires the Akka Streams topology; owns the `ActorSystem` lifecycle |
| `DecodeMode` | `parquet` | Chooses between scalar (row-at-a-time) and vectorised (batch-at-a-time) decoding |
| `CsvResultWriter` | `io` | Writes the final `Vector[AggregateRow]` to a CSV file |
| `NoOpResultWriter` | `io` | Discards output — used during benchmarking to avoid I/O skewing timing |
| `StageMetrics` | `metrics` | Per-stage latency (fetch, decode, aggregate, wait) with p50/p95/max |
| `DecoderPageMetrics` | `parquet` | Per-column page-level decode latency |
| `PipelineLogger` | `logging` | Structured logger abstraction (console implementation provided) |

### Decode modes

| Mode | How it works |
|---|---|
| `scalar` | Decodes one value at a time in a `while` loop |
| `vectorised` | Decodes an entire page into a pre-allocated array in one pass; avoids per-row branching |

Vectorised is consistently ~25–30% faster on high-entropy data (see benchmark results below).

### Parallelism

Two independent knobs control throughput:

- `--read-parallelism` — number of Parquet files read concurrently (`flatMapMerge` breadth)
- `--aggregate-parallelism` — number of `PartialAggregate` workers running concurrently (`mapAsyncUnordered`)

Aggregate parallelism should always be ≤ read parallelism. Increasing aggregate beyond read adds no gain because read is the bottleneck.

---

## Project Structure

```
src/main/scala/com/example/segmentagg/
├── AggregationPipeline.scala       # stream topology + lifecycle
├── Benchmark.scala                 # parallelism grid-search benchmark
├── ColumnBatch.scala               # columnar batch model
├── BatchTiming.scala               # per-batch timing counters
├── Main.scala                      # CLI entry point
├── ParquetBatchSource.scala        # Parquet → ColumnBatch source
├── SegmentRevenueAggregator.scala  # partial aggregation + merge
├── io/
│   ├── ResultWriter.scala          # output writer trait
│   ├── CsvResultWriter.scala       # CSV implementation
│   └── NoOpResultWriter.scala      # no-op for benchmarks
├── logging/
│   └── PipelineLogger.scala        # logger abstraction
├── metrics/
│   ├── LatencyDistribution.scala   # histogram + formatting
│   └── StageMetrics.scala          # named per-stage collectors
├── model/
│   ├── AggregateRow.scala          # output row model
│   └── PipelineConfig.scala        # immutable pipeline config
└── parquet/
    ├── ColumnChunkDecoder.scala         # scalar decoder
    ├── VectorisedColumnChunkDecoder.scala
    ├── VectorisedDecoders.scala
    ├── DecodeMode.scala
    ├── DecoderPageMetrics.scala
    ├── RequiredColumnDecoders.scala
    └── ...
```

---

## Run

```bash
sbt "run \
  --input  /path/to/fact_events_parquet \
  --output /path/to/results.csv \
  --read-parallelism 10 \
  --aggregate-parallelism 10 \
  --decode-mode vectorised"
```

**CLI flags**

| Flag | Default | Description |
|---|---|---|
| `--input` | required | Path to a Parquet file or directory |
| `--output` | required | Path for the CSV output file |
| `--read-parallelism` | `nCPU` | Concurrent Parquet file readers |
| `--aggregate-parallelism` | `nCPU` | Concurrent partial-aggregate workers (≤ read-parallelism) |
| `--decode-mode` | `scalar` | `scalar` or `vectorised` |
| `--parallelism` | `nCPU` | Sets both read and aggregate parallelism in one flag |

---

## Benchmark

Runs all combinations of `read-parallelism × aggregate-parallelism` with vectorised decoding and writes results to `/tmp/benchmark_results.csv`.

```bash
sbt "runMain com.example.segmentagg.Benchmark /path/to/fact_events_parquet"
```

Grid searched: `read ∈ {4,6,8,10,12,16}`, `agg ∈ {4,6,8,10,12}` where `agg ≤ read`.

---

## Test

```bash
sbt test
```

Tests cover: aggregation correctness, merge commutativity, empty batch handling, high-parallelism stress, determinism across repeated runs, CSV writer, latency metrics, decode modes.

---

## Notes

- Single-node only — not a distributed system.
- The final `GROUP BY` state grows with the number of distinct `segment_id` values (unavoidable for exact aggregation).
- Back-pressure is enforced with `inputBuffer(1, 1)` so the source does not read ahead and blow up heap.
- The `ActorSystem` is created and torn down per pipeline run; it is not shared across benchmark iterations.
