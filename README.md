# Akka Segment Aggregation

High-performance streaming pipeline that processes Parquet data and executes:

```sql
SELECT segment_id, SUM(impressions) AS total_impressions, SUM(revenue) AS total_revenue
FROM fact_events
GROUP BY segment_id;
```

**Performance**: 500M rows in ~54 seconds on 12-core system (read=12, agg=12)

## Quick Start

```bash
# Compile
sbt clean compile

# Run
sbt "run --input /path/to/parquet --output /path/to/results.csv --read-parallelism 8 --aggregate-parallelism 8"

# Test
sbt test
```

## Pipeline Architecture

```
Parquet Files (16 files, 7631 row groups)
     ↓
Read Stage (N threads) → ParquetBatchSource
     ↓
Decode Stage (M threads) → ColumnBatch (segment_ids[], impressions[], revenues[])
     ↓
Aggregate Stage (M threads) → PartialAggregate (LongMap)
     ↓
Merge Stage (1 thread) → Final result
     ↓
Output CSV (segment_id, total_impressions, total_revenue)
```

## Parallelism

- `--read-parallelism N` — Number of concurrent file readers (max 16 for 16 files)
- `--aggregate-parallelism M` — Number of concurrent aggregate workers (should be ≤ read-parallelism)

For 12-core system: use `--read-parallelism 12 --aggregate-parallelism 12`

## Generate Test Data (500M rows)

Using [DuckDB](https://duckdb.org/):

```bash
duckdb << 'EOF'
CREATE TABLE fact_events AS
SELECT
    ABS(hash(i) % 10000000) AS segment_id,
    (ABS(hash(i + 1) % 1000) + 1) AS impressions,
    ABS(hash(i + 2) % 999999) * 0.00000100001 + ABS(hash(i + 3) % 9973) * 0.001 AS revenue
FROM range(500000000) t(i);

COPY fact_events TO '/path/to/fact_events_parquet' (FORMAT parquet, ROW_GROUP_SIZE 65536);
EOF
```

Output: 16 Parquet files (~650MB each)

## CLI Options

```
--input <path>                Path to Parquet file or directory (required)
--output <path>               Path for CSV output file (required)
--read-parallelism <n>        Concurrent file readers (default: nCPU)
--aggregate-parallelism <n>   Concurrent aggregate workers (default: nCPU)
--parallelism <n>             Set both read and aggregate (default: nCPU)
```

## Output Format

CSV with 10M unique segment IDs:
```csv
segment_id,total_impressions,total_revenue
1,102345,123.45
2,98765,98.76
...
```

## Project Structure

```
src/main/scala/com/example/segmentagg/
├── AggregationPipeline.scala       # Main pipeline orchestrator
├── ParquetBatchSource.scala        # Reads Parquet files
├── SegmentRevenueAggregator.scala  # Partial aggregation + merge
├── ColumnBatch.scala               # Columnar data model
├── Main.scala                      # CLI entry point
├── model/
│   ├── PipelineConfig.scala        # Immutable config
│   └── AggregateRow.scala          # Output row
├── io/
│   ├── ResultWriter.scala          # Output abstraction
│   ├── CsvResultWriter.scala       # CSV implementation
│   └── NoOpResultWriter.scala      # No-op for benchmarks
├── metrics/
│   ├── StageMetrics.scala          # Per-stage latency
│   └── LatencyDistribution.scala   # Histogram + formatting
├── logging/
│   └── PipelineLogger.scala        # Logger abstraction
├── parquet/
│   ├── DecodeMode.scala            # Scalar vs Vectorised
│   ├── ColumnChunkDecoder.scala    # Scalar decoder
│   ├── VectorisedColumnChunkDecoder.scala
│   └── ...
└── util/
    ├── Benchmark.scala             # Performance grid search
    └── ParquetFooterInspector.scala # Parquet metadata

src/test/scala/...                 # Test suite
src/main/resources/
├── logback.xml                    # Logging configuration
└── application.conf               # Akka configuration
```

## Configuration

### JVM Options (in build.sbt)
```scala
-Xmx4g              # Heap size
-XX:+UseG1GC        # Garbage collector
```

### Logging (logback.xml)
- INFO and above to console
- Rolling files (100MB or daily)
- Suppressed Hadoop/Parquet verbose logs

### Akka (application.conf)
- Fork-join executor with parallelism tuning
- Back-pressure with bounded buffers (initial=1, max=1)

## Performance Benchmarks (500M rows, 12-core)

| Read | Agg | Mode | Runtime | Throughput |
|-----|-----|------|---------|-----------|
| 8 | 8 | scalar | 56.6s | 8.8M rows/s |
| 8 | 8 | vectorised | 54.8s | 9.1M rows/s |
| 12 | 12 | scalar | 55.2s | 9.1M rows/s |
| **12** | **12** | **vectorised** | **54.1s** | **9.2M rows/s** |

To run benchmarks:
```bash
sbt "runMain com.example.segmentagg.util.Benchmark /path/to/parquet"
# Output: /tmp/benchmark_results.csv
```

## Testing

```bash
# Run all tests
sbt test

# Run specific test
sbt "testOnly com.example.segmentagg.SegmentRevenueAggregatorSpec"
```

## Logging Output

```
[2026-05-15T14:32:10.123Z] [AggregationPipeline] INFO Starting job
[2026-05-15T14:33:04.567Z] [AggregationPipeline] INFO Final aggregate materialised with 10000000 groups (total elapsed=54432.44 ms)
[2026-05-15T14:33:04.573Z] [AggregationPipeline] INFO Stage metrics fetch=count=7631 avg=0.24 ms p50=0.16 ms p95=0.60 ms max=51.21 ms
[2026-05-15T14:33:04.580Z] [AggregationPipeline] INFO Stage metrics decode=count=7631 avg=5.28 ms p50=2.44 ms p95=4.61 ms max=1158.48 ms
```

Per-stage latencies:
- **fetch**: Read row group from file
- **decode**: Decode columns (segment_id, impressions, revenue)
- **aggregate**: Build partial aggregate (LongMap)
- **wait**: Time waiting for upstream (depends on parallelism)

## License

MIT License - see LICENSE file

