# Akka Segment Aggregation

Streaming pipeline built with Akka Streams that reads Parquet files and computes:

```sql
SELECT segment_id, SUM(impressions) AS total_impressions, SUM(revenue) AS total_revenue
FROM fact_events
GROUP BY segment_id;
```

## Quick Start

```bash
sbt clean compile

sbt "run --input /path/to/parquet --output /path/to/results.csv --read-parallelism 8 --aggregate-parallelism 8"

sbt test
```

## How It Works

```
Parquet Files
     ↓
Read (N concurrent file readers, one ColumnBatch per row group)
     ↓
Aggregate (M concurrent workers, each builds a partial LongMap)
     ↓
Merge (single thread, folds all partials into final result)
     ↓
Write CSV
```

- **Read parallelism** controls how many files are decoded in parallel
- **Aggregate parallelism** controls how many partial aggregations run concurrently
- Aggregate should be ≤ read (no gain when aggregate > read since read is the bottleneck)

## CLI Options

```
--input <path>                Path to Parquet file or directory (required)
--output <path>               Path for CSV output file (required)
--read-parallelism <n>        Concurrent file readers (default: available CPUs)
--aggregate-parallelism <n>   Concurrent aggregate workers (default: available CPUs)
--parallelism <n>             Set both read and aggregate at once
```

## Project Structure

```
src/main/scala/com/example/segmentagg/
├── Main.scala                 # CLI entry point, argument parsing
├── PipelineConfig.scala       # Immutable config case class
├── AggregationPipeline.scala  # Pipeline orchestration, metrics, CSV writing
├── ColumnBatch.scala          # Columnar data model + batch timing
├── Aggregator.scala           # GROUP BY logic with partial merge
├── ParquetBatchSource.scala   # Akka Streams source over Parquet row groups
└── parquet/
    └── ColumnDecoder.scala    # Parquet page decoding (Long + Double columns)

src/test/scala/com/example/segmentagg/
└── AggregatorSpec.scala       # Unit tests for aggregation logic
```

## Generate Test Data

Using [DuckDB](https://duckdb.org/):

```sql
CREATE TABLE fact_events AS
SELECT
    ABS(hash(i) % 10000000) AS segment_id,
    (ABS(hash(i + 1) % 1000) + 1) AS impressions,
    ABS(hash(i + 2) % 999999) * 0.00000100001 + ABS(hash(i + 3) % 9973) * 0.001 AS revenue
FROM range(500000000) t(i);

COPY fact_events TO '/path/to/fact_events_parquet' (FORMAT parquet, ROW_GROUP_SIZE 65536);
```

## Output

```csv
segment_id,total_impressions,total_revenue
1,102345,123.45
2,98765,98.76
...
```

## License

MIT
