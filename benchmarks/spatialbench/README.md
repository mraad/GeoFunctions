# SpatialBench Q1/Q2: geofunctions on Spark vs DuckDB

[SpatialBench](https://sedona.apache.org/spatialbench/queries/) Q1 and Q2, run on the same
Parquet files with geofunctions on Apache Spark and with DuckDB spatial. `bench.py` runs one
engine per process: one warm-up run, then N timed runs, and it reports the median.

- **Q1**: trips whose pickup is within 0.45° of Sedona, the 100 nearest. Uses `ST_DWITHIN`,
  `ST_DISTANCE`, `ST_X`, `ST_Y`.
- **Q2**: count of pickups inside Coconino County. Uses `ST_INTERSECTS` against a scalar
  subquery that returns a 10,583-vertex polygon.
- **scan**: `count(*) WHERE length(t_pickuploc) > 0`. This is the engine's cost to read the
  geometry column with no spatial work. It is the floor for both queries.

The geofunctions SQL is the SpatialBench SQL without `ST_GeomFromWKB`, because geofunctions
already uses WKB as its geometry type.

## Reproduce

```shell
pip install huggingface-hub duckdb pyspark==4.3.0
hf download apache-sedona/spatialbench --repo-type dataset \
  --include "v0.1.0/sf10/trip/*" --include "v0.1.0/sf10/zone/*" --local-dir spatialbench-data

mvn -P spark-4.3 clean package          # Spark 4.3 jar; pair with pyspark==4.3.0
JAR=$(ls -S target/geofunctions-*.jar | head -1)   # the shaded jar is the largest in target/
python benchmarks/spatialbench/bench.py spark  "$JAR" spatialbench-data/v0.1.0/sf10 sf10
python benchmarks/spatialbench/bench.py duckdb -      spatialbench-data/v0.1.0/sf10 sf10
```

Extra `key=value` arguments after the run count become Spark configs, for example
`5 spark.sql.adaptive.enabled=false`.

## Results

Test machine: Apple silicon, 16 cores, 128 GB RAM, local NVMe. Spark ran as `local[*]` with a
64 GB driver; DuckDB 2.0.0 used its defaults. Times are warm medians in seconds (5 runs; 3 at
SF100). The OS page cache was warm for both engines. The engines ran one after the other, not
interleaved, so slow drift (thermal, page cache) is not controlled for. Repeating a whole SF10
series later showed about 10% drift for both engines, so treat differences under about 10% as
noise.

| SF | trips | query | DuckDB | Spark 4.3.0 | Spark 3.5.9 | Spark 4.3 / DuckDB |
|---|---|---|---|---|---|---|
| 1 | 6 M | scan | 0.018 | 0.098 | 0.105 | 5.4× |
| 1 | | Q1 | 0.066 | 0.127 | 0.149 | 1.9× |
| 1 | | Q2 | 0.102 | 0.202 | 0.195 | 2.0× |
| 10 | 60 M | scan | 0.058 | 0.210 | 0.282 | 3.6× |
| 10 | | Q1 | 0.227 | 0.399 | 0.403 | 1.8× |
| 10 | | Q2 | 0.306 | 0.388 | 0.480 | 1.3× |
| 100 | 600 M | scan | 0.540 | 1.687 | 1.355 | 3.1× |
| 100 | | Q1 | 2.239 | 2.788 | 3.247 | 1.25× |
| 100 | | Q2 | 3.368 | **3.213** | 3.403 | **0.95×** |

Every engine returned the same answers. Q1's top 3 keys match, and the Q2 counts are 541,
5499 and 53348 at SF1, SF10 and SF100. At SF1, all 100 Q1 rows match DuckDB to 5.6e-17.

Costs that the warm medians leave out:

| | DuckDB | Spark 4.3 |
|---|---|---|
| Start-up (connect, or create a session) | 0.08 s | 2.5 s |
| Start-up plus first result | — | 3.5 s |
| First run of a query (SF100 Q1) | 2.2 s | 6.3 s (JIT warm-up) |

## What the numbers say

- **The scan is the gap, not the rest of the query.** At SF100, query time minus scan time is
  1.1 s (Q1) and 1.5 s (Q2) for Spark, and 1.7 s and 2.8 s for DuckDB. That difference
  includes all non-scan work (spatial predicates, projection, sorting, Q2's subquery), so it
  does not isolate the geometry engine. It does show that, after the scan, Spark with
  geofunctions does this work no slower than DuckDB.
- **The gap is the Spark engine.** Each Spark job has about 60 ms of fixed overhead
  (scheduling, task launch, building rows). Spark's Parquet scan of the WKB column is about 3×
  slower than DuckDB's at every scale, because Spark copies each value into its own `byte[]`.
  Q2's subquery also runs as a separate job (about 60 ms) before the main scan.
- **The gap shrinks with scale.** The fixed costs stay the same while the spatial work grows.
  Q2 goes from 2.0× at SF1 to 1.3× at SF10, and Spark is faster at SF100. Q1 goes from 1.9×
  to 1.25×.
- **Parquet row groups cap scan parallelism in both engines.** SF1 `trip` has only 4 row groups,
  so each engine uses at most 4 of the 16 cores. SF10 has 45 and SF100 has 462. See
  [Does splitting the input help?](#does-splitting-the-input-help).
- **Spark tuning did not help.** These made no difference or made things worse, at SF1 and
  SF10: AQE off, `maxPartitionBytes=32m` (empty splits, because no split can be smaller than
  a row group), and `columnarReaderBatchSize=16384`. The Spark 4.3 defaults are the best
  setting here.
- **Spark 4.3 vs 3.5.9.** With the same library code, Spark 4.3 is 0–19% faster on Q1 and Q2.
  The scan floor alone is mixed: faster at SF10, slower at SF100.

## Does splitting the input help?

`split.py <src_sf_dir> <dst_dir> <N>` rewrites `trip` as N snappy files, one row group each,
in the original row order, and links `zone`. Spark then gets N non-empty scan tasks. On the
original SF1 layout, Spark plans 16 tasks but only 4 of them hold data.

| layout | scan DuckDB | scan Spark | Q1 DuckDB | Q1 Spark | Q1 S/D | Q2 DuckDB | Q2 Spark | Q2 S/D |
|---|---|---|---|---|---|---|---|---|
| SF1, original (4 row groups) | 0.018 | 0.105 | 0.065 | 0.112 | 1.7× | 0.103 | 0.184 | 1.8× |
| SF1, 16 parts | 0.009 | 0.088 | 0.029 | 0.111 | 3.8× | 0.047 | 0.154 | 3.3× |
| SF1, 32 parts | 0.009 | 0.096 | 0.028 | 0.092 | 3.3× | 0.045 | 0.189 | 4.2× |
| SF1, 64 parts | 0.010 | 0.090 | 0.029 | 0.101 | 3.5× | 0.046 | 0.145 | 3.2× |
| SF10, original (45 row groups) | 0.066 | 0.221 | 0.248 | 0.506 | 2.0× | 0.343 | 0.433 | 1.3× |
| SF10, 16 parts | 0.080 | 0.235 | 0.258 | 0.488 | 1.9× | 0.393 | 0.500 | 1.3× |
| SF10, 32 parts | 0.071 | 0.239 | 0.270 | 0.410 | 1.5× | 0.392 | 0.478 | 1.2× |
| SF10, 48 parts | 0.068 | 0.262 | 0.276 | 0.411 | 1.5× | 0.391 | 0.442 | 1.1× |
| SF10, 64 parts | 0.065 | 0.253 | 0.262 | 0.371 | 1.4× | 0.373 | 0.458 | 1.2× |

All layouts return the same answers. These SF10 runs were about 10% slower than the main table
in both engines, so compare within this table only.

- **Splitting does not help Spark at SF1.** All 16 cores get work, but each query still costs
  about 0.1 s. At this size Spark's time is fixed per-job overhead, not scan work, so more
  cores cannot shorten it.
- **Splitting helps DuckDB at SF1, about 2×.** It has almost no fixed overhead, so its time
  does scale with cores. The gap between the engines grows, from about 1.8× to 3–4×.
- **At SF10 and above, the original layout is already fine.** 45 row groups is more than
  16 cores. Splitting finer (32–64 parts) gives Spark about 0–25% on Q1 and nothing clear on
  Q2, which is close to run-to-run noise. Do not rewrite SF100 (462 row groups).
- **Rule of thumb:** make sure a dataset has at least as many row groups as cores, and two to
  four times as many if you can. Above that, file layout stops mattering, and Spark's
  overhead is spread over more data as the data grows.

## When to use which

- **DuckDB** wins on small data and interactive one-off queries. A Spark session costs 2.5 s
  before any work, and each query pays about 60 ms in fixed overhead.
- **Spark with geofunctions** matches DuckDB once data reaches hundreds of millions of rows,
  even on one machine. Spark's advantage is what one machine cannot do: data larger than
  memory or local disk, a cluster, and running inside existing Spark pipelines.
