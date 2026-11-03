# SpatialBench Q1–Q4: geofunctions on Spark vs DuckDB

[SpatialBench](https://sedona.apache.org/spatialbench/queries/) Q1–Q4, run on the same
Parquet files with geofunctions on Apache Spark and with DuckDB spatial. `bench.py` runs one
engine per process: one warm-up run, then N timed runs, and it reports the median.

- **Q1**: trips whose pickup is within 0.45° of Sedona, the 100 nearest. Uses `ST_DWITHIN`,
  `ST_DISTANCE`, `ST_X`, `ST_Y`.
- **Q2**: count of pickups inside Coconino County. Uses `ST_INTERSECTS` against a scalar
  subquery that returns a 10,583-vertex polygon.
- **Q3**: monthly trip statistics for pickups within 0.045° of a box around Sedona. Uses
  `ST_DWITHIN` against a constant polygon, then a monthly aggregate.
- **Q4**: zones holding the 1000 highest-tipping pickups. A point-in-polygon join of 1000 points
  against every zone (156k at SF1, 1.03M at SF100). geofunctions runs it as a QR join: both
  sides exploded into 10° cells (`QR_ENVP`), an equi-join on the cell with the points broadcast,
  `QR_INTERSECT` to keep each pair once, then `ST_WITHIN`.
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
pip install .                           # the geofunctions Python package, from the repo root
JAR=$(ls -S target/geofunctions-*.jar | head -1)   # the shaded jar is the largest in target/
python benchmarks/spatialbench/bench.py spark  "$JAR" spatialbench-data/v0.1.0/sf10 sf10
python benchmarks/spatialbench/bench.py duckdb -      spatialbench-data/v0.1.0/sf10 sf10
```

Extra `key=value` arguments after the run count become Spark configs, for example
`5 spark.sql.adaptive.enabled=false`.

For the QR-prepared layout, prepare once per cell size, then run both engines on it:

```shell
python benchmarks/spatialbench/prepare.py "$JAR" spatialbench-data/v0.1.0/sf10 prepared/sf10_c1 1
python benchmarks/spatialbench/bench_qr.py spark  "$JAR" prepared/sf10_c1 1 sf10
python benchmarks/spatialbench/bench_qr.py duckdb -      prepared/sf10_c1 1 sf10
```

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

## Q1–Q4, as-is and QR-prepared

Two layouts of the same data. **As-is** is the published Parquet. **QR-prepared** is written by
`prepare.py` once per cell size:

- `trip` gains `t_pickup_qr`, the QR cell of the pickup point, and is range-sorted by it.
- `zone` becomes per-cell pieces: each boundary is clipped (`qr_geom`) to every cell it touches,
  with its envelope stored alongside.

`bench_qr.py` runs the same plan on both engines, with identical cell lists (asserted equal to
`qr_cells`):

- **Q1 and Q3** keep only the cells their search area reaches, so Parquet skips the other row
  groups.
- **Q2** reads the county's cells, then joins trips to the county's pieces in those cells.
- **Q4** joins the 1000 points to the pieces of their own cell, with an envelope check before
  `ST_WITHIN`.

A point lies in exactly one cell, and each piece is clipped to a cell grown by a tiny `dist`. So
testing a point against its cell's pieces gives the same answer as testing it against the whole
boundary.

Warm medians in seconds (5 runs; 3 at SF100), cell = 1°. Every engine and layout returned the
same results at every scale.

| SF | query | DuckDB as-is | Spark as-is | Spark ÷ DuckDB | DuckDB QR | Spark QR | Spark ÷ DuckDB |
|---|---|---|---|---|---|---|---|
| 1 | Q1 | 0.066 | 0.126 | 1.9× | 0.004 | 0.055 | 14× |
| | Q2 | 0.103 | 0.203 | 2.0× | 0.017 | 0.136 | 8× |
| | Q3 | 0.100 | 0.170 | 1.7× | 0.002 | 0.081 | 40× |
| | Q4 | 0.124 | 0.506 | 4.1× | 0.045 | 0.338 | 7.5× |
| 10 | Q1 | 0.229 | 0.445 | 1.9× | 0.005 | 0.075 | 15× |
| | Q2 | 0.317 | 0.477 | 1.5× | 0.048 | 0.182 | 3.8× |
| | Q3 | 0.336 | 0.599 | 1.8× | 0.005 | 0.109 | 22× |
| | Q4 | 0.246 | 0.827 | 3.4× | 0.143 | 0.710 | 5.0× |
| 100 | Q1 | 2.334 | 2.757 | 1.2× | 0.028 | 0.272 | 9.7× |
| | Q2 | 3.549 | 2.989 | 0.8× | 0.389 | 0.568 | 1.5× |
| | Q3 | 3.743 | 3.643 | 1.0× | 0.028 | 0.302 | 11× |
| | Q4 | 1.233 | 3.755 | 3.0× | 1.072 | 4.270 | 4.0× |

Preparation is a one-time cost: 46 s at SF1, about 65 s at SF10 and 233 s at SF100 (trip 116 s,
zone 113 s).

- **QR preparation is the biggest lever for Spark.** At SF100, Q1 drops from 2.76 to 0.27 s,
  Q3 from 3.64 to 0.30 s and Q2 from 2.99 to 0.57 s. On prepared data, Spark answers Q1–Q3 6–12×
  faster than DuckDB does on the published data.
- **It helps DuckDB as much or more.** On the same prepared layout, DuckDB answers Q1 and Q3 in
  2–28 ms. Spark's floor is its per-job overhead (about 60 ms per job, and Q2 needs two to three
  jobs), so the ratio on prepared data looks worse even though both engines got faster.
- **Q4 is Spark's weakest query, and preparation does not fix it.** Its cost is a top-1000 by
  tip over every trip, carrying the geometry column, plus a full read of the zones. Neither part
  is spatially selective. DuckDB reads the geometry only for the 1000 winning rows (late
  materialization); Spark reads it for every row.
- **Cell size (SF10 sweep, 0.25°, 1°, 4°).** Q1 and Q3 sit at Spark's floor at every size. Q2 was
  0.43 / 0.195 / 0.21 s at 4° / 1° / 0.25°, because a big cell holds more trips per county piece.
  Q4 was 0.70–0.83 s at every size once each piece carried its envelope (1.49 s at 4° without
  it). Smaller cells cost more to prepare (zone: 45 s at 4°, 52 s at 1°, 83 s at 0.25°). 1° was
  the best balance here: about the size of the query areas.
- **Prune only compact areas.** Q1–Q3 prune to a few dozen cells. For Q4, a list of the ~700
  cells of the scattered top-1000 points made Spark slower (0.43 s vs 0.24 s for the join phase
  at SF1) than reading every piece. `bench_qr.py` therefore does not prune Q4.

### Q4 join strategy and cell size (as-is data)

`join_qr` runs this join two ways. By default it shuffles both sides by cell (cogroup, R-tree
per cell). With `broadcast="lhs"`, it equi-joins on the cell with the 1000 points broadcast.
Both give the same 258 zones.

| cell | broadcast | shuffle (SQL equi-join) | `join_qr` default (cogroup) |
|---|---|---|---|
| 0.5° | 0.573 | 55.4 | 44.2 |
| 2° | 0.433 | 3.53 | 3.55 |
| 10° | **0.419** | 1.54 | 2.17 |
| 45° | 0.568 | 1.29 | 1.93 |

SF1, seconds. The shuffle paths move all 1.4 GB of zone polygons, and small cells replicate the
large polygons (some zones span 360°) into many more rows. With one side this small, broadcast
it. At SF10, broadcast is flat at 0.71–0.84 s for 1°–20°.

## When to use which

- **DuckDB** wins on small data and interactive one-off queries. A Spark session costs 2.5 s
  before any work, and each query pays about 60 ms in fixed overhead.
- **Spark with geofunctions** matches DuckDB once data reaches hundreds of millions of rows,
  even on one machine. Spark's advantage is what one machine cannot do: data larger than
  memory or local disk, a cluster, and running inside existing Spark pipelines.
- **QR-prepare data that is queried by area again and again.** Sorting by a QR cell costs one
  rewrite (4 minutes for 600M trips here). After it, area queries read only their cells, and
  Spark beats DuckDB-on-raw-data by 6–12×. Pick a cell near the size of the typical query area.
- **Broadcast the small side of a QR join** (`join_qr(..., broadcast="lhs")`). Shuffling a large
  polygon table to meet a few thousand points costs 4–100× more.
