# SpatialBench Q1–Q10: geofunctions on Spark vs DuckDB

**Report:** [`index.html`](index.html) is the full report, with figures (open it in a browser). It is
built from [`results/sf1-sf10.jsonl`](results/sf1-sf10.jsonl) by `make_report.py`. This README keeps
the run notes and the history of each measurement.

[SpatialBench](https://sedona.apache.org/spatialbench/queries/) Q1–Q10, run on the same
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
- **Q5**: for each customer-month with more than 5 trips, the area of the convex hull of the
  dropoffs; the top 100 by area. `collect_list` → `ST_MULTIPOINT` → `ST_CONVEXHULL` → `ST_AREA`.
- **Q6**: trip counts and averages per zone, for zones intersecting a box around Sedona. A QR
  point-in-polygon join: one cell per point (`QR_FROMGEOM`), zones exploded with `QR_LIST`.
- **Q7**: the 100 trips with the largest reported-to-straight-line distance ratio. geofunctions
  uses `ST_DISTANCE(pickup, dropoff)`: a two-point line's length is the distance between its
  points, so building the line (`ST_LENGTH(ST_LINE(...))`) only costs time (3× at SF1).
- **Q8**: pickups within 0.0045° (~500 m) of each building, top 100. A QR distance join: each
  building is listed in every 0.05° cell its envelope grown by 0.0045 touches (`QR_LIST` with
  `dist`), each pickup in its one cell, so every pair within range meets exactly once.
- **scan**: `count(*) WHERE length(t_pickuploc) > 0`. This is the engine's cost to read the
  geometry column with no spatial work. It is the floor for both queries.

The geofunctions SQL is the SpatialBench SQL without `ST_GeomFromWKB`, because geofunctions
already uses WKB as its geometry type.

## Reproduce

```shell
pip install huggingface-hub duckdb pyspark==4.3.0
hf download apache-sedona/spatialbench --repo-type dataset \
  --include "v0.1.0/sf10/trip/*" --include "v0.1.0/sf10/zone/*" \
  --include "v0.1.0/sf10/customer/*" --include "v0.1.0/sf10/building/*" --local-dir spatialbench-data

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
- **Keys-first Q4 helps at SF100 only.** Sort just `(t_tip, t_tripkey)` for the top 1000
  keys, collect them, then fetch their geometry with `t_tripkey IN (<1000 keys>)` (needs
  `spark.sql.parquet.pushdown.inFilterThreshold` ≥ 1000). The published trip files are sorted by
  `t_tripkey` and carry Parquet page indexes, so the fetch skips most pages. Interleaved A/B on
  as-is data, 4 rounds: SF100 4.00 s vs 5.01 s (−20%), SF10 0.86 s vs 0.87 s (no change). At
  SF10 the fetch costs 0.20 s, more than a full geometry read (0.14 s), because 1000 random keys
  touch nearly every row group; 1–100 keys cost 0.08 s. Keeping the key subquery in one SQL
  statement (`IN (SELECT ...)`) is slower (7.6 s at SF100): it becomes a semi-join and nothing
  reaches Parquet. `bench.py` keeps the one-statement Q4.
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

## Q5 and Q6

SF1 and SF10, warm medians in seconds (5 runs), Spark 4.3 vs DuckDB 2.0, same run as the
re-measured Q1–Q4. QR-prepared at 1° cells. Q5 groups by customer, so the QR layout does not
apply to it. Every engine and layout returned the same rows. The one exception is Q6's
`avg_duration` at SF10, shown rounded to 0.01 s: 6159.64 s on Spark vs 6159.65 s on DuckDB.
The unrounded averages differ by less than 1 µs (Spark truncates an interval average to whole
microseconds, DuckDB rounds); they sit on either side of a rounding boundary, so the displayed
values differ by 0.01 s.

| SF | query | DuckDB as-is | Spark as-is | Spark ÷ DuckDB | DuckDB QR | Spark QR | Spark ÷ DuckDB |
|---|---|---|---|---|---|---|---|
| 1 | Q5 | 0.614 | 2.162 | 3.5× | — | — | — |
| | Q6 | 0.229 | 0.387 | 1.7× | 0.028 | 0.201 | 7.2× |
| 10 | Q5 | 4.098 | 14.126 | 3.4× | — | — | — |
| | Q6 | 0.631 | 1.299 | 2.1× | 0.060 | 0.230 | 3.8× |

- **Q5 is bound by Spark's collect path, not geometry.** At SF1, a count-only group-by takes
  0.83 s; adding `collect_list` makes it 2.06 s; multipoint, hull and area add nothing
  measurable. Customer-months average about 4 trips spread over every partition, so partial
  aggregation cannot shrink anything. Collecting doubles, structs or WKB all cost the same, a
  higher `objectHashAggregate` fallback threshold and sort-based aggregation saved under 5%, so a
  custom hull aggregate (same object-aggregate machinery) would not help. The one gain is
  `bench.py`'s two-phase form: count groups first, then collect points only for qualifying
  groups through a broadcast semi-join (2.4 s → 2.1 s at SF1).
- **Q6 gained 35–55% from two library changes.** (1) The six predicates now reject far-apart
  envelopes before the exact test: filtering 156k zones by the box fell from 0.283 s to 0.229 s.
  (2) `WkbCache` keeps the last 4 geometries, not 1: each trip near a county border meets 2–3
  counties in turn, and with one slot every switch re-parsed a 170 KB county. Q6 went from
  0.64–0.86 s to 0.39–0.41 s at SF1, and stopped depending on the cell size (0.5°–2°).
- **QR-prepared Q6** reads only the trips in the matching zones' cells: 0.23 s at SF10 vs
  1.30 s as-is.

## All queries, SF1 and SF10

Latest run, same jar for every query (with the point fast path below), warm medians in seconds
(5 runs). QR-prepared at 1° cells; "—" where the layout cannot prune (see `bench_qr.py`). All
rows agree across engines and layouts, except Q6's `avg_duration` at SF10, where unrounded
values less than 1 µs apart round to 0.01 s apart (see above).

| SF | query | DuckDB | Spark | Spark ÷ DuckDB | DuckDB QR | Spark QR | Spark ÷ DuckDB |
|---|---|---|---|---|---|---|---|
| 1 | Q1 | 0.067 | 0.121 | 1.8× | 0.004 | 0.054 | 13× |
| | Q2 | 0.104 | 0.170 | 1.6× | 0.018 | 0.139 | 7.7× |
| | Q3 | 0.101 | 0.174 | 1.7× | 0.002 | 0.078 | 39× |
| | Q4 | 0.127 | 0.503 | 4.0× | 0.046 | 0.355 | 7.7× |
| | Q5 | 0.624 | 2.012 | 3.2× | — | — | — |
| | Q6 | 0.230 | 0.332 | 1.4× | 0.024 | 0.195 | 8.1× |
| | Q7 | 0.142 | 0.155 | 1.1× | — | — | — |
| | Q8 | 0.303 | **0.211** | **0.7×** | — | — | — |
| 10 | Q1 | 0.233 | 0.451 | 1.9× | 0.006 | 0.079 | 13× |
| | Q2 | 0.320 | 0.465 | 1.5× | 0.050 | 0.197 | 3.9× |
| | Q3 | 0.343 | 0.532 | 1.6× | 0.005 | 0.114 | 23× |
| | Q4 | 0.245 | 0.975 | 4.0× | 0.168 | 0.784 | 4.7× |
| | Q5 | 4.069 | 13.410 | 3.3× | — | — | — |
| | Q6 | 0.607 | 1.054 | 1.7× | 0.059 | 0.267 | 4.5× |
| | Q7 | 0.536 | 0.629 | 1.2× | — | — | — |
| | Q8 | 1.285 | **0.988** | **0.8×** | — | — | — |

- **Q8 is Spark's best query: 23–30% faster than DuckDB.** The cell barely matters between 0.01°
  and 0.2° at SF1; 0.05° was best at both scales (SF10: 1.27 / 1.09 / 1.42 s at 0.01 / 0.05 /
  0.2°). Buildings are a few MB, so they are broadcast. They fill 45% of the 1° cells that hold
  trips, so a QR-prepared layout would prune little.
- **Q7 is within 10–20% of DuckDB.** At SF10: reading the 4 columns 0.43 s, + the distance 0.09
  s, + the ratio 0.03 s, + the top-100 over 60M rows with 3 ordering keys 0.18 s. The geometry
  is about 12% of the query and Esri already short-circuits point-to-point distance, so a custom
  distance path would save at most ~0.05 s.
- **Point fast path.** `bytes.geom` decodes a plain 2D WKB point directly (anything else still
  goes through Esri's importer, so results are bit-identical). Interleaved A/B at SF1: Q1 −21%,
  Q3 −9%, Q6 −8%, Q7 −7% (`ST_EUCLID`, four parses per row, −27%), Q8 −10%.

## Q9 and Q10

Warm medians in seconds (3 runs). A run over 10 minutes is stopped and shown as "> 600".

| SF | query | DuckDB | Spark | Spark speedup |
|---|---|---|---|---|
| 1 | Q9 | 39.4 | 0.178 | 221× |
| | Q10 | 83.8 | 3.73 | 22× |
| 10 | Q9 | > 600 | 0.315 | > 1900× |
| | Q10 | > 600 | 11.8 | > 50× |

- **Q9 (building overlap)** is a QR self-join at 0.05° cells: both sides exploded with
  `QR_ENVP`, one side broadcast, `QR_INTERSECT` and `id1 < id2` keep each pair once. DuckDB runs
  a nested-loop join on one core. Only 37 pairs intersect at SF1, so the result has 37 rows. IoU
  values differ from DuckDB's around the 12th digit (Esri vs GEOS intersection areas).
- **Q10 (trips in each zone)** was 20 s at SF1 with a per-row `ST_WITHIN`. Spark copies a binary
  value for each row, so 7.3M (zone, point) candidates copied about 1 TB of zone polygons.
  `ST_WITHIN_MASK` tests all candidate points of a zone in one call: the polygon is copied, parsed
  and accelerated once. Q10 fell to 3.7 s. QR-prepared pieces did not help (16–17 s): European
  cells hold hundreds of pieces, and some pieces are still 2.6 MB.

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
