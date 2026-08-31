# QR point-in-time-zone benchmark

`optimize_qr_join.py` selects a QR cell size and times a point-in-polygon join
between 5 million deterministic random longitude/latitude points and
`data/world/tz_world.shp`.

The quality calculation follows Esri's
[Evaluate Bin Sizes for Point Aggregation](https://pro.arcgis.com/en/pro-app/latest/tool-reference/spatial-statistics/how-evaluate-bins-works.htm)
methodology for square bins:

- **Internal uniformity** is the fraction of nonempty bins whose 5-by-5
  subcell counts pass a chi-square goodness-of-fit test at `p > 0.05`.
- **Point-count variety** is normalized Shannon entropy over five equal-width
  classes spanning the observed bin-count range. Empty bins are included.
- **Quality score** is the product of the two metrics.

Only QR cells fully contained by the `[-180, 180) × [-80, 80)` study boundary
are used for quality evaluation, preventing partial edge cells from appearing
artificially nonuniform. Join timing still uses all 5 million points.

## Reproduce

Build the current JAR, then run the benchmark from the repository root:

```shell
source .venv/bin/activate
./pw.sh
python benchmarks/optimize_qr_join.py \
  --points 5000000 \
  --partitions 200 \
  --timing-cells 0.9,1,1.1,1.2,1.25 \
  --repeats 3 \
  --output target/qr-benchmark.json
```

The script finds the versioned shaded JAR under `target/`, fixes both random
seeds, verifies that all timed cell sizes produce the same match count, and
defaults to the faster broadcast strategy. Use `--strategies both` to compare
it with the grouped/R-tree implementation.

## Results

The measurements below were collected locally with Spark 3.5.9, 200
partitions, 16 concurrent tasks, and seed 1729. All cell sizes returned exactly
1,556,053 time-zone matches.

The refined quality sweep around the timing minimum was:

| Cell (degrees) | Internal uniformity | Point-count variety | Quality score |
|---:|---:|---:|---:|
| 0.9 | 0.9530 | 0.6518 | 0.6211 |
| 1.0 | 0.9515 | 0.6463 | 0.6150 |
| 1.1 | 0.9510 | 0.6461 | 0.6144 |
| **1.2** | **0.9506** | **0.6465** | **0.6145** |
| 1.25 | 0.9508 | 0.6454 | 0.6137 |
| 1.5 | 0.9525 | 0.6806 | 0.6483 |

Three timing passes in different candidate orders produced these median total
times for the finalists:

| Cell (degrees) | Median total time (seconds) |
|---:|---:|
| 0.9 | 6.78 |
| 1.0 | 6.44 |
| 1.1 | 6.47 |
| **1.2** | **6.29** |
| 1.25 | 6.47 |

The broad sweep explains why a statistically larger bin is not the best join
index for this workload:

| Cell (degrees) | Polygon QR rows | Candidate pairs | Total time (seconds) |
|---:|---:|---:|---:|
| 0.5 | 135,486 | 2,616,266 | 10.15 |
| 1.0 | 59,674 | 4,828,467 | 6.58 |
| 2.0 | 38,134 | 12,800,623 | 7.10 |
| 4.0 | 31,554 | 43,015,909 | 10.04 |
| 5.0 | 30,548 | 65,368,314 | 11.55 |
| 10.0 | 28,907 | 248,182,473 | 25.81 |
| 20.0 | 28,287 | 972,336,239 | 77.48 |

The 20-degree bin has the largest pure quality score (`0.8720`) because the
input is deliberately uniform random data and therefore has no genuine local
clusters for large bins to hide. It is a poor join index: it generates nearly
one billion candidate pairs. The selected **1.2-degree cell** retains a 0.9506
internal-uniformity score and 0.6465 point-count-variety score while minimizing
measured execution time.

At 1.2 degrees, the grouped/R-tree strategy took 22.72 seconds in a separate
5-million-point check. Explicitly broadcasting the 51,230 QR-clipped polygon
rows is therefore the preferred execution plan for this many-points-to-few-
polygons workload.
