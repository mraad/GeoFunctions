#!/usr/bin/env python3
"""Select and benchmark a QR cell size for the world time-zone join.

The quality metrics follow the point-count workflow described by ArcGIS Pro's
"Evaluate Bin Sizes for Point Aggregation" documentation:

* internal uniformity is the fraction of nonempty QR cells whose 5-by-5
  subcell counts pass a chi-square goodness-of-fit test at p > 0.05; and
* point-count variety is normalized Shannon entropy after the distribution of
  cell counts (including empty cells) is split into five equal-width classes.

The quality score is the product of those two values.  Spatial-join timings
are then collected for every cell that meets the requested minimum values for
both metrics.  The fastest qualifying cell is recommended.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shlex
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Sequence


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SHAPEFILE = REPOSITORY_ROOT / "data" / "world" / "tz_world.shp"
DEFAULT_CELLS = (
    0.25,
    0.5,
    0.6,
    0.7,
    0.75,
    0.8,
    0.9,
    1.0,
    1.1,
    1.2,
    1.25,
    1.5,
    1.75,
    2.0,
    3.0,
    4.0,
    5.0,
    7.5,
    10.0,
    15.0,
    20.0,
)

# p > 0.05 for a chi-square distribution with 24 degrees of freedom.
CHI_SQUARE_24_P05 = 36.41502850180731


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class QualityResult:
    cell: float
    evaluated_points: int
    total_bins: int
    nonempty_bins: int
    min_count: int
    max_count: int
    internal_uniformity: float
    point_count_variety: float
    quality_score: float
    seconds: float


@dataclass(frozen=True)
class TimingResult:
    cell: float
    strategy: str
    trial: int
    point_index_rows: int
    polygon_index_rows: int
    candidate_pairs: int | None
    matched_pairs: int
    point_index_seconds: float
    polygon_index_seconds: float
    join_seconds: float
    total_seconds: float


@dataclass(frozen=True)
class Recommendation:
    cell: float
    strategy: str
    trials: int
    median_total_seconds: float
    # False when the timed cells never cleared the quality floor: the fallback path
    # and --timing-cells both time cells the quality filter did not select, and the
    # recommendation is chosen on median time alone. Without this the JSON output
    # cannot tell a qualifying recommendation from a non-qualifying one.
    meets_quality_minima: bool = True


def select_benchmark_cells(
    quality: Sequence[QualityResult],
    all_cells: Sequence[float],
    min_internal_uniformity: float,
    min_point_count_variety: float,
    timing_cells: Sequence[float] | None,
) -> tuple[tuple[float, ...], bool, str | None]:
    """Choose which cells to time, and say whether they cleared the quality floor.

    Returns (cells, meets_quality_minima, notice). The flag is what keeps a
    recommendation honest: both the fallback and an explicit --timing-cells time
    cells the quality filter did not select, and the winner is picked on median
    time alone, so without it a non-qualifying cell is indistinguishable from a
    qualifying one in the printed and JSON output.
    """
    cells = tuple(all_cells)
    # Start false: "qualified" means measured and passing. With --skip-quality nothing
    # is measured at all, which is not the same as clearing the floor.
    qualified = False
    notice: str | None = None

    if quality:
        passing = tuple(
            result.cell
            for result in quality
            if result.internal_uniformity >= min_internal_uniformity
            and result.point_count_variety >= min_point_count_variety
        )
        if passing:
            cells = passing
            qualified = True
            notice = (
                "Benchmarking cells with internal uniformity at or above "
                f"{min_internal_uniformity:.4f} and point-count variety at or above "
                f"{min_point_count_variety:.4f}: "
                + ", ".join(f"{cell:g}" for cell in cells)
            )
        else:
            best = max(quality, key=lambda result: result.quality_score)
            cells = (best.cell,)
            qualified = False
            notice = (
                "No candidate met both quality minima; falling back to the best "
                f"quality score at {best.cell:g}. The recommendation below is the "
                "fastest of a set that failed the quality floor, not a qualifying cell."
            )

    if timing_cells:
        cells = tuple(timing_cells)
        qualified = False
        notice = (
            "Using explicit timing cells, which bypass the quality filter: "
            + ", ".join(f"{cell:g}" for cell in cells)
        )

    return cells, qualified, notice


def _parse_cells(raw: str) -> tuple[float, ...]:
    cells = tuple(dict.fromkeys(float(value) for value in raw.split(",")))
    if not cells or any(not math.isfinite(cell) or cell <= 0.0 for cell in cells):
        raise argparse.ArgumentTypeError("cell sizes must be finite positive numbers")
    return cells


def _find_jar() -> Path:
    target = REPOSITORY_ROOT / "target"
    candidates = sorted(
        path
        for path in target.glob("geofunctions-*.jar")
        if not path.name.startswith("original-") and not path.stem.endswith("-2.12")
    )
    if not candidates:
        raise FileNotFoundError("no shaded geofunctions JAR found; run ./pw.sh first")
    return candidates[-1]


def _configure_pyspark(jar: Path) -> None:
    if "PYSPARK_SUBMIT_ARGS" in os.environ:
        return
    quoted_jar = shlex.quote(str(jar))
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-class-path {quoted_jar} --jars {quoted_jar} pyspark-shell"
    )


def _create_spark(master: str, driver_memory: str) -> "SparkSession":
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master(master)
        .appName("OptimizeQRPointInTimeZone")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _make_points(
    spark: "SparkSession",
    count: int,
    partitions: int,
    seed: int,
) -> "DataFrame":
    import geofunctions as S
    import pyspark.sql.functions as F
    from pyspark import StorageLevel

    points = (
        spark.range(count, numPartitions=partitions)
        .select(
            "id",
            (F.rand(seed) * 360.0 - 180.0).alias("x"),
            (F.rand(seed + 1) * 160.0 - 80.0).alias("y"),
        )
        .withColumn("point", S.st_point("x", "y"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    actual = points.count()
    if actual != count:
        raise RuntimeError(f"expected {count} generated points, found {actual}")
    return points


def _make_polygon_parts(
    spark: "SparkSession",
    shapefile: Path,
) -> "DataFrame":
    import geofunctions as S
    from pyspark import StorageLevel

    parts = (
        spark.read.format("shp")
        .option("path", str(shapefile))
        .load()
        .select("tzid", S.st_dump_explode("shape").alias("shape"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    parts.count()
    return parts


def _full_grid_range(
    lower: float,
    upper: float,
    cell: float,
) -> tuple[int, int]:
    """Return the QR-index range fully contained in [lower, upper)."""
    return math.ceil(lower / cell), math.floor(upper / cell)


def _entropy(bucket_counts: Iterable[int]) -> float:
    counts = tuple(bucket_counts)
    total = sum(counts)
    if total == 0:
        return 0.0
    return -sum(
        (count / total) * math.log(count / total) for count in counts if count
    ) / math.log(len(counts))


def evaluate_quality(points: "DataFrame", cell: float) -> QualityResult:
    """Calculate exact square-bin quality metrics for one candidate size."""
    import pyspark.sql.functions as F
    from pyspark import StorageLevel

    started = time.perf_counter()
    subcell = cell / 5.0
    q_start, q_stop = _full_grid_range(-180.0, 180.0, cell)
    r_start, r_stop = _full_grid_range(-80.0, 80.0, cell)
    total_bins = (q_stop - q_start) * (r_stop - r_start)
    if total_bins <= 0:
        raise ValueError(f"cell {cell:g} is too large for the study boundary")
    subcell_counts = (
        points.select(
            F.floor(F.col("x") / F.lit(cell)).cast("long").alias("q"),
            F.floor(F.col("y") / F.lit(cell)).cast("long").alias("r"),
            F.floor(F.col("x") / F.lit(subcell)).cast("long").alias("sq"),
            F.floor(F.col("y") / F.lit(subcell)).cast("long").alias("sr"),
        )
        .filter(
            F.col("q").between(q_start, q_stop - 1)
            & F.col("r").between(r_start, r_stop - 1)
        )
        .groupBy("q", "r", "sq", "sr")
        .count()
    )
    bin_counts = (
        subcell_counts.groupBy("q", "r")
        .agg(
            F.sum("count").cast("long").alias("point_count"),
            F.sum(F.col("count") * F.col("count"))
            .cast("double")
            .alias("sum_squared_subcell_counts"),
        )
        .withColumn(
            "chi_square",
            F.lit(25.0) * F.col("sum_squared_subcell_counts") / F.col("point_count")
            - F.col("point_count"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    summary = bin_counts.agg(
        F.sum("point_count").alias("evaluated_points"),
        F.count(F.lit(1)).alias("nonempty_bins"),
        F.min("point_count").alias("min_nonempty_count"),
        F.max("point_count").alias("max_count"),
        F.avg((F.col("chi_square") < F.lit(CHI_SQUARE_24_P05)).cast("double")).alias(
            "internal_uniformity"
        ),
    ).first()
    nonempty_bins = int(summary.nonempty_bins)
    max_count = int(summary.max_count)
    if nonempty_bins > total_bins:
        raise RuntimeError(
            f"found {nonempty_bins} nonempty bins in a {total_bins}-bin boundary"
        )
    empty_bins = total_bins - nonempty_bins
    min_count = 0 if empty_bins else int(summary.min_nonempty_count)

    # Five equal-width classes over the observed count range.  Empty cells
    # make the observed minimum zero; otherwise the least nonempty count is
    # the lower endpoint.  The cap puts the maximum into class 4 instead of
    # creating a sixth class at the right endpoint.
    if min_count == max_count:
        bucket_rows = [(0, nonempty_bins)]
    else:
        bucket_rows = (
            bin_counts.select(
                F.least(
                    F.floor(
                        (F.col("point_count") - F.lit(min_count))
                        * F.lit(5.0)
                        / F.lit(max_count - min_count)
                    ).cast("int"),
                    F.lit(4),
                ).alias("bucket")
            )
            .groupBy("bucket")
            .count()
            .collect()
        )
    counts = [0] * 5
    counts[0] = empty_bins
    for row in bucket_rows:
        if isinstance(row, tuple):
            bucket, count = row
        else:
            bucket, count = int(row.bucket), int(row["count"])
        counts[bucket] += count

    internal_uniformity = float(summary.internal_uniformity)
    point_count_variety = _entropy(counts)
    bin_counts.unpersist()
    return QualityResult(
        cell=cell,
        evaluated_points=int(summary.evaluated_points),
        total_bins=total_bins,
        nonempty_bins=nonempty_bins,
        min_count=min_count,
        max_count=max_count,
        internal_uniformity=internal_uniformity,
        point_count_variety=point_count_variety,
        quality_score=internal_uniformity * point_count_variety,
        seconds=time.perf_counter() - started,
    )


def _time_broadcast_join(
    points: "DataFrame",
    polygon_parts: "DataFrame",
    cell: float,
    trial: int,
    dist: float,
    wkid: int,
) -> TimingResult:
    import geofunctions as S
    import pyspark.sql.functions as F
    from pyspark import StorageLevel

    started = time.perf_counter()
    point_started = time.perf_counter()
    point_index = points.select(
        "id",
        F.col("point"),
        S.qr_fromxy("x", "y", cell).alias("qr"),
    ).persist(StorageLevel.MEMORY_AND_DISK)
    point_index_rows = point_index.count()
    point_seconds = time.perf_counter() - point_started

    polygon_started = time.perf_counter()
    polygon_index = (
        polygon_parts.select(
            "tzid",
            S.qr_geom_explode("shape", cell, dist, wkid).alias("qr_part"),
        )
        .select(
            "tzid",
            F.col("qr_part.qr").alias("qr"),
            F.col("qr_part.geom").alias("polygon"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    polygon_index_rows = polygon_index.count()
    polygon_seconds = time.perf_counter() - polygon_started

    join_started = time.perf_counter()
    result = (
        point_index.join(F.broadcast(polygon_index), "qr")
        .agg(
            F.count(F.lit(1)).alias("candidate_pairs"),
            F.sum(S.st_contains("polygon", "point", wkid).cast("long")).alias(
                "matched_pairs"
            ),
        )
        .first()
    )
    join_seconds = time.perf_counter() - join_started
    point_index.unpersist()
    polygon_index.unpersist()
    return TimingResult(
        cell=cell,
        strategy="broadcast",
        trial=trial,
        point_index_rows=point_index_rows,
        polygon_index_rows=polygon_index_rows,
        candidate_pairs=int(result.candidate_pairs),
        matched_pairs=int(result.matched_pairs),
        point_index_seconds=point_seconds,
        polygon_index_seconds=polygon_seconds,
        join_seconds=join_seconds,
        total_seconds=time.perf_counter() - started,
    )


def _time_grouped_join(
    points: "DataFrame",
    polygon_parts: "DataFrame",
    cell: float,
    trial: int,
    partitions: int,
    dist: float,
    wkid: int,
) -> TimingResult:
    import geofunctions as S
    import pyspark.sql.functions as F
    from pyspark import StorageLevel

    started = time.perf_counter()
    point_started = time.perf_counter()
    point_index = (
        points.select("id", F.col("point").alias("geometry"))
        .withColumn("qr", S.qr_envp_explode("geometry", cell))
        .repartition(partitions, "qr.qr")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    point_index_rows = point_index.count()
    point_seconds = time.perf_counter() - point_started

    polygon_started = time.perf_counter()
    polygon_index = (
        polygon_parts.select(
            "tzid",
            S.qr_envp_geom_explode("shape", cell, dist, wkid).alias("qr_part"),
        )
        .select(
            "tzid",
            F.col("qr_part.geom").alias("geom"),
            F.col("qr_part").dropFields("geom").alias("qr"),
        )
        .repartition(partitions, "qr.qr")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    polygon_index_rows = polygon_index.count()
    polygon_seconds = time.perf_counter() - polygon_started

    join_started = time.perf_counter()
    matched_pairs = point_index.join_qr(
        polygon_index,
        cell,
        wkid=wkid,
        oper="within",
        lhs_geom="geometry",
        acceleration="hot",
    ).count()
    join_seconds = time.perf_counter() - join_started
    point_index.unpersist()
    polygon_index.unpersist()
    return TimingResult(
        cell=cell,
        strategy="grouped",
        trial=trial,
        point_index_rows=point_index_rows,
        polygon_index_rows=polygon_index_rows,
        candidate_pairs=None,
        matched_pairs=matched_pairs,
        point_index_seconds=point_seconds,
        polygon_index_seconds=polygon_seconds,
        join_seconds=join_seconds,
        total_seconds=time.perf_counter() - started,
    )


def _print_quality(results: Sequence[QualityResult]) -> None:
    print("\nQuality sweep")
    print(
        "cell  points      bins     nonempty  min   max  "
        "uniform  variety  score   seconds"
    )
    for result in results:
        print(
            f"{result.cell:>5g} {result.evaluated_points:>9,d} "
            f"{result.total_bins:>9,d} "
            f"{result.nonempty_bins:>9,d} {result.min_count:>4,d} "
            f"{result.max_count:>5,d} "
            f"{result.internal_uniformity:>7.4f} "
            f"{result.point_count_variety:>7.4f} "
            f"{result.quality_score:>7.4f} {result.seconds:>8.2f}"
        )


def _print_timings(results: Sequence[TimingResult]) -> None:
    print("\nSpatial-join timings")
    print(
        "cell  strategy  trial  polygon rows  candidates  matches  "
        "index(s)  join(s)  total(s)"
    )
    for result in results:
        candidates = (
            f"{result.candidate_pairs:,}"
            if result.candidate_pairs is not None
            else "n/a"
        )
        index_seconds = result.point_index_seconds + result.polygon_index_seconds
        print(
            f"{result.cell:>5g} {result.strategy:<10} {result.trial:>5,d} "
            f"{result.polygon_index_rows:>12,d} {candidates:>11} "
            f"{result.matched_pairs:>8,d} {index_seconds:>8.2f} "
            f"{result.join_seconds:>8.2f} {result.total_seconds:>9.2f}"
        )


def _write_results(
    output: Path,
    quality: Sequence[QualityResult],
    timings: Sequence[TimingResult],
    recommendation: Recommendation | None,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() == ".csv":
        rows = [{"result_type": "quality", **asdict(result)} for result in quality] + [
            {"result_type": "timing", **asdict(result)} for result in timings
        ]
        keys = sorted({key for row in rows for key in row})
        with output.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)
        return
    payload = {
        "quality": [asdict(result) for result in quality],
        "timings": [asdict(result) for result in timings],
        "recommendation": asdict(recommendation) if recommendation else None,
    }
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--points", type=int, default=5_000_000)
    parser.add_argument("--partitions", type=int, default=200)
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument(
        "--cells",
        type=_parse_cells,
        default=DEFAULT_CELLS,
        help="comma-separated candidate cell sizes in degrees",
    )
    parser.add_argument(
        "--min-internal-uniformity",
        type=float,
        default=0.95,
        help="minimum internal-uniformity score required for join timing",
    )
    parser.add_argument(
        "--min-point-count-variety",
        type=float,
        default=0.60,
        help="minimum point-count-variety score required for join timing",
    )
    parser.add_argument(
        "--strategies",
        choices=("broadcast", "grouped", "both"),
        default="broadcast",
    )
    parser.add_argument(
        "--timing-cells",
        type=_parse_cells,
        help="optional candidate subset to time after the quality sweep",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="number of timing trials; even trials use reverse candidate order",
    )
    parser.add_argument("--dist", type=float, default=0.00001)
    parser.add_argument("--wkid", type=int, default=4326)
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--driver-memory", default="16g")
    parser.add_argument("--shapefile", type=Path, default=DEFAULT_SHAPEFILE)
    parser.add_argument("--jar", type=Path, default=None)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--skip-quality", action="store_true")
    parser.add_argument("--skip-join", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.points <= 0 or args.partitions <= 0 or args.repeats <= 0:
        raise ValueError("points, partitions, and repeats must be positive")
    if not 0.0 <= args.min_internal_uniformity <= 1.0:
        raise ValueError("min-internal-uniformity must be in [0, 1]")
    if not 0.0 <= args.min_point_count_variety <= 1.0:
        raise ValueError("min-point-count-variety must be in [0, 1]")
    shapefile = args.shapefile.resolve()
    if not shapefile.exists():
        raise FileNotFoundError(shapefile)
    jar = (args.jar or _find_jar()).resolve()
    _configure_pyspark(jar)

    spark = _create_spark(args.master, args.driver_memory)
    quality: list[QualityResult] = []
    timings: list[TimingResult] = []
    recommendation: Recommendation | None = None
    try:
        points = _make_points(spark, args.points, args.partitions, args.seed)
        polygon_parts = (
            None if args.skip_join else _make_polygon_parts(spark, shapefile)
        )

        if not args.skip_quality:
            for cell in args.cells:
                result = evaluate_quality(points, cell)
                quality.append(result)
                _print_quality([result])

        benchmark_cells, qualified, notice = select_benchmark_cells(
            quality,
            args.cells,
            args.min_internal_uniformity,
            args.min_point_count_variety,
            args.timing_cells,
        )
        if notice:
            print("\n" + notice)

        if not args.skip_join:
            assert polygon_parts is not None
            strategies = (
                ("broadcast", "grouped")
                if args.strategies == "both"
                else (args.strategies,)
            )
            for trial in range(1, args.repeats + 1):
                trial_cells = (
                    benchmark_cells if trial % 2 else tuple(reversed(benchmark_cells))
                )
                for cell in trial_cells:
                    for strategy in strategies:
                        if strategy == "broadcast":
                            result = _time_broadcast_join(
                                points,
                                polygon_parts,
                                cell,
                                trial,
                                args.dist,
                                args.wkid,
                            )
                        else:
                            result = _time_grouped_join(
                                points,
                                polygon_parts,
                                cell,
                                trial,
                                args.partitions,
                                args.dist,
                                args.wkid,
                            )
                        timings.append(result)
                        _print_timings([result])
            if timings:
                expected_matches = timings[0].matched_pairs
                if any(result.matched_pairs != expected_matches for result in timings):
                    raise RuntimeError("candidate cells produced different join counts")
                grouped_times: dict[tuple[float, str], list[float]] = {}
                for result in timings:
                    grouped_times.setdefault((result.cell, result.strategy), []).append(
                        result.total_seconds
                    )
                (cell, strategy), totals = min(
                    grouped_times.items(),
                    key=lambda item: statistics.median(item[1]),
                )
                recommendation = Recommendation(
                    cell=cell,
                    strategy=strategy,
                    trials=len(totals),
                    median_total_seconds=statistics.median(totals),
                    meets_quality_minima=qualified,
                )
                print(
                    f"\nRecommended cell: {recommendation.cell:g} degrees "
                    f"using {recommendation.strategy} "
                    f"({recommendation.median_total_seconds:.2f} seconds median "
                    f"over {recommendation.trials} trial(s))."
                    + ("" if qualified else " [did NOT meet the quality minima]")
                )

        if args.output:
            _write_results(args.output, quality, timings, recommendation)
        points.unpersist()
        if polygon_parts is not None:
            polygon_parts.unpersist()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
