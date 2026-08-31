"""Checks for the cell-selection logic behind the recommendation.

Runnable without Spark: `python benchmarks/test_optimize_qr_join.py`.
The point of these is the `meets_quality_minima` flag — the recommendation is
picked on median time alone, so the flag is the only thing separating a cell that
cleared the quality floor from one that never did.
"""

import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from optimize_qr_join import QualityResult, Recommendation, select_benchmark_cells


def q(cell: float, iu: float, pcv: float) -> QualityResult:
    return QualityResult(
        cell=cell,
        evaluated_points=100,
        total_bins=10,
        nonempty_bins=8,
        min_count=1,
        max_count=20,
        internal_uniformity=iu,
        point_count_variety=pcv,
        quality_score=iu * pcv,
        seconds=0.1,
    )


def test_passing_cells_are_kept_and_marked_qualified():
    cells, qualified, notice = select_benchmark_cells(
        [q(1.0, 0.9, 0.9), q(2.0, 0.1, 0.1)], (1.0, 2.0), 0.5, 0.5, None
    )
    assert cells == (1.0,), cells
    assert qualified is True
    assert "at or above" in notice


def test_fallback_is_marked_not_qualified():
    cells, qualified, notice = select_benchmark_cells(
        [q(1.0, 0.2, 0.2), q(2.0, 0.3, 0.3)], (1.0, 2.0), 0.9, 0.9, None
    )
    assert cells == (2.0,), cells          # best quality_score of a failing set
    assert qualified is False               # the bug: this used to read as qualifying
    assert "failed the quality floor" in notice


def test_explicit_timing_cells_bypass_the_filter_and_are_not_qualified():
    cells, qualified, notice = select_benchmark_cells(
        [q(1.0, 0.9, 0.9)], (1.0,), 0.5, 0.5, (7.0, 8.0)
    )
    assert cells == (7.0, 8.0), cells
    assert qualified is False
    assert "bypass the quality filter" in notice


def test_skip_quality_leaves_cells_untouched_but_unqualified():
    # --skip-quality means nothing was measured. Unmeasured is not "meets the floor":
    # reporting True here would put a qualifying stamp on cells never evaluated.
    cells, qualified, notice = select_benchmark_cells([], (1.0, 2.0), 0.5, 0.5, None)
    assert cells == (1.0, 2.0), cells
    assert qualified is False
    assert notice is None


def test_flag_reaches_the_json_output():
    rec = Recommendation(
        cell=2.0, strategy="broadcast", trials=3,
        median_total_seconds=1.5, meets_quality_minima=False,
    )
    assert asdict(rec)["meets_quality_minima"] is False
    assert asdict(Recommendation(1.0, "grouped", 1, 1.0))["meets_quality_minima"] is True


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("\nall checks passed")
