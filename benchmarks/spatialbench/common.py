"""Query geometry and result summaries shared by bench.py and bench_qr.py.

Both scripts must use the same constants and the same rounding, or their results stop being
comparable across engines and layouts.
"""
CENTER_XY = (-111.7610, 34.8697)  # Sedona: Q1
BOX_ENV = (-111.9060, 34.7347, -111.6160, 35.0047)  # Q3 box (xmin, ymin, xmax, ymax)
BOX6_ENV = (-112.2110, 34.4197, -111.3110, 35.3197)  # Q6 box


def box_wkt(xmin, ymin, xmax, ymax):
    return (f"POLYGON(({xmin:.4f} {ymin:.4f}, {xmax:.4f} {ymin:.4f}, {xmax:.4f} {ymax:.4f}, "
            f"{xmin:.4f} {ymax:.4f}, {xmin:.4f} {ymin:.4f}))")


CENTER = f"POINT ({CENTER_XY[0]:.4f} {CENTER_XY[1]:.4f})"
BOX = box_wkt(*BOX_ENV)
BOX6 = box_wkt(*BOX6_ENV)


def _top_counts(rows):
    return [(r[0], r[2]) for r in rows]


# Comparable form of each query's result: ints and strings as-is, floats and durations rounded.
# Q6's avg_duration is rounded to 0.01 s; Spark truncates an interval average to whole
# microseconds while DuckDB rounds, so a value on a rounding boundary can still differ.
SUMMARY = {
    "Q1": lambda r: [x[0] for x in r],
    "Q3": lambda r: [(str(m)[:7], n, round(float(d), 9), round(dur.total_seconds(), 3), round(float(f), 9))
                     for m, n, d, dur, f in r],
    "Q4": _top_counts,
    "Q5": lambda r: [(x[0], str(x[2])[:7], round(float(x[3]), 6), x[4]) for x in r],
    "Q6": lambda r: [(x[0], x[2], round(float(x[3]), 9), round(x[4].total_seconds(), 2)) for x in r],
    "Q7": lambda r: [(x[0], float(x[1]), round(x[2], 6), float(f"{x[3]:.12g}")) for x in r],
    "Q8": _top_counts,
    "Q9": lambda r: [(x[0], x[1], round(x[5], 9)) for x in r],
    "Q12": lambda r: [(x[0], round(x[1], 9)) for x in r],
    "Q10": lambda r: [(x[0], None if x[2] is None else round(x[2].total_seconds(), 2),
                       None if x[3] is None else round(float(x[3]), 9), x[4]) for x in r],
}


def summarize(name, rows):
    """SUMMARY for a query, or the single scalar of a one-value result (scan, Q2)."""
    return SUMMARY.get(name, lambda r: r[0][0])(rows)
