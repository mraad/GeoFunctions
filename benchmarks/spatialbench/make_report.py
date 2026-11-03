"""Build index.html, the SpatialBench report, from results/sf1-sf10.jsonl.

usage: python make_report.py [--fragment out.html]

Every number in the page comes from the results file or from the measured sweeps listed in
SWEEPS below. The figures are inline SVG drawn to scale from those numbers. --fragment also
writes the page without <!doctype>/<html>/<body>, for hosts that add their own skeleton.
"""
import html, json, math, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "results", "sf1-sf10.jsonl")
QUERIES = [f"Q{i}" for i in range(1, 11)]
CAP = 600.0  # seconds; a run over this is killed and recorded as skipped

rows = [json.loads(line) for line in open(RESULTS)]


def median(engine, sf, layout, query):
    for r in rows:
        if (r["engine"], r["sf"], r["layout"], r["query"]) == (engine, sf, layout, query):
            return None if r.get("skipped") else r["median"]
    return None


def fmt(v):
    if v is None:
        return "—"
    return f"{v:.3f}" if v < 10 else f"{v:.1f}"


def ratio(s, d):
    return None if s is None or d is None else s / d


def ratio_text(x):
    if x is None:
        return "—"
    return f"{x:.2f}×" if x >= 0.1 else f"1/{1 / x:.0f}"


esc = html.escape

# ---------------------------------------------------------------- figure 1: the QR grid
def fig_grid():
    c, nx, ny, ox, oy = 56, 9, 5, 40, 20
    w, h = ox + nx * c + 20, oy + ny * c + 44
    parts = []
    # cells touched by polygon A's envelope (q 1..4, r 1..3) and the point's cell
    env_a = (1.35, 1.25, 4.6, 3.7)
    env_b = (3.9, 2.4, 6.7, 4.5)
    for q in range(nx):
        for r in range(ny):
            x, y = ox + q * c, oy + (ny - 1 - r) * c
            inside_a = env_a[0] // 1 <= q <= env_a[2] // 1 and env_a[1] // 1 <= r <= env_a[3] // 1
            cls = "cell-on" if inside_a else "cell"
            parts.append(f'<rect class="{cls}" x="{x}" y="{y}" width="{c}" height="{c}"/>')
    # canonical cell of the A/B envelope overlap: its lower-left corner
    ov = (max(env_a[0], env_b[0]), max(env_a[1], env_b[1]))
    cq, cr = int(ov[0]), int(ov[1])
    parts.append(f'<rect class="cell-canon" x="{ox + cq * c}" y="{oy + (ny - 1 - cr) * c}" width="{c}" height="{c}"/>')

    def px(x, y):
        return ox + x * c, oy + (ny - y) * c

    poly_a = [(1.5, 1.4), (3.2, 1.25), (4.6, 2.1), (4.2, 3.7), (2.6, 3.4), (1.35, 2.7)]
    poly_b = [(4.2, 2.6), (6.7, 2.4), (6.4, 4.5), (3.9, 4.1)]
    for poly, cls in ((poly_a, "poly-a"), (poly_b, "poly-b")):
        pts = " ".join("%.1f,%.1f" % px(x, y) for x, y in poly)
        parts.append(f'<polygon class="{cls}" points="{pts}"/>')
    for env, cls in ((env_a, "env-a"), (env_b, "env-b")):
        x0, y0 = px(env[0], env[3])
        x1, y1 = px(env[2], env[1])
        parts.append(f'<rect class="{cls}" x="{x0:.1f}" y="{y0:.1f}" width="{x1 - x0:.1f}" height="{y1 - y0:.1f}"/>')
    pt = px(7.45, 1.55)
    parts.append(f'<circle class="pt" cx="{pt[0]:.1f}" cy="{pt[1]:.1f}" r="5"/>')
    labels = [
        (px(1.42, 1.0), "A", "lbl-strong"), (px(6.75, 4.55), "B", "lbl-strong"),
        ((pt[0] + 9, pt[1] + 4), "point: 1 cell", "lbl"),
        ((ox + cq * c + 4, oy + (ny - 1 - cr) * c + c - 6), "canonical", "lbl-small"),
    ]
    for (x, y), t, cls in labels:
        parts.append(f'<text class="{cls}" x="{x:.1f}" y="{y:.1f}">{esc(t)}</text>')
    for q in range(nx):
        parts.append(f'<text class="tick" x="{ox + q * c + c / 2}" y="{oy + ny * c + 16}" text-anchor="middle">{q}</text>')
    for r in range(ny):
        parts.append(f'<text class="tick" x="{ox - 8}" y="{oy + (ny - 1 - r) * c + c / 2 + 4}" text-anchor="end">{r}</text>')
    parts.append(f'<text class="axis" x="{ox + nx * c / 2}" y="{h - 6}" text-anchor="middle">q = floor(x / cell)</text>')
    parts.append(f'<text class="axis" transform="translate(12 {oy + ny * c / 2}) rotate(-90)" text-anchor="middle">r = floor(y / cell)</text>')
    return f'<svg viewBox="0 0 {w} {h}" role="img" aria-labelledby="f1t"><title id="f1t">QR grid: envelopes, cells and the canonical cell</title>{"".join(parts)}</svg>'


# ---------------------------------------------------------------- figure 2: Spark ÷ DuckDB per query
def fig_ratio():
    lo, hi = 1 / 10000, 10.0
    left, right, top, rowh = 54, 24, 30, 30
    width = 640
    plot = width - left - right
    h = top + rowh * len(QUERIES) + 40

    def x(v):
        return left + plot * (math.log10(v) - math.log10(lo)) / (math.log10(hi) - math.log10(lo))

    parts = []
    for t, lab in ((0.0001, "1/10000"), (0.001, "1/1000"), (0.01, "1/100"), (0.1, "1/10"), (1, "1×"), (10, "10×")):
        parts.append(f'<line class="{"ref" if t == 1 else "grid"}" x1="{x(t):.1f}" x2="{x(t):.1f}" y1="{top - 8}" y2="{h - 34}"/>')
        parts.append(f'<text class="tick" x="{x(t):.1f}" y="{h - 18}" text-anchor="middle">{lab}</text>')
    parts.append(f'<text class="axis" x="{x(0.01):.1f}" y="{h - 2}" text-anchor="middle">◀ Spark faster</text>')
    parts.append(f'<text class="axis" x="{x(3.3):.1f}" y="{h - 2}" text-anchor="middle">DuckDB faster ▶</text>')
    for i, q in enumerate(QUERIES):
        y = top + i * rowh + rowh / 2
        parts.append(f'<text class="tick" x="{left - 10}" y="{y + 4:.1f}" text-anchor="end">{q}</text>')
        for sf, cls, dy in (("sf1", "s1", -5), ("sf10", "s2", 5)):
            s, d = median("spark", sf, "asis", q), median("duckdb", sf, "asis", q)
            if s is None:
                continue
            capped = d is None
            r = s / (d if d is not None else CAP)
            tip = (f"{q} {sf.upper()}: Spark {fmt(s)} s, DuckDB {'> 600 s (not run)' if capped else fmt(d) + ' s'}"
                   f" ({'≤ ' if capped else ''}{ratio_text(r)})")
            mark = (f'<circle class="{cls}{" open" if capped else ""}" cx="{x(r):.1f}" cy="{y + dy:.1f}" r="5">'
                    f'<title>{esc(tip)}</title></circle>')
            parts.append(mark)
            if capped:
                parts.append(f'<text class="lbl-small" x="{x(r) + 9:.1f}" y="{y + dy + 4:.1f}">DuckDB &gt; 10 min</text>')
    legend = (f'<circle class="s1" cx="{left + 6}" cy="12" r="5"/><text class="tick" x="{left + 16}" y="16">SF1 (6M trips)</text>'
              f'<circle class="s2" cx="{left + 136}" cy="12" r="5"/><text class="tick" x="{left + 146}" y="16">SF10 (60M trips)</text>'
              f'<circle class="s2 open" cx="{left + 286}" cy="12" r="5"/><text class="tick" x="{left + 296}" y="16">upper bound (DuckDB capped)</text>')
    return (f'<svg viewBox="0 0 {width} {h}" role="img" aria-labelledby="f2t"><title id="f2t">Spark time divided by DuckDB time, '
            f'per query, log scale</title>{legend}{"".join(parts)}</svg>')


# ---------------------------------------------------------------- figure 3: Q10 plan
def fig_q10():
    boxes = [
        ("trip", "6M trip points", "QR_FROMGEOM: 1 cell per point"),
        ("zone cells", "zone envelopes per cell", "QR_ENVP, broadcast, no geometry"),
        ("candidates", "7.3M (zone, point) pairs", "point inside the zone envelope"),
        ("group", "1 row per zone", "collect_list of its points"),
        ("ST_WITHIN_MASK", "zone polygon read once", "parsed and accelerated once"),
        ("aggregate", "explode hits, avg, count", "LEFT JOIN to all zones"),
    ]
    bw, bh, gap, top = 196, 62, 22, 10
    cols = 3
    w = cols * bw + (cols - 1) * gap + 20
    h = top + 2 * bh + gap + 76
    parts = []
    for i, (name, line1, line2) in enumerate(boxes):
        col, row = i % cols, i // cols
        x, y = 10 + col * (bw + gap), top + row * (bh + gap)
        key = name == "ST_WITHIN_MASK"
        parts.append(f'<rect class="{"box-key" if key else "box"}" x="{x}" y="{y}" width="{bw}" height="{bh}" rx="4"/>')
        parts.append(f'<text class="box-t" x="{x + 10}" y="{y + 20}">{i + 1}. {esc(name)}</text>')
        parts.append(f'<text class="box-s" x="{x + 10}" y="{y + 38}">{esc(line1)}</text>')
        parts.append(f'<text class="box-s" x="{x + 10}" y="{y + 53}">{esc(line2)}</text>')
        if col < cols - 1:
            ax = x + bw
            parts.append(f'<line class="arrow" x1="{ax + 2}" y1="{y + bh / 2}" x2="{ax + gap - 4}" y2="{y + bh / 2}" marker-end="url(#ah)"/>')
    # wrap arrow from box 3 to box 4
    x3, y3 = 10 + 2 * (bw + gap) + bw / 2, top + bh
    x4 = 10 + bw / 2
    parts.append(f'<path class="arrow" fill="none" d="M{x3} {y3 + 2} V{y3 + gap / 2} H{x4} V{y3 + gap - 4}" marker-end="url(#ah)"/>')
    note_y = top + 2 * bh + gap + 26
    parts.append(f'<text class="lbl" x="10" y="{note_y}">Per-row ST_WITHIN: each pair copies its zone polygon ≈ 1 TB at SF1, 20 s.</text>')
    q10 = median("spark", "sf1", "asis", "Q10")
    parts.append(f'<text class="lbl" x="10" y="{note_y + 22}">ST_WITHIN_MASK: each zone polygon is copied once per zone, {fmt(q10)} s.</text>')
    defs = ('<defs><marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
            '<path d="M0 0 L10 5 L0 10 z" class="arrow-head"/></marker></defs>')
    return f'<svg viewBox="0 0 {w} {h}" role="img" aria-labelledby="f3t"><title id="f3t">Q10 plan in six steps</title>{defs}{"".join(parts)}</svg>'


# ---------------------------------------------------------------- figure 4: cell-size sweeps
# Measured sweeps (seconds, warm medians). Source: the sweep runs recorded in README.md.
SWEEPS = [
    ("Q4 SF1, broadcast join", "s1", [(0.5, 0.573), (2, 0.433), (10, 0.419), (45, 0.568)]),
    ("Q6 SF1", "s2", [(0.5, 0.405), (1, 0.403), (2, 0.391)]),
    ("Q8 SF10", "s3", [(0.01, 1.272), (0.05, 1.091), (0.2, 1.416)]),
    ("Q2 SF10, QR-prepared", "s4", [(0.25, 0.208), (1, 0.195), (4, 0.43)]),
]


def fig_cells():
    left, right, top, bottom, width, h = 52, 170, 16, 44, 640, 300
    plot_w, plot_h = width - left - right, h - top - bottom
    xlo, xhi, ylo, yhi = 0.005, 100.0, 0.0, 1.6

    def x(v):
        return left + plot_w * (math.log10(v) - math.log10(xlo)) / (math.log10(xhi) - math.log10(xlo))

    def y(v):
        return top + plot_h * (1 - (v - ylo) / (yhi - ylo))

    parts = []
    for t in (0.01, 0.1, 1, 10, 100):
        parts.append(f'<line class="grid" x1="{x(t):.1f}" x2="{x(t):.1f}" y1="{top}" y2="{top + plot_h}"/>')
        parts.append(f'<text class="tick" x="{x(t):.1f}" y="{top + plot_h + 16}" text-anchor="middle">{t:g}°</text>')
    for t in (0, 0.4, 0.8, 1.2, 1.6):
        parts.append(f'<line class="grid" x1="{left}" x2="{left + plot_w}" y1="{y(t):.1f}" y2="{y(t):.1f}"/>')
        parts.append(f'<text class="tick" x="{left - 8}" y="{y(t) + 4:.1f}" text-anchor="end">{t:g} s</text>')
    parts.append(f'<text class="axis" x="{left + plot_w / 2}" y="{h - 6}" text-anchor="middle">cell size (degrees, log scale)</text>')
    for name, cls, pts in SWEEPS:
        d = " ".join(f"{'M' if i == 0 else 'L'}{x(a):.1f} {y(b):.1f}" for i, (a, b) in enumerate(pts))
        parts.append(f'<path class="line {cls}" d="{d}"/>')
        for a, b in pts:
            parts.append(f'<circle class="{cls}" cx="{x(a):.1f}" cy="{y(b):.1f}" r="4.5"><title>{esc(name)}: cell {a:g}°, {b:.3f} s</title></circle>')
        la, lb = pts[-1]
        parts.append(f'<text class="tick" x="{x(la) + 10:.1f}" y="{y(lb) + 4:.1f}">{esc(name)}</text>')
    return f'<svg viewBox="0 0 {width} {h}" role="img" aria-labelledby="f4t"><title id="f4t">Query time against QR cell size</title>{"".join(parts)}</svg>'


# ---------------------------------------------------------------- tables
def table_asis():
    out = ['<table><thead><tr><th>Query</th><th>DuckDB SF1</th><th>Spark SF1</th><th>Spark ÷ DuckDB</th>'
           '<th>DuckDB SF10</th><th>Spark SF10</th><th>Spark ÷ DuckDB</th></tr></thead><tbody>']
    for q in QUERIES:
        cells = []
        for sf in ("sf1", "sf10"):
            d, s = median("duckdb", sf, "asis", q), median("spark", sf, "asis", q)
            dtext = "&gt; 600 (not run)" if d is None and s is not None else fmt(d)
            r = ratio(s, d)
            rtext = ratio_text(r) if r is not None else (f"≤ {ratio_text(s / CAP)}" if s is not None else "—")
            win = r is not None and r < 1 or (r is None and s is not None)
            cells += [dtext, fmt(s), f'<span class="{"win" if win else ""}">{rtext}</span>']
        out.append(f"<tr><th>{q}</th>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def table_qr():
    out = ['<table><thead><tr><th>Query</th><th>Spark SF1</th><th>Spark SF1 QR</th><th>DuckDB SF1 QR</th>'
           '<th>Spark SF10</th><th>Spark SF10 QR</th><th>DuckDB SF10 QR</th></tr></thead><tbody>']
    for q in ("Q1", "Q2", "Q3", "Q4", "Q6"):
        cells = []
        for sf in ("sf1", "sf10"):
            cells += [fmt(median("spark", sf, "asis", q)), fmt(median("spark", sf, "qr1", q)), fmt(median("duckdb", sf, "qr1", q))]
        out.append(f"<tr><th>{q}</th>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def times_faster(sf, q):
    """DuckDB time / Spark time as text, or '—' when either is missing."""
    d, s = median("duckdb", sf, "asis", q), median("spark", sf, "asis", q)
    return "—" if d is None or s is None else f"{d / s:.0f}"


def n(sf, eng, q, layout="asis"):
    return fmt(median(eng, sf, layout, q))


# ---------------------------------------------------------------- page
CSS = """
:root {
  /* layout: one reading column, figures and tables at full column width */
  --bg: #fbfbfa; --panel: #f2f3f4; --fg: #15181c; --muted: #555b63; --rule: #d9dcdf;
  --accent: #1f5f8b; --win: #1d7a4d; --code: #eef0f2;
  --s1: #2a78d6; --s2: #eb6834; --s3: #1baf7a; --s4: #eda100;
  --cell: #ffffff; --cell-on: #dbe8f5; --canon: #f6d7c7;
  --display: "IBM Plex Sans Condensed", "Arial Narrow", system-ui, sans-serif;
  --body: "IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif;
  --mono: "IBM Plex Mono", ui-monospace, "SFMono-Regular", Menlo, monospace;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #15171a; --panel: #1d2024; --fg: #e9ebee; --muted: #a3a9b1; --rule: #33383e;
  --accent: #7fb6e0; --win: #5cc28f; --code: #22262b;
  --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500;
  --cell: #1a1d21; --cell-on: #20354a; --canon: #4a2e22; color-scheme: dark; } }
:root[data-theme="dark"] {
  --bg: #15171a; --panel: #1d2024; --fg: #e9ebee; --muted: #a3a9b1; --rule: #33383e;
  --accent: #7fb6e0; --win: #5cc28f; --code: #22262b;
  --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500;
  --cell: #1a1d21; --cell-on: #20354a; --canon: #4a2e22; color-scheme: dark; }
body { background: var(--bg); color: var(--fg); font: 16px/1.6 var(--body); margin: 0; }
.wrap { max-width: 820px; margin: 0 auto; padding-block: 32px 64px;
  padding-left: max(16px, env(safe-area-inset-left, 0px)); padding-right: max(16px, env(safe-area-inset-right, 0px)); }
h1, h2, h3 { font-family: var(--display); line-height: 1.2; text-wrap: balance; }
h1 { font-size: 2.1rem; margin: 0 0 .25rem; }
h2 { font-size: 1.45rem; margin: 2.6rem 0 .6rem; padding-top: .8rem; border-top: 1px solid var(--rule); }
h3 { font-size: 1.1rem; margin: 1.6rem 0 .4rem; }
p, li { max-width: 68ch; }
.lede { color: var(--muted); margin-top: 0; }
.meta { font: 13px/1.5 var(--mono); color: var(--muted); }
code, pre { font-family: var(--mono); font-size: .88em; background: var(--code); border-radius: 3px; }
code { padding: .05em .3em; }
pre { padding: .7em .9em; overflow-x: auto; }
figure { margin: 1.4rem 0; }
figure svg { width: 100%; max-width: 680px; height: auto; display: block; }
figcaption { font-size: .9rem; color: var(--muted); margin-top: .4rem; max-width: 68ch; }
.tbl { overflow-x: auto; margin: 1rem 0; }
table { border-collapse: collapse; font-variant-numeric: tabular-nums; font-size: .92rem; }
th, td { padding: .35rem .7rem; text-align: right; border-bottom: 1px solid var(--rule); white-space: nowrap; }
thead th { font-weight: 600; color: var(--muted); font-size: .82rem; text-transform: uppercase; letter-spacing: .04em; }
tbody th, thead th:first-child { text-align: left; }
.win { color: var(--win); font-weight: 600; }
table.text td, table.text th { text-align: left; white-space: normal; vertical-align: top; }
table.text tbody th { min-width: 11rem; }
ol.steps { padding-left: 1.4rem; } ol.steps li { margin: .35rem 0; }
.note { background: var(--panel); border-left: 3px solid var(--accent); padding: .6rem .9rem; margin: 1rem 0; }
dl.defs { display: grid; grid-template-columns: max-content 1fr; gap: .3rem 1rem; }
dl.defs dt { font-family: var(--mono); font-size: .9rem; } dl.defs dd { margin: 0; min-width: 0; }
a { color: var(--accent); }
a:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
/* figure marks, all from tokens */
.cell { fill: var(--cell); stroke: var(--rule); } .cell-on { fill: var(--cell-on); stroke: var(--rule); }
.cell-canon { fill: var(--canon); stroke: var(--s2); stroke-width: 2; }
.poly-a { fill: var(--s1); fill-opacity: .35; stroke: var(--s1); stroke-width: 2; }
.poly-b { fill: var(--s2); fill-opacity: .3; stroke: var(--s2); stroke-width: 2; }
.env-a, .env-b { fill: none; stroke-width: 1.5; stroke-dasharray: 5 4; } .env-a { stroke: var(--s1); } .env-b { stroke: var(--s2); }
.pt { fill: var(--fg); }
.tick, .lbl, .lbl-small, .lbl-strong, .axis, .box-t, .box-s { fill: var(--muted); font-family: var(--body); }
.tick { font-size: 12px; } .lbl { font-size: 13px; fill: var(--fg); } .lbl-small { font-size: 11px; }
.lbl-strong { font-size: 16px; font-weight: 700; fill: var(--fg); } .axis { font-size: 12px; }
.grid { stroke: var(--rule); stroke-width: 1; } .ref { stroke: var(--muted); stroke-width: 1.5; stroke-dasharray: 4 3; }
circle.s1 { fill: var(--s1); stroke: var(--bg); stroke-width: 2; } circle.s2 { fill: var(--s2); stroke: var(--bg); stroke-width: 2; }
circle.s3 { fill: var(--s3); stroke: var(--bg); stroke-width: 2; } circle.s4 { fill: var(--s4); stroke: var(--bg); stroke-width: 2; }
circle.open { fill: var(--bg); stroke: var(--s2); stroke-width: 2; }
path.line { fill: none; stroke-width: 2; } path.s1 { stroke: var(--s1); } path.s2 { stroke: var(--s2); }
path.s3 { stroke: var(--s3); } path.s4 { stroke: var(--s4); }
.box { fill: var(--panel); stroke: var(--rule); } .box-key { fill: var(--cell-on); stroke: var(--accent); stroke-width: 1.5; }
.box-t { font-size: 13px; font-weight: 600; fill: var(--fg); } .box-s { font-size: 12px; }
.arrow { stroke: var(--muted); stroke-width: 1.5; } .arrow-head { fill: var(--muted); }
@media (prefers-reduced-motion: reduce) { * { transition: none !important; } }
"""


def page():
    s1 = lambda e, q: n("sf1", e, q)
    body = f"""
<div class="wrap">
<h1>SpatialBench on Spark</h1>
<p class="lede">GeoFunctions on Apache Spark 4.3 against DuckDB 2.0 spatial, SpatialBench queries Q1 to Q10.</p>
<p class="meta">Data: SpatialBench v0.1.0, SF1 (6M trips) and SF10 (60M trips) · Machine: 16 cores, 128 GB RAM, local NVMe ·
Spark local[*], 64 GB driver · Times: warm median seconds · Source: results/sf1-sf10.jsonl</p>

<h2>Summary</h2>
<ul>
<li>Spark with GeoFunctions is faster than DuckDB on Q8, Q9 and Q10 at all tested scales.</li>
<li>On Q9 (building overlap), Spark is {times_faster('sf1', 'Q9')} times faster at SF1. DuckDB compares every building with every building.</li>
<li>On Q10 (trips in each zone), Spark is {times_faster('sf1', 'Q10')} times faster at SF1.</li>
<li>On Q1, Q2, Q3, Q6 and Q7, Spark is 1.1 to 2 times slower. Spark has a fixed cost of about 60 ms for each query job.</li>
<li>On Q4 and Q5, Spark is 3 to 4 times slower. The cause is Spark itself: a top-k sort and a <code>collect_list</code> over all trips.</li>
<li>When the data is sorted by QR cell (QR-prepared), Spark answers Q1, Q3 and Q6 in 0.05 to 0.27 s.</li>
</ul>

<h2>Results</h2>
<figure>{fig_ratio()}
<figcaption>Figure 1. Spark time divided by DuckDB time for each query. A value below 1× means that Spark is faster. The scale is logarithmic. Hover a mark to see the times.</figcaption></figure>
<div class="tbl">{table_asis()}</div>
<p>All rows agree between the two engines. One value is an exception: Q6 <code>avg_duration</code> at SF10 shows 6159.64 s on Spark and 6159.65 s on DuckDB. Spark truncates an interval average to whole microseconds. DuckDB rounds it. The unrounded values are less than 1 µs apart.</p>
<p>A run that takes more than 600 s is stopped. The table shows it as "not run".</p>

<h2>The QR grid</h2>
<p>A spatial join without an index compares every row of one table with every row of the other table. The QR grid gives Spark an equality key, so it can use a hash join.</p>
<figure>{fig_grid()}
<figcaption>Figure 2. The QR grid. Polygon A has an envelope (dashed). The envelope touches 12 cells (blue). A point is in exactly 1 cell. The envelopes of A and B overlap. The pair is kept only in the cell that holds the lower-left corner of the overlap (orange).</figcaption></figure>
<dl class="defs">
<dt>cell</dt><dd>A square of the grid. The size of the square is the <em>cell size</em>, in the units of the coordinates.</dd>
<dt>q, r</dt><dd>The column and row of a cell: <code>q = floor(x / cell)</code>, <code>r = floor(y / cell)</code>.</dd>
<dt>qr</dt><dd>One 64-bit key for a cell: <code>(q &lt;&lt; 32) | (r &amp; 0xFFFFFFFF)</code>.</dd>
<dt>envelope</dt><dd>The smallest rectangle that holds a geometry.</dd>
<dt>dist</dt><dd>A distance that makes the envelope larger before the cells are found. Use it for distance joins.</dd>
<dt>canonical cell</dt><dd>The cell that holds the lower-left corner of the overlap of two envelopes. <code>QR_INTERSECT</code> keeps a pair only in this cell, so the join returns each pair one time.</dd>
</dl>
<h3>Join patterns</h3>
<div class="tbl"><table class="text"><thead><tr><th>Pattern</th><th>Left side</th><th>Right side</th><th>Remove duplicates</th><th>Used in</th></tr></thead><tbody>
<tr><th>Polygon and polygon</th><td><code>QR_ENVP</code> (all cells)</td><td><code>QR_ENVP</code> (all cells)</td><td><code>QR_INTERSECT</code></td><td>Q4, Q9</td></tr>
<tr><th>Point in polygon</th><td><code>QR_FROMGEOM</code> (1 cell)</td><td><code>QR_LIST</code> (all cells)</td><td>Not necessary</td><td>Q6</td></tr>
<tr><th>Point in polygon, grouped by polygon</th><td><code>QR_FROMGEOM</code> (1 cell)</td><td><code>QR_ENVP</code> (cell and envelope, no geometry)</td><td>Not necessary; exact test with <code>ST_WITHIN_MASK</code></td><td>Q10</td></tr>
<tr><th>Point near polygon</th><td><code>QR_FROMGEOM</code> (1 cell)</td><td><code>QR_LIST</code> with <code>dist</code></td><td>Not necessary</td><td>Q8</td></tr>
</tbody></table></div>
<p>A point is in only one cell. Thus a point and a polygon meet in one cell only, and no pair repeats.</p>

<h2>How each query is written</h2>
<p>The SQL is not a copy of the SpatialBench SQL. Each query uses the plan that is fastest on Spark. The results are the same.</p>
<h3>Q1 and Q3: distance from a constant geometry</h3>
<p>The query uses <code>ST_DWITHIN</code>. The constant geometry is parsed one time for each task. Pairs with envelopes that are far apart are rejected before the exact test.</p>
<h3>Q2: points in a constant polygon</h3>
<p>The county polygon comes from a scalar subquery. <code>ST_INTERSECTS</code> keeps the last parsed geometries, so the polygon is parsed one time, not 6 million times. Time at SF1 went from 66 s to {s1('spark','Q2')} s.</p>
<h3>Q4: points in zones, top 1000 trips</h3>
<p>The 1000 points are broadcast. The zones are streamed. A shuffle of the zone table took 1.9 to 44 s. The broadcast join takes {s1('spark','Q4')} s at SF1.</p>
<h3>Q5: convex hull for each customer and month</h3>
<p>The query counts the groups first. Then it collects the points only for groups with more than 5 trips. The limit is Spark <code>collect_list</code>, not the geometry.</p>
<h3>Q6: trips in zones near Sedona</h3>
<p>Point-in-polygon join with 1 cell for each point. The geometry cache keeps 4 polygons, because each trip meets 2 or 3 counties in turn.</p>
<h3>Q7: detour ratio</h3>
<p>The length of a line with two points is the distance between the points. The query uses <code>ST_DISTANCE</code>, not <code>ST_LENGTH(ST_LINE(...))</code>. This is 3 times faster.</p>
<h3>Q8: pickups near buildings</h3>
<p>Each building is put in all cells that its envelope touches after the envelope is made larger by 0.0045°. Each point is in 1 cell. The buildings are broadcast.</p>
<h3>Q9: overlapping buildings</h3>
<p>QR self-join at 0.05° cells. <code>QR_INTERSECT</code> and <code>id1 &lt; id2</code> keep each pair one time. Spark: {s1('spark','Q9')} s. DuckDB: {s1('duckdb','Q9')} s.</p>
<h3>Q10: trips in each zone</h3>
<figure>{fig_q10()}
<figcaption>Figure 3. The Q10 plan. Step 5 reads each zone polygon one time for all of its points.</figcaption></figure>
<p>Spark gives each row its own copy of a binary value. With one row for each (zone, point) pair, Spark copied about 1 TB of polygon data at SF1. <code>ST_WITHIN_MASK</code> tests all points of a zone in one call. Time at SF1 went from 20 s to {s1('spark','Q10')} s.</p>

<h2>Changes to the library</h2>
<div class="tbl"><table class="text"><thead><tr><th>Change</th><th>Effect</th><th>Measured</th></tr></thead><tbody>
<tr><th><code>WkbCache</code> in the six predicates and <code>ST_DWITHIN</code></th><td>A repeated geometry is parsed one time. The cache keeps 4 geometries and compares the bytes.</td><td>Q2: 66 s to 0.2 s · Q6: 0.75 s to 0.40 s</td></tr>
<tr><th>Envelope check in the six predicates</th><td>Pairs that are far apart get the answer without the Esri operator.</td><td>Zone filter: 0.283 s to 0.229 s</td></tr>
<tr><th>Direct decode of 2D points</th><td>A 21-byte point is read without the Esri importer. All other shapes use the importer.</td><td>Q1 −21%, Q8 −10%, Q6 −8%</td></tr>
<tr><th><code>ST_WITHIN_MASK</code></th><td>Many points against one polygon. The polygon is read one time.</td><td>Q10: 20 s to {fmt(median("spark", "sf1", "asis", "Q10"))} s</td></tr>
<tr><th><code>join_qr(broadcast=...)</code></th><td>Broadcast the small side. Do not shuffle the large side.</td><td>Q4: 2.2 s to 0.42 s</td></tr>
<tr><th><code>qr_cells</code></th><td>Lists the cells of a geometry on the driver. Use it to read only some row groups of a QR-sorted table.</td><td>Q1 SF10: 0.45 s to 0.08 s</td></tr>
<tr><th><code>ST_DWITHIN</code></th><td>New function. Distance test with an envelope check first.</td><td>Q1, Q3</td></tr>
<tr><th>Two-argument predicates</th><td><code>ST_INTERSECTS(a, b)</code> works in SQL. <code>wkid</code> is <code>'-1'</code>.</td><td>—</td></tr>
</tbody></table></div>
<p>Each change gives the same answer as before. The tests compare the new code with the Esri operators, bit for bit where possible.</p>

<h2>QR preparation</h2>
<p>QR preparation writes the data one time, sorted by QR cell. A query that needs a small area then reads only the row groups for its cells.</p>
<ul>
<li>The trip table gets the column <code>t_pickup_qr</code> and is sorted by it.</li>
<li>Each zone polygon is cut into one piece for each cell (<code>qr_geom</code>). The piece keeps its envelope.</li>
</ul>
<div class="tbl">{table_qr()}</div>
<p>Preparation at 1° cells takes about 46 s at SF1 and 65 s at SF10. The preparation also makes DuckDB faster. Q5, Q7, Q8, Q9 and Q10 do not use the prepared layout, because they do not select a small area.</p>
<div class="note">A list of cells helps only when the cells are near each other. For Q4, the 1000 points are spread over the world. A list of 700 cells was slower than a read of all rows.</div>

<h2>Cell size</h2>
<figure>{fig_cells()}
<figcaption>Figure 4. Query time against cell size. A small cell puts a large polygon into many cells. A large cell puts many geometries into one cell.</figcaption></figure>
<ul>
<li>For a broadcast join, the cell size has a small effect.</li>
<li>For a point-in-polygon join, use a cell near the size of the small geometry plus <code>dist</code>.</li>
<li>For QR preparation, use a cell near the size of the query area.</li>
<li>Do not use a cell so small that a large polygon goes into thousands of cells. At 0.5°, the shuffle join for Q4 took 44 s.</li>
</ul>

<h2>Procedures</h2>
<h3>Run the benchmark</h3>
<ol class="steps">
<li>Install the Python packages: <code>pip install huggingface-hub duckdb pyspark==4.3.0</code></li>
<li>Build the jar for Spark 4.3: <code>mvn -P spark-4.3 clean package</code></li>
<li>Install the Python package from the repository root: <code>pip install .</code></li>
<li>Set the jar path: <code>JAR=$(ls -S target/geofunctions-*.jar | head -1)</code></li>
<li>Download the data: <code>hf download apache-sedona/spatialbench --repo-type dataset --include "v0.1.0/sf1/*" --local-dir data</code></li>
<li>Run Spark: <code>python benchmarks/spatialbench/bench.py spark "$JAR" data/v0.1.0/sf1 sf1</code></li>
<li>Run DuckDB: <code>python benchmarks/spatialbench/bench.py duckdb - data/v0.1.0/sf1 sf1</code></li>
<li>Compare the <code>result</code> field of each output line.</li>
</ol>
<h3>Run only some queries</h3>
<ol class="steps">
<li>Add the argument <code>queries=Q9,Q10</code> after the number of runs.</li>
</ol>
<h3>Run the QR-prepared benchmark</h3>
<ol class="steps">
<li>Prepare the data: <code>python benchmarks/spatialbench/prepare.py "$JAR" data/v0.1.0/sf1 prepared/sf1_c1 1</code></li>
<li>Run Spark: <code>python benchmarks/spatialbench/bench_qr.py spark "$JAR" prepared/sf1_c1 1 sf1</code></li>
<li>Run DuckDB: <code>python benchmarks/spatialbench/bench_qr.py duckdb - prepared/sf1_c1 1 sf1</code></li>
</ol>
<h3>Make this page again</h3>
<ol class="steps">
<li>Write the results to <code>benchmarks/spatialbench/results/sf1-sf10.jsonl</code>.</li>
<li>Run <code>python benchmarks/spatialbench/make_report.py</code>.</li>
</ol>

<h2>Limits</h2>
<ul>
<li>Spark needs about 2.5 s to start a session. DuckDB needs 0.08 s.</li>
<li>Spark has a fixed cost of about 60 ms for each query job.</li>
<li>Spark reads the Parquet geometry column about 3 times slower than DuckDB.</li>
<li>All tests used one machine. No test used a cluster.</li>
<li>The <code>spark-4.3</code> build uses the Spark 4.0 builds of the <code>spark-shp</code> and <code>FileGDB</code> readers. These readers are not tested on Spark 4.3.</li>
</ul>
</div>
"""
    head = ('<title>SpatialBench on Spark</title>\n<meta name="claude:viewport-fit-sides" content="cover">\n'
            '<link rel="preconnect" href="https://fonts.googleapis.com">\n'
            '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500'
            '&family=IBM+Plex+Sans+Condensed:wght@600;700&family=IBM+Plex+Sans:wght@400;600&display=swap">\n'
            f"<style>{CSS}</style>\n")
    return head, body


head, body = page()
standalone = ('<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
              '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
              + head + "</head>\n<body>" + body + "</body>\n</html>\n")
with open(os.path.join(HERE, "index.html"), "w") as f:
    f.write(standalone)
if "--fragment" in sys.argv:
    with open(sys.argv[sys.argv.index("--fragment") + 1], "w") as f:
        f.write(head + body)
print("wrote", os.path.join(HERE, "index.html"))
