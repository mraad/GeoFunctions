"""Regenerate Shapes.gdb, the GDBShapeSpec fixture, with GDAL's OpenFileGDB driver (GDAL >= 3.6).

usage: python make_shapes_gdb.py   (writes Shapes.gdb next to this script; needs ogr2ogr on PATH)

Sixteen layers, one feature each: point_*, mpoint_*, line_* and poly_* in the xy, xyz, xym and
xyzm layouts.
Vertex i carries z = 100 + i and m = 1000 + i, so a value read from the wrong slot can never pass.
- point: the single vertex (5, 7).
- mpoint: three points (1, 2), (3, 4), (5, 6).
- line: two paths; the second is closed in x/y (its last vertex meets its first) and must stay so.
- poly: an outer ring (CW) with a hole (CCW) plus a second part (CW); each ring's closing vertex
  repeats its first, as a polygon ring must.
"""
import os
import shutil
import subprocess
import tempfile

RINGS = [[(0, 0), (0, 10), (10, 10), (10, 0)], [(2, 2), (4, 2), (4, 4), (2, 4)], [(20, 0), (20, 5), (25, 5)]]
MPOINTS = [(1, 2), (3, 4), (5, 6)]
PATHS = [[(0, 0), (5, 5), (10, 0)], [(20, 0), (25, 5), (30, 0), (20, 0)]]
KINDS = ("xy", "xyz", "xym", "xyzm")
SUFFIX = {"xy": "", "xyz": "Z", "xym": "M", "xyzm": "ZM"}


def coord(kind: str, x: float, y: float, i: int) -> str:
    c = [x, y] + ([100 + i] if "z" in kind else []) + ([1000 + i] if "m" in kind else [])
    return " ".join(map(str, c))


def tag(kind: str) -> str:
    return f" {SUFFIX[kind]}" if SUFFIX[kind] else ""


def point_wkt(kind: str) -> str:
    return f"POINT{tag(kind)} ({coord(kind, 5, 7, 0)})"


def mpoint_wkt(kind: str) -> str:
    pts = ", ".join(f"({coord(kind, x, y, i)})" for i, (x, y) in enumerate(MPOINTS))
    return f"MULTIPOINT{tag(kind)} ({pts})"


def line_wkt(kind: str) -> str:
    i, paths = 0, []
    for path in PATHS:
        pts = []
        for x, y in path:
            pts.append(coord(kind, x, y, i))
            i += 1
        paths.append("(" + ", ".join(pts) + ")")
    return f"MULTILINESTRING{tag(kind)} ({', '.join(paths)})"


def poly_wkt(kind: str) -> str:
    i, rings = 0, []
    for ring in RINGS:
        pts = []
        for x, y in ring:
            pts.append(coord(kind, x, y, i))
            i += 1
        pts.append(pts[0])
        rings.append("(" + ", ".join(pts) + ")")
    return f"MULTIPOLYGON{tag(kind)} (({rings[0]}, {rings[1]}), ({rings[2]}))"


LAYERS = [(f"{name}_{kind}", f"{ogr}{SUFFIX[kind]}", make(kind))
          for name, ogr, make in (("point", "POINT", point_wkt), ("mpoint", "MULTIPOINT", mpoint_wkt),
                                  ("line", "MULTILINESTRING", line_wkt),
                                  ("poly", "MULTIPOLYGON", poly_wkt))
          for kind in KINDS]


def main() -> None:
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Shapes.gdb")
    with tempfile.TemporaryDirectory() as tmp:
        # Built aside and swapped in at the end, so a failed run never leaves the fixture deleted.
        new = os.path.join(tmp, "Shapes.gdb")
        for n, (layer, geom_type, wkt) in enumerate(LAYERS):
            csv = os.path.join(tmp, f"{layer}.csv")
            with open(csv, "w") as f:
                f.write(f'id,WKT\n1,"{wkt}"\n')
            # The first call creates the geodatabase; -update adds each later layer to it.
            subprocess.run(["ogr2ogr", "-f", "OpenFileGDB", *(["-update"] if n else []), new, csv,
                            "-nln", layer, "-nlt", geom_type,
                            "-oo", "GEOM_POSSIBLE_NAMES=WKT", "-oo", "KEEP_GEOM_COLUMNS=NO"], check=True)
        # Copied next to the fixture first, so only renames can fail once the old one moves.
        aside, old = out + ".new", out + ".old"
        shutil.rmtree(aside, ignore_errors=True)
        shutil.rmtree(old, ignore_errors=True)
        shutil.copytree(new, aside)
        os.replace(out, old)
        try:
            os.replace(aside, out)
        except OSError:
            os.replace(old, out)
            raise
        shutil.rmtree(old)
    print("wrote", out)


if __name__ == "__main__":
    main()
