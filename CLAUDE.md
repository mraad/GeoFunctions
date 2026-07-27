# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & package

This is a hybrid Scala + Python project. The Scala side compiles to a shaded fat JAR loaded by Spark; the Python side is a py4j wrapper (plus a handful of pure-Python helpers) distributed as a wheel. Both must be rebuilt together — `gf.sh` is the canonical build:

```shell
./gf.sh        # mvn clean package + pip wheel + zip release bundle
./pw.sh        # same minus the zip (faster local dev loop)
```

Both scripts `cd` to their own directory, so they run from any working directory, and every input they use is inside the repo — `toolbox/` (the ArcGIS `.pyt`/`.lyrx` assets, previously pulled from `$HOME`), `data/` (`world.zip`, `Miami.gdb.zip`), `notebooks/`. **This repo lives on an external drive whose mount name changes; never hardcode an absolute path anywhere, including in scripts, notebooks, or the venv.** `gf.sh` also needs the `data/` zips present, which are gitignored — use `./pw.sh` for the inner dev loop.

Run either through the repo venv (`source .venv/bin/activate`); both call `python3`, which must be the venv's. The venv is relocatable — the `activate` scripts derive `VIRTUAL_ENV` from their own location and the console scripts use a `#!/bin/sh` + `realpath` shebang wrapper instead of an absolute interpreter path — so renaming the volume does not break it. If you ever recreate the venv, use `uv venv --relocatable`, otherwise the absolute paths come back.

Internals worth knowing:
- `pom_to_pyproject.py` runs in the Maven `validate` phase and copies `<version>` from `pom.xml` into `pyproject.toml`. **Always edit the version in `pom.xml` only** — the pyproject value is overwritten on every build.
- The default Maven build uses `<scope>provided</scope>` for `spark-core`/`spark-sql` (assumes a host Spark runtime). To produce a JAR that can run standalone, activate `-P spark-scope-compile`.
- Spark profiles: `spark-3.3`, `spark-3.4`, `spark-3.5` (default — Spark 3.5.9, Java 11, Scala 2.12.21), `spark-4.0` (Java 17 + Scala 2.13). Activate with `mvn -P spark-4.0 package`.
- **`spark-4.0` does not build.** Four `com.esri` deps have no Scala 2.13 artifact published — `webmercator`, `grid-hex`, `spark-shp`, `filegdb` (the local `~/.m2` entries for these are `.lastUpdated` failure markers, not jars). `WebMercator/` and `grid-hex/` in the workspace have `scala-2.13` profiles and could be built locally; `spark-shp/` has no `spark-4.0` profile; `filegdb` has no source in the workspace at all. On top of that, `QRScan` and `JoinQRInnerProcessor` use `.par`, which needs `scala-parallel-collections` on 2.13 — not currently a dependency. Don't attempt the profile until all five are resolved.
- Tests are **skipped by default** (`<skipTests>true</skipTests>` property). Surefire cannot discover scalatest specs, so `scalatest-maven-plugin` runs them: `mvn -DskipTests=false test`, or one suite with `mvn -DskipTests=false -Dsuites=com.esri.spark.ClipLineDistSpec test`.
- The shade plugin aggressively excludes ~15 transitive groups (jaxb, slf4j, log4j, scala-lang, geosolutions, ehcache, etc.) — when adding a dependency that pulls one of these, expect runtime `ClassNotFoundException` unless you remove the exclusion or relocate.
- `.gitignore` ignores `*.sh`, `*.xml`, `*.zip`, `data/`, `docs/`. `pom.xml` and `gf.sh` are tracked only because they were force-added; `pw.sh` is untracked. A new script or XML file needs `git add -f` or it silently never gets committed.
- `environment.yml` is stale (`python<3.10`, `pyspark==3.5.1`) and contradicts `pyproject.toml` (`requires-python = ">3.10"`). Trust `pyproject.toml`/`pom.xml`.

## Runtime target

This library is built to run inside an **ArcGIS Pro conda environment** (`arcgispro-py3` clone) with the [`spark-esri`](https://github.com/mraad/spark-esri) package providing the SparkSession. Per README, **Pro 3.5 is the supported ceiling — Pro 3.6 is currently broken**. The notebooks under `notebooks/` are the primary usage examples and assume this environment.

Standalone PySpark also works: `pyspark==3.5.9` (non-Windows) or `3.5.4` (Windows) is the only required runtime dep beyond the JAR. `.venv/` in the repo root has a working 3.11 + pyspark 3.5.7 for this — the JAR is `provided`-scope against Spark, so it runs on any 3.5.x.

To exercise the library outside ArcGIS Pro, the shaded JAR bundles `com.esri:filegdb`, which registers a `gdb` Spark data source (options: `path`, `name`, `numPartitions`, `wkid`) — so a File Geodatabase reads directly:

```python
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-class-path {JAR} --jars {JAR} pyspark-shell"  # py4j needs the system classloader
df = spark.read.format("gdb").option("path", "X.gdb").option("name", "LayerName").load()
```

Points come back as `Shape: struct<x, y>`, lines/polygons as `Shape: struct<xmin, ymin, xmax, ymax, parts, coords>` — feed the latter to `gdb_polyline2`/`gdb_polygon2` to get WKB. H3 is `provided`, so H3 functions need the Uber h3 JAR added separately.

## Architecture

The whole library is **Spark Catalyst expressions wrapped twice**:

1. **Scala expressions** (`src/main/scala/com/esri/spark/*.scala`) — one file per function. Each extends `Expression with ImplicitCastInputTypes`, declares `inputTypes`/`dataType`, and implements both an interpreted `eval(InternalRow)` and a codegen `doGenCode(...)`. The codegen path delegates to a `<Name>Obj` companion `object` so the generated Java can call `STPointObj.eval(x, y)` directly — keep this pattern when adding new functions, otherwise codegen-enabled queries will fall back to interpretation.
2. **Geometry serialization**: geometries flow through Spark as `BinaryType` WKB. The `package object spark` provides `geom.bytes` and `bytes.geom` implicits that wrap `OperatorExportToWkb` / `OperatorImportFromWkb` from `esri-geometry-api`. Always use these — they enforce `ShapeImportDefaults`/`ShapeExportDefaults` consistently across functions.
3. **Two registration surfaces** for the Scala layer:
   - `Registry.registerFunctions()` — registers expressions as SQL temp functions (`ST_POINT`, `QR_CLIP`, `H3_LATLNGTOCELL`, …). Add new expressions to this list or they will not be callable from SQL. **The SQL surface is neither complete nor name-consistent**: names are not mechanically derived from class names (`QRGeom` → `QR_CLIP`), `ClipLine`/`ClipLineDist`/`GDBPolylineM`/`GDBPolylineZ` are commented out, and `QR_ENVPGROM` is a live typo for `QR_ENVPGEOM` — don't rename it silently, callers may depend on it.
   - **Axis-symmetric reuse is intentional, not a bug**: the QR grid math is identical on both axes, so `ST_RTOY`/`st_rtoy` are wired to `STQToX` and `st_ytor` to `stXToQ`. There is no `STRToY.scala` or `STYToR.scala` — don't add one.
   - `GeoFunctions` object — exposes typed `Column => Column` wrappers for the DataFrame API (~71 defs). Add a wrapper here so the Python layer can invoke it.
4. **Python layer** (`src/main/python/geofunctions/__init__.py`, single 84 KB file, ~88 functions) — most functions call `SparkContext._active_spark_context._jvm.com.esri.spark.GeoFunctions.<camelCaseName>(...)` via py4j and return a `Column` with a sensible `.alias(...)`. They do **not** use the SQL registry; they go directly through the Scala `GeoFunctions` object. `st_register_functions()` is the only Python function that touches `Registry`.

**`wkid` convention**: functions that need a spatial reference take `wkid` as a *string* Catalyst arg, resolved by `SpatialReferenceObj.create`. `"-1"` means **null spatial reference** (planar math, no projection); `"4326"`/`"EPSG:4326"`/`3857`/`102008` are pre-cached, anything else is created on demand into a `TrieMap`. Python defaults vary per function — check before assuming `-1`.

Adding a new spatial function therefore requires four coordinated edits:
1. New `STFoo.scala` (or `QRFoo`, `H3Foo`) following the existing expression+companion-object pattern.
2. Append to `Registry.registerFunctions()` to expose it via SQL.
3. Append a `def stFoo(...)` to `GeoFunctions.scala` for the DataFrame API.
4. Append a `def st_foo(...)` to `src/main/python/geofunctions/__init__.py` that calls `_jvm.com.esri.spark.GeoFunctions.stFoo(...)`, with the reST-style docstring (`:param:`/`:type:`/`:return:`/`Example::`) the rest of the file uses.

Then update `functions.md` and the README function list — nothing generates them.

## Python-only layer

Roughly a dozen Python functions have **no Scala counterpart** — don't go looking for one:

- `*_explode` (`qr_geom_explode`, `qr_envp_explode`, `qr_envp_geom_explode`, `qr_list_explode`, `st_dump_explode`) — wrap `explode()` around the array-returning function and re-`alias`. This is the idiom for consuming any `ArrayType` expression; use it rather than open-coding `explode`.
- `join_qr(lhs, rhs, cell, ...)` — the spatial-join entry point, with **two dispatch paths**: if neither side already has a `qr` column it builds a pure-DataFrame join (`qr_envp_explode` on both sides → equi-join on `qr` → `qr_intersect` filter); if a `qr` column exists it drops into `JoinQRInnerProcessor.apply(...)` over the raw `_jdf`.
- **arcpy interop** (`to_spark`, `to_feature_class`, `to_feature_table`) — bridge via pyarrow. `to_spark` uses `arcpy.da.TableToArrowTable(..., "WKB")`; the write side stamps `{"esri.encoding": "WKB", "esri.sr_wkt": ...}` field metadata on the `SHAPE` column before `arcpy.management.CopyFeatures`. `import arcpy` is deliberately function-local so the module imports outside ArcGIS Pro.
- `pairwise_dissolve(...)` — progressive multi-resolution dissolve. Each pass: clip by inflated QR envelope → `collect_list` per QR → `st_union_col` → `st_dump_explode` → `st_exterior_ring` → flag with `qr_contains_geom`. Rows fully inside a QR are unioned into the result; the rest re-enter the loop at `cell *= cell_mul` until empty or `breaker` passes. Every intermediate is `localCheckpoint()`ed to truncate the lineage — the loop will blow up query-plan size if you remove those.

At import time the module **monkey-patches `pyspark.sql.DataFrame`** with `pairwise_dissolve`, `join_qr`, `to_feature_table`, and `to_feature_class` (bottom of the file), so notebooks call `df.join_qr(...)` directly. Keep new DataFrame-level helpers registered there.

## Other notable pieces

- `JoinQRInnerProcessor.scala` + the `QR*` family implement quad-region (QR) spatial indexing — used for broadcast-join-style spatial joins. `QREnvpGeom`, `QRGeom`, and `QRContainsGeom` are the load-bearing ones.
- **`dist` inflates the cell, and every candidate range must account for it.** A cell `(q,r)` spans `[q*cell-dist, (q+1)*cell+dist]` on each axis, so the q/r search range is the geometry extent **inflated by dist** — `floor((xmin-dist)/cell) .. floor((xmax+dist)/cell)`. `QRCount`, `QRList`, `QREnvp`, `QRScan` and `ClipLineDist` all follow this; a new function that uses the raw extent will silently drop boundary cells whenever `dist > 0`. `QRScanSpec` pins the family to agreeing on the cell set.
- `QRScan.scala` is the single cell scan behind both `QR_GEOM` and `QR_ENVPGEOM` — walk the range, intersect the geometry with a reused scratch `Envelope` per cell, emit via a caller-supplied row builder. The two expressions were near-identical 200+ line copies before; keep new variants as row builders over `QRScan.eval`, not as new scans. Do not accelerate the per-cell envelope (it is a rectangle — pure overhead); accelerate the input geometry once, which `QRScan` already does.
- Executor-side config must come from `SparkEnv.get.conf`, **not** `SparkSession.getActiveSession` — the latter is empty on executors, which is where these expressions run. `spark.esri.accelerate` (default true) and `spark.esri.parallel` (default 64, the cell count above which one geometry's scan fans out over the parallel collection pool) were both dead knobs until they were moved.
- `ClipLine.scala` / `ClipLineDist.scala` take **one segment** as `array(x1,y1,x2,y2)` (not a polyline) and return one row per `(q, r, length)` cell it crosses — pair with `QR*` for line-vs-cell joins. `ClipLine` is a one-line delegate to `ClipLineDistObj.eval(.., 0.0)`. Two invariants any change must preserve, both covered by `ClipLineDistSpec`: with `dist = 0` the emitted lengths sum exactly to the segment length, and the emitted cell set equals a brute-force scan of the `dist`-inflated bbox. Do **not** hoist a shared `UnsafeRowWriter` out of the emit loop — `resetRowWriter()` does not rewind the cursor, so every row silently becomes a copy of the first (this shipped broken in `c66232c`). Build rows with `InternalRow(q, r, l)`, as `QREnvpObj` does.
- `GDBPolygon2.scala`, `GDBPolygonM.scala`, `GDBPolygonZ.scala`, `GDBPolyline2.scala` convert the `Shape` **struct** the `gdb`/`shp` readers return — `(xmin, ymin, xmax, ymax, parts: array<int>, coords: array<double>)` — into WKB. They index the struct **positionally** (`row.getArray(4)`/`row.getArray(5)`), so a reader schema change breaks them silently.
- `STDump.scala` explodes a multi-geometry into one row per part — generator expression, follows a slightly different codegen path than the scalar ones.
- `H3Instance.scala` lazily holds a single `H3Core` instance (Uber H3 4.4.0) and nothing else. The H3 dep is `provided` in the POM — the host Spark/ArcGIS env must supply it (or switch to compile scope locally). To exercise H3 outside ArcGIS, put `~/.m2/repository/com/uber/h3/*/h3-*.jar` on the driver classpath alongside the shaded JAR.
- **Null inputs are the caller's responsibility — never add per-row null checks.** By design a GeoFunction is never invoked with a null argument; the caller filters upstream. That is why `doGenCode` sets `isNull = FalseLiteral` and interpreted `eval` casts straight through with `asInstanceOf`, uniformly across all 68 expressions. **This is deliberate, not an oversight — do not "fix" it.** A null guard puts a branch on every row of every function for a case that cannot happen. There are no null-input tests, and adding one would encode the wrong contract.

  What null input actually does, if you are debugging a report of nonsense output: nothing throws, the value is silently fabricated. Interpreted `eval` unboxes null to `0.0`/`0L`; codegen substitutes `CodeGenerator.defaultValue` (`-1.0` for `DoubleType`, `-1L` for `LongType`), so the two paths disagree. `ST_POINT(NULL, 2.0)` yields `POINT (-1 2)` under codegen; `H3_LATLNGTOCELL(NULL, lng, 7)` yields the cell on the equator. Garbage in, garbage out — by contract. Look for the missing upstream filter, not for a bug in the expression.

  (`QRAsGeom` briefly carried a null guard from `c66232c`; it was reverted to restore uniformity.)
- `SpatialReferenceObj.scala` centralizes the `SpatialReference` used by buffer/distance/repair/simplify operations (see the `wkid` convention above).
- `Rect.scala`, `OneGeometryCursor.scala`, `ArrayDataGeometryCursor.scala` are small helpers feeding the esri-geometry `Operator*` APIs — not user-facing functions.
- `notebooks/` is ~63 standalone `.ipynb` demos in one flat directory — treat them as the function-level documentation. `functions.md` (34 KB) is the long-form reference; the README carries a shorter one-line-per-function list. Both are hand-maintained: adding a function means updating them too, or it becomes undiscoverable.
- `data/` and `docs/` are in `.gitignore` (some `data/` files predate that and are still tracked); don't add new zips/PDFs from there.
