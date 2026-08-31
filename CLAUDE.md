# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A PySpark geospatial function library built on the Esri Geometry Library: Spark Catalyst expressions in Scala (shaded fat JAR) wrapped by a py4j Python layer (wheel). Runs inside an ArcGIS Pro conda env via `spark-esri` (Pro 3.5 is the ceiling — 3.6 is broken) or against plain `pyspark>=3.5.9,<5`. Notebooks under `notebooks/` (~63, flat) are the usage documentation; `functions.md` is the long-form reference. Both are hand-maintained.

## Build & package

```shell
./pw.sh        # inner dev loop: mvn clean package + pip wheel
./gf.sh        # same + zip release bundle (needs the gitignored data/ zips)
mvn -DskipTests=false test                                            # all scalatest suites
mvn -DskipTests=false -Dsuites=com.esri.spark.ClipLineDistSpec test   # one suite
```

- Tests are **skipped by default** (`<skipTests>true</skipTests>`); surefire can't discover scalatest specs, so `scalatest-maven-plugin` runs them.
- Run the scripts through the repo venv (`source .venv/bin/activate`) — they call `python3`, which must be the venv's. The venv is relocatable; if you recreate it, use `uv venv --relocatable` or absolute paths come back.
- **This repo lives on an external drive whose mount name changes — never hardcode an absolute path** in scripts, notebooks, or config.
- `pom_to_pyproject.py` (Maven `validate` phase) copies `<version>` from `pom.xml` into `pyproject.toml`. **Edit the version in `pom.xml` only** — pyproject is overwritten every build.
- Spark deps are `provided`-scope by default; `-P spark-scope-compile` produces a standalone-runnable JAR.
- Profiles: `spark-3.3`, `spark-3.4`, **`spark-3.5` (default: Spark 3.5.9, Java 11, Scala 2.12.21)**, `spark-4.0` (Spark 4.0.0, Java 17, Scala 2.13.16). **`spark-4.0` builds and passes the full suite**, but only against locally installed 2.13 deps — Maven Central has none of them. Build them first, in any order:
  - `WebMercator`: `mvn -P scala-2.13 clean install` → `webmercator-1.15-2.13`.
  - `spark-shp`: `mvn -P spark-4.0,esri-geometry-github clean install` → `spark-shp-0.32-4.0-2.13`. **Name `esri-geometry-github` explicitly**: Maven drops *every* `activeByDefault` profile as soon as `-P` selects one, and that profile is what supplies `geometry.api.*`.
  - `FileGDB`: `mvn -P spark-4.0 clean install` → `filegdb-0.68-4.0-2.13`.
- **Two files are compiled per Scala version, not one.** Spark 4.0 moved `Column` to `spark-sql-api`, where it wraps a `ColumnNode` instead of a Catalyst `Expression` — the `Expression` constructor and `.expr` are gone. `src/main/scala-2.12/` and `src/main/scala-2.13/` each hold one `ColumnCompat.scala`, and `build-helper-maven-plugin` puts the matching one on the source path from `${scala.compact}`. Both live in **`org.apache.spark.sql.esri`, not `com.esri.spark`** — every 4.0 route across that boundary is `private[spark]` or `private[sql]` (`ExpressionUtils`, `object Column`, `ExpressionColumnNode`), so an outside package cannot reach any of them. Nothing else in the tree is version-specific; keep it that way.
- The three `.par` call sites are gone — Scala 2.13 puts `.par` behind an import 2.12 rejects, so `QRScan` and `JoinQRInnerProcessor` fan out over `java.util.stream.IntStream.parallel()` instead, writing into a pre-sized array at disjoint indices. Same shared-ForkJoin-pool characteristic `spark.esri.parallel` already guarded. Don't reintroduce `.par`.
- **Tests pin the driver to loopback** (`spark.driver.bindAddress`/`spark.driver.host` = `127.0.0.1`, in the scalatest plugin config here and in `spark-shp`/`FileGDB`). Spark 4.0 gives every session an artifact class loader that fetches unknown classes over RPC from the driver's own address, and Janino probes `<generated package>.Object` on each codegen compile; on a host that refuses the self-connection on its LAN address (macOS firewall) every probe becomes a `RemoteClassLoaderError` and the task dies. Spark 3.x never used that loader. Removing the pin makes the 4.0 suite fail with `RemoteClassLoaderError: org/apache/spark/sql/catalyst/expressions/Object.class`.
- The scalatest plugin also carries the `--add-opens` block. It is **required on JDK 17 for every profile, 3.5 included** — without it any suite that starts a `SparkContext` aborts in `StorageUtils` with `IllegalAccessError` on `sun.nio.ch.DirectBuffer`.
- **The Python layer spans both lines; the jar does not.** `pyproject.toml` declares `pyspark>=3.5.9,<5`, so one wheel installs against 3.5.x or 4.0.x — but the *jar* is per-Scala-version, and a 2.12 jar on PySpark 4.0 dies with `NoClassDefFoundError: scala/collection/SeqOps`. Pair `-P spark-3.5` output with `pyspark` 3.5.x and `-P spark-4.0` output with 4.0.x. ArcGIS Pro 3.5 ships Spark 3.5 through `spark-esri`, so **releases stay on the 3.5 jar**.
  - The version range lives in `pyproject.toml` as data, and `pom_to_pyproject.py` rewrites that file through `toml.load`/`toml.dump` on every build — data survives, **comments do not**. Explain the range here or in the README, never in `pyproject.toml`.
  - `geofunctions/__init__.py` imports `Column`/`_to_java_column` from `pyspark.sql.classic.column` first and falls back to `pyspark.sql.column`: PySpark 4.0 split `Column` into an abstract base plus a classic implementation and moved `_to_java_column` with it, so the 3.x import path raises `ImportError` on 4.0 and the 4.0 path does not exist on 3.x. All 134 wrappers build a classic `Column`; keep the try/except, don't "simplify" it to one import.
- **`SPARK_HOME` overrides pyspark's bundled jars.** With sdkman's Spark on `PATH`, `SPARK_HOME` points at a 2.12 Spark 3.5 and any PySpark 4.0 session dies at `getOrCreate` with `TypeError: 'JavaPackage' object is not callable`, or at the first geofunctions call with `ClassNotFoundException: scala.collection.SeqOps`. Neither is a defect in this library — unset `SPARK_HOME` when testing against a pip-installed PySpark.
- Four files pin Spark/Python versions and must move together: `pom.xml`, `pyproject.toml`, `environment.yml`, `.venv/`. Python floor is 3.10 — don't raise it without checking what Python the supported ArcGIS Pro releases ship.
- The **project** version appears in the README install snippets, and `gf.sh` ships the 9 jar-referencing notebooks inside the release zip. Those notebooks deliberately carry a **`geofunctions-0.XX.jar` placeholder** for the reader to substitute — never a real version, which goes stale and ships a release whose own examples can't find the jar (0.28 shipped that way inside `geofunctions-0.29.zip`). `gf.sh` fails the build if a concrete version creeps back in.
- The shade plugin excludes ~15 transitive groups (jaxb, slf4j, log4j, scala-lang, …) — a new dependency pulling one of these hits runtime `ClassNotFoundException` unless you drop the exclusion or relocate.
- **`./gf.sh` is warning-free, and two `pom.xml` entries exist only to keep it that way — don't "clean up" either:**
  - `org.apache.yetus:audience-annotations` is excluded from **both** `spark-core` and `spark-sql` (its POM declares a system-scoped `jdk.tools` gone since JDK 8; the collector walks both paths, so dropping either exclusion brings the warning back).
  - `-Wconf:msg=InlineInfoAttribute\sfrom\sorg\.apache\.spark:s` suppresses an **upstream defect in the published `spark-catalyst_2.12-3.5.9.jar`** (sha1 matches Maven Central): 3 of the 94 entries in `CodeGenerator$`'s `ScalaInlineInfo` attribute index constant-pool slots holding `Methodref`/`NameAndType`/`Class` instead of `Utf8`, so ASM's `readUTF8` overruns its buffer. Reproduces on Scala 2.12.18/20/21. **Dead ends already tried:** dropping `-opt:l:inline` silences it but `QRIntersect.scala:15` explicitly asks for the inliner; neither `-opt-warnings` (any value) nor `-Wconf:cat=optimizer` suppresses it, because the message comes from the classfile reader — `msg=` is the only lever. `\s` keeps the filter one whitespace-free token and the escaped dots keep them literal. If you change the pattern, verify it still *matches* (point it at a bogus package and confirm the warning reappears) — a filter that silently stops matching looks identical to one that works.
- **`pyarrow` must stay a function-local import** in `to_feature_table`/`to_feature_class`, next to `import arcpy`. It is not in `dependencies` (only the `jupyter` extra); hoisting it to module scope breaks `import geofunctions` on a clean install — invisible in ArcGIS Pro and `.venv/`, which both happen to have it.
- `toml` in `dependencies` looks unused but `pom_to_pyproject.py` needs it via the editable install — left deliberately.
- `.gitignore` ignores `*.sh`, `*.xml`, `*.zip`, `data/`, `docs/`. `pom.xml`/`gf.sh` were force-added; a new script or XML file needs `git add -f` or it silently never gets committed.

## Running outside ArcGIS Pro

The shaded JAR bundles `com.esri:filegdb`, registering a `gdb` Spark data source (options: `path`, `name`, `numPartitions`, `wkid`):

```python
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-class-path {JAR} --jars {JAR} pyspark-shell"  # py4j needs the system classloader
df = spark.read.format("gdb").option("path", "X.gdb").option("name", "LayerName").load()
```

Points come back as `Shape: struct<x, y>`; lines/polygons as `struct<xmin, ymin, xmax, ymax, parts, coords>` — feed the latter to `gdb_polyline2`/`gdb_polygon2` for WKB. Three non-obvious failure modes:

- H3 is `provided` and not bundled — nothing supplies it, including ArcGIS Pro. Fetch the version `pom.xml` pins from `https://repo1.maven.org/maven2/com/uber/h3/<ver>/` (or `mvn dependency:get`, which leaves it under `~/.m2/repository/com/uber/h3/*/h3-*.jar`) and list it in `spark.jars` alongside the shaded jar, which covers driver and executors. Switching the dep to compile scope locally also works and needs no Maven repo at all. Without it H3 fails at *task* time with `NoClassDefFoundError: com/uber/h3core/H3Core`, not at session start, because `H3Instance.h3` is a lazy val evaluated inside `eval` on the executors — so a driver-only classpath entry passes under `local` and fails everywhere else. Don't hardcode the H3 version into docs; `pom.xml` owns it and `~/.m2` already holds both 4.1.1 and 4.4.0.
- **`--driver-class-path` is `os.pathsep`-separated, `--jars` is comma-separated.** Colons in `--jars` kill the launch with an opaque `Java gateway process exited before sending its port number`.
- Set `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to the venv interpreter, or the worker grabs the first `python3` on `PATH` and dies with `PYTHON_VERSION_MISMATCH` — but only on functions that round-trip through a Python worker, so most of the library appears to work first.

## Architecture

The library is Catalyst expressions wrapped twice:

1. **Scala expressions** (`src/main/scala/com/esri/spark/*.scala`, one file per function, ~68 expressions). Each extends `Expression with ImplicitCastInputTypes` and implements both interpreted `eval(InternalRow)` and codegen `doGenCode(...)`. Codegen delegates to a `<Name>Obj` companion `object` so generated Java calls `STPointObj.eval(x, y)` directly — keep this pattern or codegen-enabled queries fall back to interpretation.
2. **Geometry serde**: geometries flow through Spark as `BinaryType` WKB. The `package object spark` provides `geom.bytes` / `bytes.geom` implicits wrapping `OperatorExportToWkb`/`OperatorImportFromWkb` — always use these; they enforce the Shape import/export defaults consistently. **WKB has no envelope type**, so an `Envelope` exports as a `Polygon` and `bytes.geom` never returns an `Envelope`. Any expression that takes `BinaryType` therefore only ever sees `Point`/`MultiVertexGeometry` for envelope input — a `case env: Envelope` branch in one of these is dead code, however plausible the class hierarchy makes it look (`Envelope` does extend `Geometry` directly, not `MultiVertexGeometry`). `MercatorSpec` pins this.
3. **Two registration surfaces**:
   - `Registry.registerFunctions()` — SQL temp functions (`ST_POINT`, `QR_CLIP`, `H3_LATLNGTOCELL`, …). Append new expressions here or they're not callable from SQL. **The SQL surface is neither complete nor name-consistent**: names aren't derived from class names (`QRGeom` → `QR_CLIP`), `ClipLine`/`ClipLineDist`/`GDBPolylineM`/`GDBPolylineZ` are commented out, and `QR_ENVPGROM` is a live typo for `QR_ENVPGEOM` — don't rename it silently, callers may depend on it.
   - `GeoFunctions` object — typed `Column => Column` wrappers (~71 defs) for the DataFrame API.
4. **Python layer** (`src/main/python/geofunctions/__init__.py`, single ~84 KB file, ~88 functions) — calls `_jvm.com.esri.spark.GeoFunctions.<camelCase>(...)` via py4j and returns an aliased `Column`. It bypasses the SQL registry; `st_register_functions()` is the only Python function touching `Registry`.

**Axis-symmetric reuse is intentional**: QR grid math is identical on both axes, so `ST_RTOY`/`st_rtoy` are wired to `STQToX` and `st_ytor` to `stXToQ`. **There is no `STRToY.scala` and no `STYToR.scala` — don't add either.** The y→r direction has no SQL registration at all (only `ST_XTOQ` and `ST_RTOY`), so it looks like a gap in the family; it isn't.

**`wkid` convention**: spatial-reference args are *string* Catalyst args resolved by `SpatialReferenceObj.create`. `"-1"` means null spatial reference (planar math); `4326`/`3857`/`102008` **and their `EPSG:`-prefixed forms** are pre-cached, others created on demand and memoized into a `TrieMap` via `getOrElseUpdate`. (It was `getOrElse` until 2026-07-31, which evaluated the default and threw it away — every row rebuilt the `SpatialReference` for any non-pre-cached wkid. Keep it `getOrElseUpdate`.) Python defaults vary per function — check before assuming `-1`.

**Adding a spatial function = four coordinated edits**, then update `functions.md` and the README list (nothing generates them):
1. New `STFoo.scala` following the expression + companion-object pattern.
2. Append to `Registry.registerFunctions()`.
3. Append `def stFoo(...)` to `GeoFunctions.scala`.
4. Append `def st_foo(...)` to the Python `__init__.py` with the reST-style docstring the file uses.

## Python-only layer

Roughly a dozen Python functions have no Scala counterpart — don't go looking for one:

- `*_explode` helpers wrap `explode()` around array-returning functions and re-alias — the idiom for consuming any `ArrayType` expression.
- `join_qr(lhs, rhs, cell, ...)` — spatial-join entry point with two dispatch paths: no `qr` column → pure-DataFrame join (`qr_envp_explode` both sides → equi-join on `qr` → `qr_intersect` filter); existing `qr` column → `JoinQRInnerProcessor.apply(...)` over the raw `_jdf`.
- arcpy interop (`to_spark`, `to_feature_class`, `to_feature_table`) — pyarrow bridge, and all three also go through pandas (`df.toPandas()` / `tab.to_pandas()`); `import arcpy` is deliberately function-local.
- `pairwise_dissolve(...)` — progressive multi-resolution dissolve loop; every intermediate is `localCheckpoint()`ed to truncate lineage — removing those blows up query-plan size.

At import time the module **monkey-patches `pyspark.sql.DataFrame`** with `pairwise_dissolve`, `join_qr`, `to_feature_table`, `to_feature_class` (bottom of the file) so notebooks call `df.join_qr(...)`. Register new DataFrame-level helpers there.

## Invariants and known traps

- **Null inputs are the caller's responsibility — never add per-row null checks.** By contract a GeoFunction is never invoked with null; `doGenCode` sets `isNull = FalseLiteral` and interpreted `eval` casts straight through, uniformly across all expressions. **Deliberate, not an oversight — don't "fix" it.** Debugging nonsense output? Null is silently fabricated (interpreted unboxes to `0.0`/`0L`, codegen substitutes `-1.0`/`-1L`, so the paths disagree): `ST_POINT(NULL, 2.0)` → `POINT (-1 2)` under codegen. Look for the missing upstream filter, not an expression bug. (`QRAsGeom` briefly carried a null guard from `c66232c`; it was reverted to restore uniformity — don't re-apply it.)
- **`dist` inflates the cell, and every candidate range must account for it.** Cell `(q,r)` spans `[q*cell-dist, (q+1)*cell+dist]` per axis, so the q/r search range is the geometry extent inflated by dist: `floor((xmin-dist)/cell) .. floor((xmax+dist)/cell)`. `QRCount`, `QRList`, `QREnvp`, `QRScan`, `ClipLineDist` all follow this; a new function using the raw extent silently drops boundary cells whenever `dist > 0`. `QRScanSpec` pins the family to agreeing.
- `QRScan.scala` is the single cell scan behind `QR_GEOM` and `QR_ENVPGEOM` — walk the range, intersect with a reused scratch `Envelope`, emit via a caller-supplied row builder. Keep new variants as row builders over `QRScan.eval`, not new scans. Don't accelerate the per-cell envelope (pure overhead); the input geometry is accelerated once already.
- Executor-side config must come from `SparkEnv.get.conf`, **not** `SparkSession.getActiveSession` (empty on executors, where expressions run). Both knobs were dead until they were moved. `spark.esri.accelerate` (default true) toggles geometry acceleration; `spark.esri.parallel` (default 64) is **the cell count above which one geometry's scan fans out over the parallel-collection pool** — not a thread count. Lowering it makes *more* geometries fan out, oversubscribing the shared ForkJoin pool inside an already-parallel Spark task; raise it to reduce fan-out.
- `ClipLine`/`ClipLineDist` take **one segment** as `array(x1,y1,x2,y2)` (not a polyline) and return one row per `(q, r, length)` cell crossed; `ClipLine` is a one-line delegate to `ClipLineDistObj.eval(.., 0.0)`, so fix bugs in the `Dist` variant. Two invariants covered by `ClipLineDistSpec`: with `dist = 0` emitted lengths sum exactly to segment length, and the emitted cell set equals a brute-force scan of the inflated bbox. Do **not** hoist a shared `UnsafeRowWriter` out of the emit loop — `resetRowWriter()` doesn't rewind the cursor, so every row becomes a copy of the first (shipped broken in `c66232c`). Build rows with `InternalRow(q, r, l)`.
- `GDBPolygon2`/`GDBPolygonM`/`GDBPolygonZ`/`GDBPolyline2` convert the gdb/shp `Shape` struct to WKB and index it **positionally** (`row.getArray(4)`/`getArray(5)`) — a reader schema change breaks them silently. Each assumes its own coord stride: `2` for the `*2` variants `(x,y)`, `3` for `Z` `(x,y,z)`, `4` for `M` `(x,y,z,m)`. **`new Point(x, y, z)` is the only 3-arg constructor and it sets Z** — a measure has to go through `point.setM(...)` or it silently lands in Z and the real Z is dropped (`GDBPolygonM` shipped that way until 2026-07-31; `GDBPolygonMSpec` pins it). Both Z and M survive the WKB round trip: `ShapeExportDefaults` is `0` and the strip flags (`ShapeExportStripZs`/`StripMs`) are opt-in.
- **`GDBPolygonZ`'s stride of 3 and `GDBPolygonM`'s stride of 4 cannot both be right** unless the `filegdb` reader emits a different tuple width per shape type. Unverified — there is no reader source in the workspace. If M shapes actually arrive as `(x,y,m)` triples, `GDBPolygonM` reads x from the wrong slot and overruns the array. Check before trusting `GDB_POLYGONM` on real data.
- **H3 is the only hexagonal indexing.** `com.esri:grid-hex` was removed as dead weight — don't reintroduce it. `H3Instance.scala` lazily holds the single `H3Core`; the dep is `provided`.
- `data/` and `docs/` are gitignored (some `data/` files predate that and remain tracked) — don't add new zips/PDFs from there.
