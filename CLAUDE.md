# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & package

This is a hybrid Scala + Python project. The Scala side compiles to a shaded fat JAR loaded by Spark; the Python side is a thin py4j wrapper distributed as a wheel. Both must be rebuilt together — `gf.sh` is the canonical build:

```shell
./gf.sh        # mvn clean package + pip wheel + zip release bundle
./pw.sh        # same minus the zip (faster local dev loop)
```

Internals worth knowing:
- `pom_to_pyproject.py` runs in the Maven `validate` phase and copies `<version>` from `pom.xml` into `pyproject.toml`. **Always edit the version in `pom.xml` only** — the pyproject value is overwritten on every build.
- The default Maven build uses `<scope>provided</scope>` for `spark-core`/`spark-sql` (assumes a host Spark runtime). To produce a JAR that can run standalone, activate `-P spark-scope-compile`.
- Spark profiles: `spark-3.3`, `spark-3.4`, `spark-3.5` (default, Java 11 + Scala 2.12), `spark-4.0` (Java 17 + Scala 2.13). Activate with `mvn -P spark-4.0 package`.
- `maven-surefire-plugin` is configured with `<skipTests>true</skipTests>`. Scalatest/Scalacheck are on the test classpath but no `src/test` exists. Run a single test (when added) with `mvn -Dtest=ClassName -DskipTests=false test`.
- The shade plugin aggressively excludes ~15 transitive groups (jaxb, slf4j, log4j, scala-lang, geosolutions, ehcache, etc.) — when adding a dependency that pulls one of these, expect runtime `ClassNotFoundException` unless you remove the exclusion or relocate.

## Runtime target

This library is built to run inside an **ArcGIS Pro conda environment** (`arcgispro-py3` clone) with the [`spark-esri`](https://github.com/mraad/spark-esri) package providing the SparkSession. Per README, **Pro 3.5 is the supported ceiling — Pro 3.6 is currently broken**. The notebooks under `notebooks/` are the primary usage examples and assume this environment.

Standalone PySpark also works: `pyspark==3.5.7` (non-Windows) or `3.5.4` (Windows) is the only required runtime dep beyond the JAR.

## Architecture

The whole library is **Spark Catalyst expressions wrapped twice**:

1. **Scala expressions** (`src/main/scala/com/esri/spark/*.scala`) — one file per function. Each extends `Expression with ImplicitCastInputTypes`, declares `inputTypes`/`dataType`, and implements both an interpreted `eval(InternalRow)` and a codegen `doGenCode(...)`. The codegen path delegates to a `<Name>Obj` companion `object` so the generated Java can call `STPointObj.eval(x, y)` directly — keep this pattern when adding new functions, otherwise codegen-enabled queries will fall back to interpretation.
2. **Geometry serialization**: geometries flow through Spark as `BinaryType` WKB. The `package object spark` provides `geom.bytes` and `bytes.geom` implicits that wrap `OperatorExportToWkb` / `OperatorImportFromWkb` from `esri-geometry-api`. Always use these — they enforce `ShapeImportDefaults`/`ShapeExportDefaults` consistently across functions.
3. **Two registration surfaces** for the Scala layer:
   - `Registry.registerFunctions()` — registers every expression as a SQL temp function (`ST_POINT`, `QR_GEOM`, `H3_LATLNGTOCELL`, …). Add new expressions to this list or they will not be callable from SQL.
   - `GeoFunctions` object — exposes typed `Column => Column` wrappers for the DataFrame API. Add a wrapper here so the Python layer can invoke it.
4. **Python wrapper** (`src/main/python/geofunctions/__init__.py`, single file) — every Python function calls `SparkContext._active_spark_context._jvm.com.esri.spark.GeoFunctions.<camelCaseName>(...)` via py4j and returns a `Column` with a sensible `.alias(...)`. It does **not** use the SQL registry; it goes directly through the Scala `GeoFunctions` object. `st_register_functions()` is the only Python function that touches `Registry`.

Adding a new spatial function therefore requires four coordinated edits:
1. New `STFoo.scala` (or `QRFoo`, `H3Foo`) following the existing expression+companion-object pattern.
2. Append to `Registry.registerFunctions()` to expose it via SQL.
3. Append a `def stFoo(...)` to `GeoFunctions.scala` for the DataFrame API.
4. Append a `def st_foo(...)` to `src/main/python/geofunctions/__init__.py` that calls `_jvm.com.esri.spark.GeoFunctions.stFoo(...)`.

## Other notable pieces

- `JoinQRInnerProcessor.scala` + the `QR*` family implement quad-region (QR) spatial indexing — used for broadcast-join-style spatial joins. `QREnvpGeom`, `QRGeom`, and `QRContainsGeom` are the load-bearing ones.
- `H3Instance.scala` lazily holds a single `H3Core` instance (Uber H3 4.1.1). The H3 dep is `provided` in the POM — the host Spark/ArcGIS env must supply it (or switch to compile scope locally).
- `SpatialReferenceObj.scala` centralizes the `SpatialReference` used by buffer/distance/repair/simplify operations.
- `notebooks/` is a large flat directory of standalone demos — treat them as the function-level documentation. `functions.md` is the long-form reference.
- `data/` and `docs/` are gitignored; don't commit zips/PDFs from there.
