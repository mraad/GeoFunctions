# Repository Guidelines

## Project Structure & Module Organization

Core Spark Catalyst expressions live in `src/main/scala/com/esri/spark/`, generally one class per spatial function. The PySpark/py4j API is the single module `src/main/python/geofunctions/__init__.py`. ScalaTest suites belong in `src/test/scala/com/esri/spark/` and use the `*Spec.scala` suffix. Usage examples are in `notebooks/`; `functions.md` is the function reference, and `README.md` covers installation and runtime setup. ArcGIS toolbox files live in `toolbox/`. Treat `build/`, `target/`, and `.ipynb_checkpoints/` as generated output; `data/` and `docs/` are generally ignored.

The geometry engine is the [Esri Geometry API for Java](https://github.com/Esri/geometry-api-java.git). `pom.xml` currently consumes `com.esri.geometry:esri-geometry-api:2.2.5-SNAPSHOT` and shades it into the project JAR. If Maven cannot resolve that snapshot, clone the linked repository and run `mvn install` there before building GeoFunctions.

## Build, Test, and Development Commands

- `source .venv/bin/activate` selects the repository environment before running scripts.
- `./pw.sh` performs the normal development build: a Maven clean/package followed by a Python wheel in `target/`.
- `mvn -DskipTests=false test` compiles the project and runs every ScalaTest suite. Tests are skipped by default.
- `mvn -DskipTests=false -Dsuites=com.esri.spark.ClipLineDistSpec test` runs one suite.
- `mvn -DskipTests=false install` runs the ScalaTest suites, builds the shaded artifacts, and installs them in the local Maven repository.
- `./gf.sh` builds the JAR and wheel, validates notebook JAR placeholders, and creates the release ZIP; it requires the ignored release data archives.

The supported baseline is Java 11, Scala 2.12, Spark/PySpark 3.5.9, and Python 3.10 or newer. Java 17 is also a supported, smoke-tested runtime. When Maven embeds Spark on Java 17, set `JAVA_TOOL_OPTIONS=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED`; PySpark's launcher supplies the necessary module options itself. Java 21 is not supported by Spark 3.5.9 and would require upgrading to Spark 4 and Scala 2.13.

## Coding Style & Naming Conventions

Follow neighboring Scala formatting (two-space indentation) and Python PEP 8 conventions (four spaces, type hints, and reStructuredText-style docstrings). Scala expression classes use PascalCase names such as `STBuffer`; Python wrappers use snake_case such as `st_buffer`. Preserve the expression/companion-object pattern used for Spark code generation. Adding a function normally requires coordinated changes to its Scala expression, `Registry.scala`, `GeoFunctions.scala`, the Python module, and user documentation. No repository-wide formatter is enforced, so keep diffs consistent with adjacent code.

QR keys pack signed 32-bit `q` and `r` indices into one `Long`. Validate finite positive cell sizes, finite non-negative padding, index bounds, and candidate counts before converting or allocating. Keep interpreted Catalyst evaluation and generated code consistent for null propagation and argument-count failures. A QR overlap join cannot implement `disjoint`; exact join operations require non-null binary geometry columns and a valid spatial reference.

## Testing Guidelines

Use ScalaTest `AnyFlatSpec` with `Matchers`, deterministic fixtures, and focused regression cases. Cover boundary conditions and invalid arguments for geometry/grid behavior, including negative coordinates, empty geometries, null propagation, generated-code parity, QR index overflow, canonical-cell ownership, and replicated-pair suppression. There is no configured coverage threshold, but all affected suites must pass with tests explicitly enabled. Run processor smoke tests on both Java 11 and Java 17 when changing Spark embedding, Catalyst code generation, or QR join execution.

## Commit & Pull Request Guidelines

Recent commits use concise, imperative subjects such as `Fix the spatial-reference cache...` or `Document the build invariants...`. Keep each commit scoped to one logical change. Pull requests should explain the behavior change, link relevant issues, list verification commands, and update `README.md`, `functions.md`, or notebooks when public APIs change. Include screenshots only when notebook or toolbox output is visually relevant.

## Configuration & Release Safety

Do not hardcode absolute workspace paths. Change the project version only in `pom.xml`; the Maven validation phase updates `pyproject.toml`. Preserve `geofunctions-0.XX.jar` placeholders in shipped notebooks.
