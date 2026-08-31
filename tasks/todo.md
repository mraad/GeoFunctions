# Spark 4.0 / Scala 2.13 compatibility — DONE 2026-08-31

Goal: `mvn -P spark-4.0 clean package` builds GeoFunctions. Achieved; full suite passes on
both profiles.

Toolchain: Spark 4.0.0, Scala 2.13.16, Java 17.
Repos live in `/Volumes/LaCie/GWorkspace/` (not `~/GWorkspace`).

## 1. WebMercator -> `webmercator-1.15-2.13`
- [x] `mvn -P scala-2.13 clean install` — profile already existed, no pom change. 5/5 tests.

## 2. spark-shp -> `spark-shp-0.32-4.0-2.13`
- [x] added `spark-4.0` profile (java 17, scala 2.13.16, scalatest 3.2.19, spark 4.0.0)
- [x] `mvn -P spark-4.0,esri-geometry-github clean install`. 13/13 tests.

## 3. FileGDB -> `filegdb-0.68-4.0-2.13`
- [x] added `spark-4.0` profile
- [x] `mvn -P spark-4.0 clean install`. 5/5 tests (1 ignored, as on 3.5).

## 4. GeoFunctions
- [x] `.par` removed from `QRScan` and `JoinQRInnerProcessor` — `IntStream.parallel()` instead
- [x] `Column`/`Expression` bridge split into `src/main/scala-{2.12,2.13}/org/apache/spark/sql/esri/ColumnCompat.scala`, selected by `build-helper-maven-plugin`
- [x] `ClipLineDistSpec` `out.sorted` -> `out.toSeq.sorted` (2.13's `Seq` is `immutable.Seq`)
- [x] `mvn -P spark-4.0 -DskipTests=false clean package`. 51/51, warning-free.

## 5. Regression
- [x] `mvn -P spark-3.5 -DskipTests=false clean package`. 51/51, warning-free.
- [x] `./pw.sh` green, wheel builds.

## 6. Docs
- [x] CLAUDE.md profile section rewritten
- [x] README note + new "Spark 4.0" section

## 7. PySpark 3.x + 4.x from one wheel
- [x] `pyproject.toml`: `pyspark==3.5.9` -> `pyspark>=3.5.9,<5`
- [x] `geofunctions/__init__.py`: `Column`/`_to_java_column` import falls back from
      `pyspark.sql.classic.column` (4.0) to `pyspark.sql.column` (3.x)
- [x] verified end to end on pyspark 3.5.9 + 2.12 jar and pyspark 4.0.0 + 2.13 jar:
      identical output for st_point/st_astext, qr_fromxy, qr_envp_explode, the DataFrame
      monkey-patch and the SQL registry
- [x] confirmed the range survives `pom_to_pyproject.py`'s toml round trip

## 8. Committed
- [x] `agent/spark-4.0-scala-2.13` pushed in GeoFunctions, spark-shp, FileGDB
- [x] `agent/readme-1.15-scala-2.13` pushed in WebMercator (docs only)

## Outstanding
- [ ] **Open and merge the three PRs.** All three branches are pushed but unmerged;
      GeoFunctions has CODEOWNERS requiring owner review.
- [ ] **Publish the Scala 2.13 dependency artifacts.** `webmercator-1.15-2.13`,
      `spark-shp-0.32-4.0-2.13` and `filegdb-0.68-4.0-2.13` exist only in the local `~/.m2`.
      Until they are published, `-P spark-4.0` works only on a machine that has built all
      three by hand. Needs a decision on where they go.
- [ ] **Decide whether releases ship a 2.13 artifact.** `gf.sh` builds one zip from the
      default 3.5 profile. A Spark 4.0 user currently has to build the jar themselves.
      Nothing in the release process is wired for two Scala lines.
- [ ] **Merge the WebMercator README fix** (`agent/readme-1.15-scala-2.13`). Not a profile
      change — its `scala-2.13` profile was already correct, and `spark-4.0` naming would be
      wrong for a Spark-independent build. The README was stale instead: it pinned 1.14
      (project is at 1.15), used the postfix argument-less Scala form that 2.13 deprecates,
      and never said `scala-2.13` targets Java 17.

## Not doing
- Raising the `spark-3.3`/`spark-3.4` profiles or testing them. Untouched and unverified
  here; they were already unverified before.
