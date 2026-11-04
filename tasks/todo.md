# Readers on Spark 4.3: spark-shp + FileGDB built for 3.5 and 4.3 — DONE 2026-11-04

Goal: GeoFunctions `-P spark-4.3` links spark-shp/FileGDB jars **built for 4.3**, not the
4.0 builds via `readers.spark.compact=4.0`. Spark 3.5 stays the default line everywhere; 4.3 is the only Spark 4 line (user, 2026-11-03).
Repos: `/Volumes/LaCie/GWorkspace/{spark-shp,FileGDB}`.

## 1. spark-shp 0.32 -> 0.33
- [x] replace `spark-4.0` with `spark-4.3` (Java 17, Scala 2.13.18, Spark 4.3.0)
- [x] tests green on spark-3.5 and spark-4.3
- [x] version 0.33, README build/jar table
- [x] `mvn install` 0.33-3.5-2.12, 0.33-4.3-2.13
- [x] ship: PR, CodeRabbit, merge; release v0.33 with the 3.5 and 4.3 jars

## 2. FileGDB 0.69
- [x] replace `spark-4.0`/`4.1`/`4.2` with `spark-4.3`
- [x] tests green on spark-3.5 and spark-4.3; real-GDB smoke (Miami, NorthSea, World) on 4.3
- [x] README build/jar table
- [x] `mvn install` 0.69-3.5-2.12, 0.69-4.3-2.13
- [x] ship: PR, merge; release v0.69 with the 3.5 and 4.3 jars

## 3. GeoFunctions
- [x] pom: spark-shp 0.33, filegdb 0.69; drop `readers.spark.compact`; drop the `spark-4.0` profile (4.3 is the only Spark 4 line)
- [x] suite green on spark-3.5 and spark-4.3
- [x] smoke: `shp` + `gdb` read through the shaded jar on PySpark 3.5.9 and 4.3.0 (gps.shp, Miami.gdb)
- [x] README dependency table + CLAUDE.md (no more "untested on 4.3")
- [x] ship; release 0.35 (jar bundles the new filegdb) — PR #29, v0.35
