"""QR-prepare SpatialBench trip and zone tables for one cell size.

usage: python prepare.py <jar> <src_sf_dir> <dst_dir> <cell> [row_group_mb]

trip: every column plus t_pickup_qr (the QR cell of the pickup point), range-sorted by it, so
      a query that keeps only some cells (qr_cells + isin) reads only their row groups.
zone: z_zonekey, z_name, qr, z_piece, z_xmin..z_ymax (the piece envelope, for a cheap test
      before the exact one): each boundary clipped to every QR cell it touches. The
      clip cell is grown by a tiny dist, so a point in cell (q, r) is never on a clip edge and
      ST_WITHIN(point, piece) equals ST_WITHIN(point, boundary) for that cell.
"""
import os, sys, time

jar, src, dst, cell = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
row_group_mb = int(sys.argv[5]) if len(sys.argv) > 5 else 8
if os.path.exists(dst) and os.listdir(dst):
    sys.exit(f"{dst} is not empty")
os.environ.pop("SPARK_HOME", None)
os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-class-path {jar} --jars {jar} pyspark-shell"
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder.master("local[*]").config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.driver.host", "127.0.0.1").config("spark.driver.memory", "64g")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
import geofunctions as gf

block = str(row_group_mb * 1024 * 1024)
parts = spark.sparkContext.defaultParallelism


def write(df, name, qr):
    t = time.perf_counter()
    (df.repartitionByRange(parts, qr).sortWithinPartitions(qr)
     .write.option("parquet.block.size", block).option("compression", "snappy").parquet(f"{dst}/{name}"))
    print(f"{name}: {time.perf_counter() - t:.1f}s", flush=True)


trip = spark.read.parquet(f"{src}/trip")
write(trip.withColumn("t_pickup_qr", gf.qr_fromgeom("t_pickuploc", cell)), "trip", "t_pickup_qr")

zone = spark.read.parquet(f"{src}/zone")
pieces = (zone.select("z_zonekey", "z_name", F.explode(gf.qr_geom("z_boundary", cell, cell * 1e-6)).alias("p"))
          .select("z_zonekey", "z_name", F.col("p.qr").alias("qr"), F.col("p.geom").alias("z_piece"))
          .withColumn("e", gf.st_extent("z_piece"))
          .select("*", *[F.col(f"e.{f}").alias(f"z_{f}") for f in ("xmin", "ymin", "xmax", "ymax")])
          .drop("e"))
write(pieces, "zone", "qr")
