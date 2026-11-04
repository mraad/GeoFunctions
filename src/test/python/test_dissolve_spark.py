"""pairwise_dissolve on a real local Spark session with the built jar.

Skipped when no shaded jar is found: set GF_JAR, or build one into target/ (./pw.sh).
Each case checks the row count and the total area, so a duplicate, a lost piece or a filled
hole all fail.
"""
import glob
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


def _jar():
    if os.environ.get("GF_JAR"):
        return os.environ["GF_JAR"]
    jars = [j for j in glob.glob(os.path.join(REPO, "target", "geofunctions-*.jar"))
            if not os.path.basename(j).startswith("original-") and "-2.1" not in os.path.basename(j)]
    return jars[0] if jars else None


JAR = _jar()


@unittest.skipIf(JAR is None, "no shaded geofunctions jar; set GF_JAR or run ./pw.sh")
class DissolveSparkTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ.pop("SPARK_HOME", None)
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-class-path {JAR} --jars {JAR} pyspark-shell"
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").appName("DissolveSparkTest")
                     .config("spark.ui.showConsoleProgress", "false")
                     .config("spark.driver.bindAddress", "127.0.0.1")
                     .config("spark.driver.host", "127.0.0.1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def dissolve(self, *wkts, **kwargs):
        import geofunctions as gf
        df = self.spark.createDataFrame([(w,) for w in wkts], "wkt string").select(gf.st_fromtext("wkt").alias("geom"))
        rows = (df.pairwise_dissolve(cell=100_000.0, dist=100.0, wkid=3857, **kwargs)
                .select(gf.st_area("geom").alias("a")).collect())
        return sorted(round(r.a, 6) for r in rows)

    def test_a_polygon_near_a_cell_edge_comes_out_once(self):
        # Wholly in cell q=0 but within dist of x=100000, and within dist of y=0 at every scale.
        self.assertEqual([7200.0], self.dissolve("POLYGON ((99900 10, 99990 10, 99990 90, 99900 90, 99900 10))"))

    def test_a_hole_survives(self):
        self.assertEqual([6000.0], self.dissolve(
            "POLYGON ((10 10, 90 10, 90 90, 10 90, 10 10), (40 40, 40 60, 60 60, 60 40, 40 40))"))

    def test_overlapping_polygons_across_a_cell_edge_merge_into_one(self):
        # Two 100x100 squares overlapping by 50x50 across x = 100000: 2 * 10000 - 2500.
        self.assertEqual([17500.0], self.dissolve(
            "POLYGON ((99950 1000, 100050 1000, 100050 1100, 99950 1100, 99950 1000))",
            "POLYGON ((100000 1050, 100100 1050, 100100 1150, 100000 1150, 100000 1050))"))

    def test_separate_polygons_stay_separate(self):
        self.assertEqual([100.0, 100.0], self.dissolve(
            "POLYGON ((1000 1000, 1010 1000, 1010 1010, 1000 1010, 1000 1000))",
            "POLYGON ((5000 5000, 5010 5000, 5010 5010, 5000 5010, 5000 5000))"))


if __name__ == "__main__":
    unittest.main()
