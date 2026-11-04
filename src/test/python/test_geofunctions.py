import unittest
from types import SimpleNamespace
from unittest.mock import patch

import geofunctions as gf


class FakeFrame:
    def __init__(self, columns, name, additions=None):
        self.columns = list(columns)
        self.name = name
        self._jdf = f"{name}-jdf"
        self.sparkSession = f"{name}-session"
        self.additions = additions if additions is not None else []

    def withColumn(self, name, expression):
        self.additions.append((self.name, name, expression))
        return FakeFrame(self.columns + [name], self.name, self.additions)


class GeoFunctionsTest(unittest.TestCase):
    def test_active_context_failure_is_not_an_optimization_sensitive_assert(self):
        with patch.object(gf.SparkContext, "_active_spark_context", None):
            with self.assertRaisesRegex(RuntimeError, "active SparkContext"):
                gf._get_active_spark_context()

    def test_numeric_literals_are_converted_at_the_shared_column_boundary(self):
        with (
            patch.object(gf, "lit", side_effect=lambda value: ("literal", value)),
            patch.object(gf, "_spark_to_java_column", side_effect=lambda value: value),
        ):
            self.assertEqual(("literal", 7), gf._to_java_column(7))
            self.assertEqual(("literal", 2.5), gf._to_java_column(2.5))
            self.assertEqual("field", gf._to_java_column("field"))

    def test_point_collections_accept_varargs_or_one_python_sequence(self):
        with patch.object(gf, "array", side_effect=lambda *values: values):
            self.assertEqual(("a", "b"), gf._point_array(("a", "b"), "fn"))
            self.assertEqual(("a", "b"), gf._point_array((["a", "b"],), "fn"))
            self.assertEqual(("a",), gf._point_array((["a"],), "fn"))
            self.assertEqual("a", gf._point_array(("a",), "fn"))
            with self.assertRaisesRegex(ValueError, "at least one point"):
                gf._point_array(([],), "fn")

    def test_join_adds_only_missing_qr_columns_and_honors_intersection_alias(self):
        calls = []

        class Processor:
            @staticmethod
            def apply(*args):
                calls.append(args)
                return "joined-jdf"

        jvm = SimpleNamespace(
            com=SimpleNamespace(
                esri=SimpleNamespace(
                    spark=SimpleNamespace(JoinQRInnerProcessor=Processor)
                )
            )
        )
        additions = []
        lhs = FakeFrame(["geom"], "lhs", additions)
        rhs = FakeFrame(["geom", "qr"], "rhs", additions)

        with (
            patch.object(gf, "_get_active_spark_context", return_value=SimpleNamespace(_jvm=jvm)),
            patch.object(gf, "qr_envp_explode", side_effect=lambda *args: ("qr", args)),
            patch.object(gf, "DataFrame", side_effect=lambda jdf, session: (jdf, session)),
        ):
            result = gf.join_qr(lhs, rhs, 10, 2, oper="intersection")

        self.assertEqual([("lhs", "qr", ("qr", ("geom", 10.0, 2.0)))], additions)
        self.assertEqual(("joined-jdf", "lhs-session"), result)
        self.assertEqual("intersects", calls[0][4])
        self.assertEqual("lhs-jdf", calls[0][0])
        self.assertEqual("rhs-jdf", calls[0][1])

    def test_join_rejects_values_the_scala_processor_would_silently_misread(self):
        lhs = FakeFrame(["geom", "qr"], "lhs")
        rhs = FakeFrame(["geom", "qr"], "rhs")
        with self.assertRaisesRegex(ValueError, "unsupported spatial operation"):
            gf.join_qr(lhs, rhs, 1.0, oper="typo")
        with self.assertRaisesRegex(ValueError, "unsupported spatial operation"):
            gf.join_qr(lhs, rhs, 1.0, oper="disjoint")
        with self.assertRaisesRegex(ValueError, "acceleration"):
            gf.join_qr(lhs, rhs, 1.0, acceleration="fast")
        with self.assertRaisesRegex(ValueError, "cell must"):
            gf.join_qr(lhs, rhs, 0.0)
        with self.assertRaisesRegex(ValueError, "dist must"):
            gf.join_qr(lhs, rhs, 1.0, dist=-1.0)

    def test_knn_join_rejects_a_k_that_is_not_a_positive_integer(self):
        lhs = FakeFrame(["geom"], "lhs")
        rhs = FakeFrame(["geom"], "rhs")
        for k in (0, -1, 2.5, True, "5"):
            with self.assertRaisesRegex(ValueError, "k must be a positive integer"):
                gf.knn_join(lhs, rhs, k)

    def test_broadcast_join_rejects_unknown_sides_and_inexpressible_operations(self):
        lhs = FakeFrame(["geom", "qr"], "lhs")
        rhs = FakeFrame(["geom", "qr"], "rhs")
        with self.assertRaisesRegex(ValueError, "broadcast must"):
            gf.join_qr(lhs, rhs, 1.0, broadcast="left")
        for oper in ("crosses", "equals"):
            with self.assertRaisesRegex(ValueError, "broadcast join does not support"):
                gf.join_qr(lhs, rhs, 1.0, oper=oper, broadcast="lhs")

    def test_dissolve_rejects_non_progressing_parameters(self):
        frame = FakeFrame(["geom"], "buf")
        with self.assertRaisesRegex(ValueError, "breaker"):
            gf.pairwise_dissolve(frame, breaker=0)
        with self.assertRaisesRegex(ValueError, "cell_mul"):
            gf.pairwise_dissolve(frame, cell_mul=1.0)

    def test_arrow_table_turns_large_string_into_string(self):
        import pandas as pd
        import pyarrow as pa
        # pandas 3 makes large_string by itself; ArcGIS drops those columns with no error.
        schema = pa.schema([pa.field("s", pa.large_string()), pa.field("SHAPE", pa.binary(), metadata={"k": "v"})])
        tab = gf._arrow_table(pd.DataFrame({"s": ["abc"], "SHAPE": [b"x"]}), schema)
        self.assertEqual(pa.string(), tab.schema.field("s").type)
        self.assertEqual({b"k": b"v"}, tab.schema.field("SHAPE").metadata)
        self.assertEqual("abc", tab.column("s")[0].as_py())

    def test_wkb_families_group_single_and_multi_part(self):
        def wkb(code, order=1):
            return bytes([order]) + code.to_bytes(4, "little" if order == 1 else "big") + b"rest"
        self.assertEqual({"polygon"}, gf._wkb_families([wkb(3), wkb(6), wkb(1006), bytearray(wkb(3, 0))]))
        self.assertEqual({"point", "polyline"}, gf._wkb_families([wkb(1), wkb(5)]))
        self.assertEqual({"multipoint"}, gf._wkb_families([wkb(3004)]))


if __name__ == "__main__":
    unittest.main()
