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


class FakeExpression:
    def alias(self, *args, **kwargs):
        return self


class FakePredicate:
    def __eq__(self, value):
        return value


class DissolveFrame:
    def __init__(self, columns=("geom",), label="source", unions=None):
        self.columns = list(columns)
        self.label = label
        self.unions = unions if unions is not None else []

    def withColumn(self, *args, **kwargs):
        return self

    def withColumnRenamed(self, old, new):
        return DissolveFrame(
            [new if column == old else column for column in self.columns],
            self.label,
            self.unions,
        )

    def select(self, *args, **kwargs):
        return self

    def groupBy(self, *args, **kwargs):
        return self

    def agg(self, *args, **kwargs):
        return self

    def localCheckpoint(self, *args, **kwargs):
        return self

    def filter(self, keep):
        return DissolveFrame(
            ("qr", "geom", "in_qr"),
            "inside" if keep else "outside",
            self.unions,
        )

    def count(self):
        return 1 if self.label == "outside" else 0

    def unionAll(self, other):
        self.unions.append((self.label, other.label))
        return self

    def drop(self, *args, **kwargs):
        return self


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

    def test_dissolve_keeps_the_remainder_when_the_breaker_fires(self):
        frame = DissolveFrame()
        expression = FakeExpression()
        with (
            patch.object(gf, "qr_geom_explode", return_value=expression),
            patch.object(gf, "collect_list", return_value=expression),
            patch.object(gf, "st_union_col", return_value=expression),
            patch.object(gf, "st_dump_explode", return_value=expression),
            patch.object(gf, "st_exterior_ring", return_value=expression),
            patch.object(gf, "qr_contains_geom", return_value=expression),
            patch.object(gf, "col", return_value=FakePredicate()),
        ):
            result = gf.pairwise_dissolve(frame, breaker=1)

        self.assertEqual("inside", result.label)
        self.assertEqual([("inside", "outside")], frame.unions)

    def test_dissolve_rejects_non_progressing_parameters(self):
        frame = DissolveFrame()
        with self.assertRaisesRegex(ValueError, "breaker"):
            gf.pairwise_dissolve(frame, breaker=0)
        with self.assertRaisesRegex(ValueError, "cell_mul"):
            gf.pairwise_dissolve(frame, cell_mul=1.0)


if __name__ == "__main__":
    unittest.main()
