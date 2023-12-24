import pytest

from bigquery_frame import BigQueryBuilder
from bigquery_frame import functions as f
from bigquery_frame.dataframe import strip_margin
from tests.utils import captured_output


class TestFunctions:
    def test_isnull_with_alias(self, bq: BigQueryBuilder):
        """isnull should work on columns with an alias"""
        df = bq.sql("""SELECT * FROM UNNEST([1, 2, NULL]) as a""")

        with captured_output() as (stdout, stderr):
            df.withColumn("b", f.isnull(f.col("a").alias("a"))).show()
            expected = strip_margin(
                """
                |+------+-------+
                ||    a |     b |
                |+------+-------+
                ||    1 | False |
                ||    2 | False |
                || null |  True |
                |+------+-------+
                |"""
            )
            assert stdout.getvalue() == expected

    def test_when(self, bq: BigQueryBuilder):
        df = bq.sql(
            """
            SELECT
                *
            FROM UNNEST ([
                STRUCT(1 as a),
                STRUCT(2 as a),
                STRUCT(3 as a),
                STRUCT(4 as a)
            ])
        """
        )
        expected = strip_margin(
            """
            |+---+---+
            || a | c |
            |+---+---+
            || 1 | a |
            || 2 | b |
            || 3 | c |
            || 4 | c |
            |+---+---+
            |"""
        )
        with captured_output() as (stdout, stderr):
            a = f.col("a")

            df.withColumn(
                "c",
                f.when(a == f.lit(1), f.lit("a")).when(a == f.lit(2), f.lit("b")).otherwise(f.lit("c")),
            ).show()
            assert stdout.getvalue() == expected

    def test_when_without_bootstrap(self, bq: BigQueryBuilder):
        with pytest.raises(AttributeError):
            f.col("1").when(f.col("a") > f.lit(1), f.lit("ok"))

    def test_when_multiple_otherwise(self, bq: BigQueryBuilder):
        with pytest.raises(AttributeError):
            f.when(f.col("a") > f.lit(1), f.lit("ok")).otherwise(f.lit(1)).otherwise(f.lit(2))

    def test_coalesce_with_alias(self, bq: BigQueryBuilder):
        """
        Given a DataFrame
        When using `functions.coalesce` on columns with aliases
        Then aliases should be ignored
        """
        df = bq.sql(
            """
            SELECT
                *
            FROM UNNEST ([
                STRUCT(2 as a, 2 as b),
                STRUCT(2 as a, null as b),
                STRUCT(null as a, 2 as b)
            ])
        """
        )
        expected = strip_margin(
            """
            |+------+------+---+
            ||    a |    b | c |
            |+------+------+---+
            ||    2 |    2 | 2 |
            ||    2 | null | 2 |
            || null |    2 | 2 |
            |+------+------+---+
            |"""
        )
        with captured_output() as (stdout, stderr):
            a = f.col("a").alias("a")
            b = f.col("b").alias("b")
            # Operators must be compatible with literals, hence the "0 + a"
            df.withColumn("c", f.coalesce(a, b)).show()
            assert stdout.getvalue() == expected

    def test_struct_with_alias(self, bq: BigQueryBuilder):
        """
        Given a DataFrame
        When using `functions.coalesce` on columns with aliases
        Then aliases should be ignored
        """
        df = bq.sql(
            """
            SELECT
                *
            FROM UNNEST ([
                STRUCT(2 as a, 2 as b),
                STRUCT(2 as a, null as b),
                STRUCT(null as a, 2 as b)
            ])
        """
        )
        expected = strip_margin(
            """
            |+------+------+---------------------+
            ||    a |    b |                   c |
            |+------+------+---------------------+
            ||    2 |    2 |    {'a': 2, 'b': 2} |
            ||    2 | null | {'a': 2, 'b': None} |
            || null |    2 | {'a': None, 'b': 2} |
            |+------+------+---------------------+
            |"""
        )
        with captured_output() as (stdout, stderr):
            a = f.col("a").alias("a")
            b = f.col("b").alias("b")
            # Operators must be compatible with literals, hence the "0 + a"
            df.withColumn("c", f.struct(a, b)).show()
            assert stdout.getvalue() == expected


class TestTransform:
    def test_transform_on_simple_array(self, bq: BigQueryBuilder):
        """
        GIVEN a simple array
        WHEN we transform it
        THEN the result should correct
        """
        df = bq.sql("""SELECT ["x", "y"] as array_col""")
        df = df.withColumn("array_col", f.transform("array_col", lambda s: f.concat(s, f.lit("a"))), replace=True)
        expected = strip_margin(
            """
            |+--------------+
            ||    array_col |
            |+--------------+
            || ['xa', 'ya'] |
            |+--------------+"""
        )
        assert df.show_string() == expected

    def test_transform_with_sort_array_on_simple_array(self, bq: BigQueryBuilder):
        """
        GIVEN a simple array
        WHEN we transform and sort it, in whichever order
        THEN the result should be correct
        """
        df = bq.sql("""SELECT ["y", "x"] as array_col""")
        df1 = df.withColumn(
            "array_col", f.sort_array(f.transform(f.col("array_col"), lambda c: f.concat(c, f.lit("a")))), replace=True
        )
        df2 = df.withColumn(
            "array_col", f.transform(f.sort_array(f.col("array_col")), lambda c: f.concat(c, f.lit("a"))), replace=True
        )
        expected = strip_margin(
            """
            |+--------------+
            ||    array_col |
            |+--------------+
            || ['xa', 'ya'] |
            |+--------------+"""
        )
        assert df1.show_string() == expected
        assert df2.show_string() == expected

    def test_transform_on_array_of_struct(self, bq: BigQueryBuilder):
        """
        GIVEN an array of structs
        WHEN we transform it
        THEN the result should be correct
        """
        df = bq.sql(
            """
            SELECT * FROM UNNEST([
                STRUCT(1 as key, [STRUCT(1 as a, 2 as b), STRUCT(3 as a, 4 as b)] as s),
                STRUCT(2 as key, [STRUCT(5 as a, 6 as b), STRUCT(7 as a, 8 as b)] as s)
            ])
        """
        )
        df = df.withColumn(
            "s",
            f.transform("s", lambda s: f.struct((s["a"] + s["b"]).alias("c"), (s["a"] - s["b"]).alias("d"))),
            replace=True,
        )
        expected = strip_margin(
            """
            |+-----+------------------------------------------+
            || key |                                        s |
            |+-----+------------------------------------------+
            ||   1 |   [{'c': 3, 'd': -1}, {'c': 7, 'd': -1}] |
            ||   2 | [{'c': 11, 'd': -1}, {'c': 15, 'd': -1}] |
            |+-----+------------------------------------------+"""
        )
        assert df.show_string() == expected

    def test_transform_then_sort_array_on_array_of_struct(self, bq: BigQueryBuilder):
        """
        GIVEN an array of structs
        WHEN we transform then sort it
        THEN the result should be the same
        """
        df = bq.sql("""SELECT [STRUCT(5 as a, 1 as b), STRUCT(4 as a, 3 as b)] as s""")
        df = df.withColumn(
            "s",
            f.sort_array(
                f.transform("s", lambda s: f.struct((s["a"] + s["b"]).alias("c"), (s["a"] - s["b"]).alias("d"))),
                lambda s: s["d"],
            ),
            replace=True,
        )
        expected = strip_margin(
            """
            |+--------------------------------------+
            ||                                    s |
            |+--------------------------------------+
            || [{'c': 7, 'd': 1}, {'c': 6, 'd': 4}] |
            |+--------------------------------------+"""
        )
        assert df.show_string() == expected

    def test_sort_array_then_transform_on_array_of_struct(self, bq: BigQueryBuilder):
        """
        GIVEN an array of structs
        WHEN we transform then sort it
        THEN the result should be the same
        """
        df = bq.sql("""SELECT [STRUCT(5 as a, 1 as b), STRUCT(4 as a, 3 as b)] as s""")
        df = df.withColumn(
            "s",
            f.transform(
                f.sort_array(f.col("s"), lambda s: s["a"]),
                lambda s: f.struct((s["a"] + s["b"]).alias("c"), (s["a"] - s["b"]).alias("d")),
            ),
            replace=True,
        )
        expected = strip_margin(
            """
            |+--------------------------------------+
            ||                                    s |
            |+--------------------------------------+
            || [{'c': 7, 'd': 1}, {'c': 6, 'd': 4}] |
            |+--------------------------------------+"""
        )
        assert df.show_string() == expected

    def test_chained_transforms(self, bq: BigQueryBuilder):
        """
        GIVEN an array
        WHEN we chain two transformation on it
        THEN the result should be correct
        """
        df = bq.sql("""SELECT ["x", "y"] as array_col""")
        df = df.withColumn(
            "array_col",
            f.transform(f.transform("array_col", lambda s: f.concat(s, f.lit("a"))), lambda s: f.concat(s, f.lit("b"))),
            replace=True,
        )
        expected = strip_margin(
            """
            |+----------------+
            ||      array_col |
            |+----------------+
            || ['xab', 'yab'] |
            |+----------------+"""
        )
        assert df.show_string() == expected

    def test_transform_inside_a_transform(self, bq: BigQueryBuilder):
        """
        GIVEN an array of struct of arrays
        WHEN we perform a transformation inside a transformation
        THEN the result should be correct
        """
        df = bq.sql("""SELECT [STRUCT([1, 2] as b), STRUCT([3, 4] as b)] as a""")
        df = df.withColumn(
            "a",
            f.transform("a", lambda a: f.struct(f.transform(a["b"], lambda b: b.cast("FLOAT64")).alias("a"))),
            replace=True,
        )
        expected = strip_margin(
            """
            |+------------------------------+
            ||                            a |
            |+------------------------------+
            || [{[1.0, 2.0]}, {[3.0, 4.0]}] |
            |+------------------------------+"""
        )
        df.show(simplify_structs=True)
        assert df.show_string(simplify_structs=True) == expected

    def test_chained_sorts(self, bq: BigQueryBuilder):
        """
        GIVEN an array
        WHEN we chain two sorts on it
        THEN the result should be correct
        """
        df = bq.sql("""SELECT ["x", "y"] as array_col""")
        df = df.withColumn(
            "array_col",
            f.sort_array(f.sort_array(f.col("array_col"), f.desc)),
            replace=True,
        )
        expected = strip_margin(
            """
            |+------------+
            ||  array_col |
            |+------------+
            || ['x', 'y'] |
            |+------------+"""
        )
        assert df.show_string() == expected

    def test_sort_array_on_array_of_struct_with_multiple_sort_keys(self, bq: BigQueryBuilder):
        """
        GIVEN an array of structs
        WHEN we transform then sort it
        THEN the result should be the same
        """
        df = bq.sql("""SELECT [STRUCT(2 as a, "x" as b), STRUCT(1 as a, "z" as b),STRUCT(1 as a, "y" as b)] as s""")
        df = df.select(f.sort_array("s", lambda s: [s["a"], s["b"]]).alias("s"))
        expected = strip_margin(
            """
            |+--------------------------+
            ||                        s |
            |+--------------------------+
            || [{1, y}, {1, z}, {2, x}] |
            |+--------------------------+"""
        )
        assert df.show_string(simplify_structs=True) == expected
