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
    def test_transform_on_array_of_struct(self, bq: BigQueryBuilder):
        """
        GIVEN a array of structs
        WHEN we transform it
        THEN the compiled expression should be correct
        """
        transform_col = f.struct((f.col("a") + f.col("b")).alias("c"), (f.col("a") - f.col("b")).alias("d"))
        actual = f.transform("s", transform_col)
        expected = strip_margin(
            """
            |ARRAY(
            |  SELECT
            |    STRUCT((`a`) + (`b`) as `c`, (`a`) - (`b`) as `d`)
            |  FROM UNNEST(`s`) as `_`
            |)"""
        )
        assert actual.expr == expected

    def test_transform_with_sort_array_on_array_of_struct(self, bq: BigQueryBuilder):
        """
        GIVEN a array of structs
        WHEN we transform and sort it, in whichever order
        THEN the compiled expression should be the same
        """
        transform_col = f.struct((f.col("a") + f.col("b")).alias("c"), (f.col("a") - f.col("b")).alias("d"))
        sort_cols = [f.col("a"), f.col("b")]
        actual_1 = f.sort_array(f.transform("s", transform_col), sort_cols)
        actual_2 = f.transform(f.sort_array("s", sort_cols), transform_col)
        expected = strip_margin(
            """
            |ARRAY(
            |  SELECT
            |    STRUCT((`a`) + (`b`) as `c`, (`a`) - (`b`) as `d`)
            |  FROM UNNEST(`s`) as `_`
            |  ORDER BY `a`, `b`
            |)"""
        )
        assert actual_1.expr == expected
        assert actual_2.expr == expected

    def test_transform_on_simple_array(self, bq: BigQueryBuilder):
        """
        GIVEN a simple array
        WHEN we transform it
        THEN the compiled expression should correct
        """
        actual = f.transform("s", f.col("_").cast("STRING"))
        expected = strip_margin(
            """
            |ARRAY(
            |  SELECT
            |    CAST(`_` as STRING)
            |  FROM UNNEST(`s`) as `_`
            |)"""
        )
        assert actual.expr == expected

    def test_transform_with_sort_array_on_simple_array(self, bq: BigQueryBuilder):
        """
        GIVEN a simple array
        WHEN we transform and sort it, in whichever order
        THEN the compiled expression should be the same
        """
        transform_col = f.col("_").cast("STRING")
        sort_cols = f.col("_").desc()
        actual_1 = f.sort_array(f.transform("s", transform_col), sort_cols)
        actual_2 = f.transform(f.sort_array("s", sort_cols), transform_col)
        expected = strip_margin(
            """
            |ARRAY(
            |  SELECT
            |    CAST(`_` as STRING)
            |  FROM UNNEST(`s`) as `_`
            |  ORDER BY `_` DESC
            |)"""
        )
        assert actual_1.expr == expected
        assert actual_2.expr == expected
