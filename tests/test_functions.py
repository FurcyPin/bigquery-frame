import unittest

from bigquery_frame import BigQueryBuilder
from bigquery_frame import functions as f
from bigquery_frame.auth import get_bq_client
from bigquery_frame.dataframe import strip_margin
from tests.utils import captured_output


class TestDataFrame(unittest.TestCase):
    def setUp(self) -> None:
        self.bigquery = BigQueryBuilder(get_bq_client())

    def tearDown(self) -> None:
        self.bigquery.close()

    def test_isnull_with_alias(self):
        """isnull should work on columns with an alias"""
        df = self.bigquery.sql("""SELECT * FROM UNNEST([1, 2, NULL]) as a""")

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
            self.assertEqual(expected, stdout.getvalue())

    def test_when(self):
        df = self.bigquery.sql(
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
            self.assertEqual(expected, stdout.getvalue())

    def test_when_without_bootstrap(self):
        with self.assertRaises(AttributeError):
            f.col("1").when(f.col("a") > f.lit(1), f.lit("ok"))

    def test_when_multiple_otherwise(self):
        with self.assertRaises(AttributeError):
            f.when(f.col("a") > f.lit(1), f.lit("ok")).otherwise(f.lit(1)).otherwise(f.lit(2))

    def test_coalesce_with_alias(self):
        """
        Given a DataFrame
        When using `functions.coalesce` on columns with aliases
        Then aliases should be ignored
        """
        df = self.bigquery.sql(
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
            self.assertEqual(expected, stdout.getvalue())

    def test_struct_with_alias(self):
        """
        Given a DataFrame
        When using `functions.coalesce` on columns with aliases
        Then aliases should be ignored
        """
        df = self.bigquery.sql(
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
            self.assertEqual(expected, stdout.getvalue())
