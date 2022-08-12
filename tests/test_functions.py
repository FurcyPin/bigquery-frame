import unittest

from bigquery_frame import BigQueryBuilder
from bigquery_frame import functions as f
from bigquery_frame.auth import get_bq_client
from bigquery_frame.dataframe import strip_margin
from bigquery_frame.exceptions import IllegalArgumentException
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
        with self.assertRaises(IllegalArgumentException):
            f.col("1").when(f.col("a") > f.lit(1), f.lit("ok"))

    def test_when_multiple_otherwise(self):
        with self.assertRaises(IllegalArgumentException):
            f.when(f.col("a") > f.lit(1), f.lit("ok")).otherwise(f.lit(1)).otherwise(f.lit(2))
