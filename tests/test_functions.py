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
            expected = strip_margin("""
            |+------+-------+
            ||    a |     b |
            |+------+-------+
            ||    1 | False |
            ||    2 | False |
            || null |  True |
            |+------+-------+
            |""")
            self.assertEqual(expected, stdout.getvalue())
