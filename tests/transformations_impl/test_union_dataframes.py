import unittest

from bigquery_frame import BigQueryBuilder
from bigquery_frame.auth import get_bq_client
from bigquery_frame.transformations_impl.union_dataframes import union_dataframes


class TestUnionDataFrames(unittest.TestCase):

    def setUp(self) -> None:
        self.bigquery = BigQueryBuilder(get_bq_client())

    def tearDown(self) -> None:
        self.bigquery.close()

    def test_union_dataframes(self):
        df1 = self.bigquery.sql("""SELECT 1 as id""")
        df2 = self.bigquery.sql("""SELECT 2 as id""")
        df3 = self.bigquery.sql("""SELECT 3 as id""")
        actual = union_dataframes([df1, df2, df3])
        expected = self.bigquery.sql("""SELECT id FROM UNNEST([1, 2, 3]) as id""")
        self.assertEqual(actual.collect(), expected.collect())
