import unittest

from bigquery_frame import BigQueryBuilder
from bigquery_frame.auth import get_bq_client
from bigquery_frame.transformations_impl.pivot_unpivot import (
    pivot, unpivot,
    __get_test_pivoted_df as get_test_pivoted_df,
    __get_test_unpivoted_df as get_test_unpivoted_df
)


class TestPivotUnpivot(unittest.TestCase):

    bigquery = BigQueryBuilder(get_bq_client())

    def test_pivot_v1(self):
        df = get_test_unpivoted_df()
        pivoted = pivot(df, pivot_column="country", agg_fun="sum", agg_col="amount", implem_version=1)
        expected = get_test_pivoted_df()
        self.assertEqual(expected.collect(), pivoted.collect())

    def test_pivot_v1_case_sensitive(self):
        df = get_test_unpivoted_df()
        pivoted = pivot(df, pivot_column="COUNTRY", agg_fun="SUM", agg_col="AMOUNT", implem_version=1)
        expected = get_test_pivoted_df()
        self.assertEqual(expected.collect(), pivoted.collect())

    def test_pivot_v2(self):
        df = get_test_unpivoted_df()
        pivoted = pivot(df, pivot_column="country", agg_fun="sum", agg_col="amount", implem_version=2)
        expected = get_test_pivoted_df()
        self.assertEqual(expected.collect(), pivoted.collect())

    def test_pivot_v2_case_sensitive(self):
        df = get_test_unpivoted_df()
        pivoted = pivot(df, pivot_column="COUNTRY", agg_fun="SUM", agg_col="AMOUNT", implem_version=2)
        expected = get_test_pivoted_df()
        self.assertEqual(expected.collect(), pivoted.collect())

    def test_unpivot_v1(self):
        df = get_test_pivoted_df()
        unpivoted = unpivot(df, ['year', 'product'], key_alias='country', value_alias='amount', implem_version=1)
        expected = get_test_unpivoted_df()
        self.assertEqual(expected.collect(), unpivoted.collect())

    def test_unpivot_v2(self):
        df = get_test_pivoted_df()
        unpivoted = unpivot(df, ['year', 'product'], key_alias='country', value_alias='amount', implem_version=2)
        unpivoted = unpivoted.select('year', 'product', 'country', 'amount')
        expected = get_test_unpivoted_df()
        self.assertEqual(expected.sort("year", "product", "country").collect(), unpivoted.sort("year", "product", "country").collect())

    def test_unpivot_v1_exclude_nulls(self):
        df = get_test_pivoted_df()
        unpivoted = unpivot(df, ['year', 'product'], key_alias='country', value_alias='amount', exclude_nulls=True, implem_version=1)
        expected = get_test_unpivoted_df().where('amount IS NOT NULL')
        self.assertEqual(expected.collect(), unpivoted.collect())

    def test_unpivot_v2_exclude_nulls(self):
        df = get_test_pivoted_df()
        unpivoted = unpivot(df, ['year', 'product'], key_alias='country', value_alias='amount', exclude_nulls=True, implem_version=2)
        unpivoted = unpivoted.select('year', 'product', 'country', 'amount')
        expected = get_test_unpivoted_df().where('amount IS NOT NULL')
        self.assertEqual(expected.sort("year", "product", "country").collect(), unpivoted.sort("year", "product", "country").collect())
