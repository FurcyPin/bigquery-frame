import unittest
from typing import List

from google.cloud.bigquery import Row

from bigquery_frame import BigQueryBuilder, DataFrame
from bigquery_frame.auth import get_bq_client
from bigquery_frame.transformations_impl.pivot_unpivot import (
    pivot, unpivot,
    __get_test_pivoted_df as get_test_pivoted_df,
    __get_test_unpivoted_df as get_test_unpivoted_df
)
from bigquery_frame.transformations_impl.union_dataframes import union_dataframes
from bigquery_frame.transformations_impl.analyze import __get_test_df as get_test_df, analyze


def get_expected() -> List[Row]:
    expected = [
            Row(('id', 'INTEGER', 9, 9, 0, '1', '9', [{'value': '1', 'count': 1}, {'value': '2', 'count': 1}, {'value': '3', 'count': 1}, {'value': '4', 'count': 1}, {'value': '5', 'count': 1}, {'value': '6', 'count': 1}, {'value': '7', 'count': 1}, {'value': '8', 'count': 1}, {'value': '9', 'count': 1}]), {'column_name': 0, 'column_type': 1, 'count': 2, 'count_distinct': 3, 'count_null': 4, 'min': 5, 'max': 6, 'approx_top_100': 7}),
            Row(('name', 'STRING', 9, 9, 0, 'Blastoise', 'Wartortle', [{'value': 'Bulbasaur', 'count': 1}, {'value': 'Ivysaur', 'count': 1}, {'value': 'Venusaur', 'count': 1}, {'value': 'Charmander', 'count': 1}, {'value': 'Charmeleon', 'count': 1}, {'value': 'Charizard', 'count': 1}, {'value': 'Squirtle', 'count': 1}, {'value': 'Wartortle', 'count': 1}, {'value': 'Blastoise', 'count': 1}]), {'column_name': 0, 'column_type': 1, 'count': 2, 'count_distinct': 3, 'count_null': 4, 'min': 5, 'max': 6, 'approx_top_100': 7}),
            Row(('types!', 'STRING', 13, 5, 0, 'Fire', 'Water', [{'value': 'Grass', 'count': 3}, {'value': 'Poison', 'count': 3}, {'value': 'Fire', 'count': 3}, {'value': 'Water', 'count': 3}, {'value': 'Flying', 'count': 1}]), {'column_name': 0, 'column_type': 1, 'count': 2, 'count_distinct': 3, 'count_null': 4, 'min': 5, 'max': 6, 'approx_top_100': 7}),
            Row(('evolution.can_evolve', 'BOOLEAN', 9, 2, 0, 'false', 'true', [{'value': 'true', 'count': 6}, {'value': 'false', 'count': 3}]), {'column_name': 0, 'column_type': 1, 'count': 2, 'count_distinct': 3, 'count_null': 4, 'min': 5, 'max': 6, 'approx_top_100': 7}),
            Row(('evolution.evolves_from', 'INTEGER', 9, 6, 3, '1', '8', [{'value': 'NULL', 'count': 3}, {'value': '1', 'count': 1}, {'value': '2', 'count': 1}, {'value': '4', 'count': 1}, {'value': '5', 'count': 1}, {'value': '7', 'count': 1}, {'value': '8', 'count': 1}]), {'column_name': 0, 'column_type': 1, 'count': 2, 'count_distinct': 3, 'count_null': 4, 'min': 5, 'max': 6, 'approx_top_100': 7})
    ]
    return expected


class TestUnionDataFrames(unittest.TestCase):

    def setUp(self) -> None:
        self.bigquery = BigQueryBuilder(get_bq_client())

    def tearDown(self) -> None:
        self.bigquery.close()

    def test_analyze(self):
        df = get_test_df()
        actual = analyze(df)
        self.assertEqual(get_expected(), actual.collect())

    def test_analyze_with_chunks(self):
        df = get_test_df()
        actual = analyze(df, _chunk_size=1)
        self.assertEqual(get_expected(), actual.collect())
