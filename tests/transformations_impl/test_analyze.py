import unittest
from typing import List

from google.cloud.bigquery import Row

from bigquery_frame import BigQueryBuilder
from bigquery_frame.auth import get_bq_client
from bigquery_frame.transformations_impl.analyze import __get_test_df as get_test_df
from bigquery_frame.transformations_impl.analyze import analyze

field_to_index = {
    "column_name": 0,
    "column_type": 1,
    "count": 2,
    "count_distinct": 3,
    "count_null": 4,
    "min": 5,
    "max": 6,
    "approx_top_100": 7,
}


def get_expected() -> List[Row]:
    # fmt: off
    expected = [
        Row((0, 'id', 'INTEGER', 9, 9, 0, '1', '9', [{'value': '1', 'count': 1}, {'value': '2', 'count': 1}, {'value': '3', 'count': 1}, {'value': '4', 'count': 1}, {'value': '5', 'count': 1}, {'value': '6', 'count': 1}, {'value': '7', 'count': 1}, {'value': '8', 'count': 1}, {'value': '9', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8}),  # noqa: E501
        Row((1, 'name', 'STRING', 9, 9, 0, 'Blastoise', 'Wartortle', [{'value': 'Bulbasaur', 'count': 1}, {'value': 'Ivysaur', 'count': 1}, {'value': 'Venusaur', 'count': 1}, {'value': 'Charmander', 'count': 1}, {'value': 'Charmeleon', 'count': 1}, {'value': 'Charizard', 'count': 1}, {'value': 'Squirtle', 'count': 1}, {'value': 'Wartortle', 'count': 1}, {'value': 'Blastoise', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8}),  # noqa: E501
        Row((2, 'types!', 'STRING', 13, 5, 0, 'Fire', 'Water', [{'value': 'Grass', 'count': 3}, {'value': 'Poison', 'count': 3}, {'value': 'Fire', 'count': 3}, {'value': 'Water', 'count': 3}, {'value': 'Flying', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8}),  # noqa: E501
        Row((3, 'evolution.can_evolve', 'BOOLEAN', 9, 2, 0, 'false', 'true', [{'value': 'true', 'count': 6}, {'value': 'false', 'count': 3}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8}),  # noqa: E501
        Row((4, 'evolution.evolves_from', 'INTEGER', 9, 6, 3, '1', '8', [{'value': 'NULL', 'count': 3}, {'value': '1', 'count': 1}, {'value': '2', 'count': 1}, {'value': '4', 'count': 1}, {'value': '5', 'count': 1}, {'value': '7', 'count': 1}, {'value': '8', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8})  # noqa: E501
    ]
    # fmt: on
    return expected


class TestAnalyze(unittest.TestCase):
    def setUp(self) -> None:
        self.bigquery = BigQueryBuilder(get_bq_client())

    def tearDown(self) -> None:
        self.bigquery.close()

    def test_analyze(self):
        df = get_test_df()
        actual = analyze(df)
        self.assertEqual(get_expected(), actual.collect())

    def test_analyze_with_keyword_column_names(self):
        """Analyze method should still work on DataFrames with columns names that collision with SQL keywords
        such as 'FROM'."""
        bq = BigQueryBuilder(get_bq_client())
        query = """SELECT 1 as `FROM`, STRUCT('a' as `ALL`) as `UNION`"""
        df = bq.sql(query)
        actual = analyze(df)
        # fmt: off
        expected = [
            Row((0, 'FROM', 'INTEGER', 1, 1, 0, '1', '1', [{'value': '1', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8}),  # noqa: E501
            Row((1, 'UNION.ALL', 'STRING', 1, 1, 0, 'a', 'a', [{'value': 'a', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8})  # noqa: E501
        ]
        # fmt: on
        self.assertEqual(expected, actual.collect())

    def test_analyze_with_array_struct_array(self):
        """
        GIVEN a DataFrame containing an ARRAY<STRUCT<ARRAY<INT>>>
        WHEN we analyze it
        THEN no crash should occur
        """
        bq = BigQueryBuilder(get_bq_client())
        query = """SELECT [STRUCT([1, 2, 3] as b)] as a"""
        df = bq.sql(query)
        actual = analyze(df)
        # fmt: off
        expected = [
            Row((0, 'a!.b!', 'INTEGER', 3, 3, 0, '1', '3', [{'value': '1', 'count': 1}, {'value': '2', 'count': 1}, {'value': '3', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8})  # noqa: E501
        ]
        # fmt: on
        self.assertEqual(expected, actual.collect())

    def test_analyze_with_bytes(self):
        """
        GIVEN a DataFrame containing a column of type bytes
        WHEN we analyze it
        THEN no crash should occur
        """
        bq = BigQueryBuilder(get_bq_client())
        query = r"""SELECT b'\377\340' as s"""
        df = bq.sql(query)
        actual = analyze(df)
        # fmt: off
        expected = [
            Row((0, 's', 'BYTES', 1, 1, 0, '/+A=', '/+A=', [{'value': '/+A=', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8})  # noqa: E501
        ]
        # fmt: on
        self.assertEqual(expected, actual.collect())

    def test_analyze_with_chunks(self):
        df = get_test_df()
        actual = analyze(df, _chunk_size=1)
        self.assertEqual(get_expected(), actual.collect())
