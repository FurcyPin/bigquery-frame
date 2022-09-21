from typing import List

from google.cloud.bigquery import Row

from bigquery_frame import BigQueryBuilder
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


def test_analyze(bq: BigQueryBuilder):
    df = get_test_df()
    actual = analyze(df)
    assert actual.collect() == get_expected()


def test_analyze_with_keyword_column_names(bq: BigQueryBuilder):
    """Analyze method should still work on DataFrames with columns names that collision with SQL keywords
    such as 'FROM'."""
    query = """SELECT 1 as `FROM`, STRUCT('a' as `ALL`) as `UNION`"""
    df = bq.sql(query)
    actual = analyze(df)
    # fmt: off
    expected = [
        Row((0, 'FROM', 'INTEGER', 1, 1, 0, '1', '1', [{'value': '1', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8}),  # noqa: E501
        Row((1, 'UNION.ALL', 'STRING', 1, 1, 0, 'a', 'a', [{'value': 'a', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8})  # noqa: E501
    ]
    # fmt: on
    assert actual.collect() == expected


def test_analyze_with_array_struct_array(bq: BigQueryBuilder):
    """
    GIVEN a DataFrame containing an ARRAY<STRUCT<ARRAY<INT>>>
    WHEN we analyze it
    THEN no crash should occur
    """
    query = """SELECT [STRUCT([1, 2, 3] as b)] as a"""
    df = bq.sql(query)
    actual = analyze(df)
    # fmt: off
    expected = [
        Row((0, 'a!.b!', 'INTEGER', 3, 3, 0, '1', '3', [{'value': '1', 'count': 1}, {'value': '2', 'count': 1}, {'value': '3', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8})  # noqa: E501
    ]
    # fmt: on
    assert actual.collect() == expected


def test_analyze_with_bytes(bq: BigQueryBuilder):
    """
    GIVEN a DataFrame containing a column of type bytes
    WHEN we analyze it
    THEN no crash should occur
    """
    query = r"""SELECT b'\377\340' as s"""
    df = bq.sql(query)
    actual = analyze(df)
    # fmt: off
    expected = [
        Row((0, 's', 'BYTES', 1, 1, 0, '/+A=', '/+A=', [{'value': '/+A=', 'count': 1}]), {'column_number': 0, 'column_name': 1, 'column_type': 2, 'count': 3, 'count_distinct': 4, 'count_null': 5, 'min': 6, 'max': 7, 'approx_top_100': 8})  # noqa: E501
    ]
    # fmt: on
    assert actual.collect() == expected


def test_analyze_with_nested_field_in_group_and_array_column(bq: BigQueryBuilder):
    """
    GIVEN a DataFrame containing a STRUCT and an array column
    WHEN we analyze it by grouping on a column inside this struct
    THEN no crash should occur
    """
    query = """SELECT 1 as id, STRUCT(2 as b, 3 as c) as a, [1, 2, 3] as arr"""
    df = bq.sql(query)
    actual = analyze(df, group_by="a.b")
    print(actual.collect())
    # fmt: off
    expected = [
        Row(({'b': 2}, 0, 'id', 'INTEGER', 1, 1, 0, '1', '1', [{'value': '1', 'count': 1}]), {'group': 0, 'column_number': 1, 'column_name': 2, 'column_type': 3, 'count': 4, 'count_distinct': 5, 'count_null': 6, 'min': 7, 'max': 8, 'approx_top_100': 9}),  # noqa: E501
        Row(({'b': 2}, 2, 'a.c', 'INTEGER', 1, 1, 0, '3', '3', [{'value': '3', 'count': 1}]), {'group': 0, 'column_number': 1, 'column_name': 2, 'column_type': 3, 'count': 4, 'count_distinct': 5, 'count_null': 6, 'min': 7, 'max': 8, 'approx_top_100': 9}),  # noqa: E501
        Row(({'b': 2}, 3, 'arr!', 'INTEGER', 3, 3, 0, '1', '3', [{'value': '1', 'count': 1}, {'value': '2', 'count': 1}, {'value': '3', 'count': 1}]), {'group': 0, 'column_number': 1, 'column_name': 2, 'column_type': 3, 'count': 4, 'count_distinct': 5, 'count_null': 6, 'min': 7, 'max': 8, 'approx_top_100': 9})  # noqa: E501
    ]
    # fmt: on
    assert actual.collect() == expected


def test_analyze_with_chunks(bq: BigQueryBuilder):
    df = get_test_df()
    actual = analyze(df, _chunk_size=1)
    assert actual.collect() == get_expected()
