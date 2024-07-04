from bigquery_frame import BigQueryBuilder, Column
from bigquery_frame import functions as f
from bigquery_frame.transformations_impl.analyze import __get_test_df as get_test_df
from bigquery_frame.transformations_impl.analyze import analyze
from bigquery_frame.utils import strip_margin


def _get_expected():
    return strip_margin(
        """
    |+---------------+------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------------------------------------------------------------------------------------------------------------------+
    || column_number |            column_name | column_type | count | count_distinct | count_null |       min |       max |                                                                                                                                 approx_top_100 |
    |+---------------+------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------------------------------------------------------------------------------------------------------------------+
    ||             0 |                     id |     INTEGER |     9 |              9 |          0 |         1 |         9 |                                                                       [{1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}, {6, 1}, {7, 1}, {8, 1}, {9, 1}] |
    ||             1 |                   name |      STRING |     9 |              9 |          0 | Blastoise | Wartortle | [{Bulbasaur, 1}, {Ivysaur, 1}, {Venusaur, 1}, {Charmander, 1}, {Charmeleon, 1}, {Charizard, 1}, {Squirtle, 1}, {Wartortle, 1}, {Blastoise, 1}] |
    ||             2 |                 types! |      STRING |    13 |              5 |          0 |      Fire |     Water |                                                                                  [{Grass, 3}, {Poison, 3}, {Fire, 3}, {Water, 3}, {Flying, 1}] |
    ||             3 |   evolution.can_evolve |     BOOLEAN |     9 |              2 |          0 |     false |      true |                                                                                                                        [{true, 6}, {false, 3}] |
    ||             4 | evolution.evolves_from |     INTEGER |     9 |              6 |          3 |         1 |         8 |                                                                                    [{NULL, 3}, {1, 1}, {2, 1}, {4, 1}, {5, 1}, {7, 1}, {8, 1}] |
    |+---------------+------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------------------------------------------------------------------------------------------------------------------+""",  # noqa: E501
    )


def test_analyze(bq: BigQueryBuilder):
    df = get_test_df()
    actual = analyze(df)
    actual.show(simplify_structs=True)
    assert actual.show_string(simplify_structs=True) == _get_expected()


def test_analyze_with_keyword_column_names(bq: BigQueryBuilder):
    """GIVEN a DataFrame containing field names that are reserved keywords
    WHEN we analyze it
    THEN no crash should occur
    """
    query = """SELECT 1 as `FROM`, STRUCT('a' as `ALL`) as `UNION`"""
    df = bq.sql(query)
    actual = analyze(df)
    assert actual.show_string(simplify_structs=True) == strip_margin(
        """
    |+---------------+-------------+-------------+-------+----------------+------------+-----+-----+----------------+
    || column_number | column_name | column_type | count | count_distinct | count_null | min | max | approx_top_100 |
    |+---------------+-------------+-------------+-------+----------------+------------+-----+-----+----------------+
    ||             0 |        FROM |     INTEGER |     1 |              1 |          0 |   1 |   1 |       [{1, 1}] |
    ||             1 |   UNION.ALL |      STRING |     1 |              1 |          0 |   a |   a |       [{a, 1}] |
    |+---------------+-------------+-------------+-------+----------------+------------+-----+-----+----------------+""",
    )


def test_analyze_with_array_struct_array(bq: BigQueryBuilder):
    """GIVEN a DataFrame containing an ARRAY<STRUCT<ARRAY<INT>>>
    WHEN we analyze it
    THEN no crash should occur
    """
    query = """SELECT [STRUCT([1, 2, 3] as b)] as a"""
    df = bq.sql(query)
    actual = analyze(df)
    assert actual.show_string(simplify_structs=True) == strip_margin(
        """
    |+---------------+-------------+-------------+-------+----------------+------------+-----+-----+--------------------------+
    || column_number | column_name | column_type | count | count_distinct | count_null | min | max |           approx_top_100 |
    |+---------------+-------------+-------------+-------+----------------+------------+-----+-----+--------------------------+
    ||             0 |       a!.b! |     INTEGER |     3 |              3 |          0 |   1 |   3 | [{1, 1}, {2, 1}, {3, 1}] |
    |+---------------+-------------+-------------+-------+----------------+------------+-----+-----+--------------------------+""",  # noqa: E501
    )


def test_analyze_with_bytes(bq: BigQueryBuilder):
    """GIVEN a DataFrame containing a column of type bytes
    WHEN we analyze it
    THEN no crash should occur
    """
    query = r"""SELECT b'\377\340' as s"""
    df = bq.sql(query)
    actual = analyze(df)
    assert actual.show_string(simplify_structs=True) == strip_margin(
        """
    |+---------------+-------------+-------------+-------+----------------+------------+------+------+----------------+
    || column_number | column_name | column_type | count | count_distinct | count_null |  min |  max | approx_top_100 |
    |+---------------+-------------+-------------+-------+----------------+------------+------+------+----------------+
    ||             0 |           s |       BYTES |     1 |              1 |          0 | /+A= | /+A= |    [{/+A=, 1}] |
    |+---------------+-------------+-------------+-------+----------------+------------+------+------+----------------+""",
    )


def test_analyze_with_nested_field_in_group_and_array_column(bq: BigQueryBuilder):
    """GIVEN a DataFrame containing a STRUCT and an array column
    WHEN we analyze it by grouping on a column inside this struct
    THEN no crash should occur
    """
    query = """SELECT 1 as id, STRUCT(2 as b, 3 as c) as a, [1, 2, 3] as arr"""
    df = bq.sql(query)
    actual = analyze(df, group_by="a.b")
    assert actual.show_string(simplify_structs=True) == strip_margin(
        """
    |+-------+---------------+-------------+-------------+-------+----------------+------------+-----+-----+--------------------------+
    || group | column_number | column_name | column_type | count | count_distinct | count_null | min | max |           approx_top_100 |
    |+-------+---------------+-------------+-------------+-------+----------------+------------+-----+-----+--------------------------+
    ||   {2} |             0 |          id |     INTEGER |     1 |              1 |          0 |   1 |   1 |                 [{1, 1}] |
    ||   {2} |             2 |         a.c |     INTEGER |     1 |              1 |          0 |   3 |   3 |                 [{3, 1}] |
    ||   {2} |             3 |        arr! |     INTEGER |     3 |              3 |          0 |   1 |   3 | [{1, 1}, {2, 1}, {3, 1}] |
    |+-------+---------------+-------------+-------------+-------+----------------+------------+-----+-----+--------------------------+""",  # noqa: E501
    )


def _build_huge_struct(value: Column, depth: int, width: int) -> Column:
    if depth == 0:
        return value.alias("s")
    return f.struct(*[_build_huge_struct(value, depth - 1, width).alias(f"c{i}") for i in range(width)])


def test_compare_df_with_huge_table(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT * FROM UNNEST([
            STRUCT(1 as id),
            STRUCT(2 as id),
            STRUCT(3 as id)
        ])
        """,
    )
    DEPTH = 3
    WIDTH = 5
    df = df.select("id", _build_huge_struct(f.lit(1), depth=DEPTH, width=WIDTH).alias("s")).persist()
    actual = analyze(df)
    actual.show(simplify_structs=True)


def test_analyze_with_chunks(bq: BigQueryBuilder):
    df = get_test_df()
    actual = analyze(df, _chunk_size=1)
    assert actual.show_string(simplify_structs=True) == _get_expected()
