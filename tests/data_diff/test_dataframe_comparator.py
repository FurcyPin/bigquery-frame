import pytest

from bigquery_frame import BigQueryBuilder
from bigquery_frame.data_diff.dataframe_comparator import DataframeComparator
from bigquery_frame.data_diff.diff_result_analyzer import DiffResultAnalyzer
from bigquery_frame.data_diff.diff_results import DiffResult
from bigquery_frame.data_diff.diff_stats import DiffStats


@pytest.fixture(autouse=True, scope="module")
def df_comparator():
    return DataframeComparator()


def test_compare_df_with_simplest(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    # fmt: off
    df = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as col1, "a" as col2),
            STRUCT(2 as col1, "b" as col2),
            STRUCT(3 as col1, NULL as col2)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df, df)
    expected_diff_stats = DiffStats(
        total=3, no_change=3, changed=0, in_left=3, in_right=3, only_in_left=0, only_in_right=0
    )
    assert diff_result.same_schema is True
    assert diff_result.is_ok is True
    assert diff_result.diff_stats == expected_diff_stats


def test_compare_df_with_ordering(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    # fmt: off
    df_1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, "a" as name),
            STRUCT(2 as id, "b" as name),
            STRUCT(3 as id, "c" as name)
       ])
    """)
    df_2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(2 as id, "b" as name),
            STRUCT(3 as id, "c" as name),
            STRUCT(1 as id, "a" as name)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df_1, df_2)
    expected_diff_stats = DiffStats(
        total=3, no_change=3, changed=0, in_left=3, in_right=3, only_in_left=0, only_in_right=0
    )
    assert diff_result.same_schema is True
    assert diff_result.is_ok is True
    assert diff_result.diff_stats == expected_diff_stats


def test_compare_df_with_empty_dataframes(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    # fmt: off
    df_1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, "a" as name),
            STRUCT(2 as id, "b" as name),
            STRUCT(3 as id, "c" as name)
       ]) LIMIT 0
    """)
    df_2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, "a" as name),
            STRUCT(2 as id, "b" as name),
            STRUCT(3 as id, "d" as name)
       ]) LIMIT 0
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df_1, df_2, join_cols=["id"])
    expected_diff_stats = DiffStats(
        total=0, no_change=0, changed=0, in_left=0, in_right=0, only_in_left=0, only_in_right=0
    )
    assert diff_result.same_schema is True
    assert diff_result.is_ok is True
    assert diff_result.diff_stats == expected_diff_stats


def test_compare_df_with_different_keys(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    # fmt: off
    df_1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, "a" as name),
            STRUCT(2 as id, "b" as name),
            STRUCT(3 as id, "c" as name)
       ])
    """)
    df_2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, "a" as name),
            STRUCT(2 as id, "b" as name),
            STRUCT(4 as id, "c" as name)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df_1, df_2, join_cols=["id"])
    print(diff_result.diff_stats)
    expected_diff_stats = DiffStats(
        total=4, no_change=2, changed=0, in_left=3, in_right=3, only_in_left=1, only_in_right=1
    )
    assert diff_result.same_schema is True
    assert diff_result.is_ok is False
    assert diff_result.diff_stats == expected_diff_stats


def test_compare_df_with_structs(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    # fmt: off
    df_1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, STRUCT(1 as a, 2 as b, 3 as c) as a),
            STRUCT(2 as id, STRUCT(1 as a, 2 as b, 3 as c) as a),
            STRUCT(3 as id, STRUCT(1 as a, 2 as b, 3 as c) as a)
       ])
    """)
    df_2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, STRUCT(1 as a, 2 as b, 3 as c) as a),
            STRUCT(2 as id, STRUCT(1 as a, 2 as b, 3 as c) as a),
            STRUCT(3 as id, STRUCT(1 as a, 2 as b, 4 as c) as a)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df_1, df_2, join_cols=["id"])
    expected_diff_stats = DiffStats(
        total=3, no_change=2, changed=1, in_left=3, in_right=3, only_in_left=0, only_in_right=0
    )
    assert diff_result.same_schema is True
    assert diff_result.is_ok is False
    assert diff_result.diff_stats == expected_diff_stats
    analyzer = DiffResultAnalyzer(df_comparator.diff_format_options)
    diff_count_per_col_df = analyzer._get_diff_count_per_col(diff_result.changed_df, join_cols=["id"])
    # We make sure that the displayed column name is 'a.c' and not 'a__DOT__c'
    assert diff_count_per_col_df.collect()[0].get("column") == "a.c"


def test_compare_df_with_struct_and_different_schemas(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    """A bug was happening when dataframes had different schemas"""
    # fmt: off
    df_1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, STRUCT(1 as a, 1 as b) as s1),
            STRUCT(2 as id, STRUCT(1 as a, 1 as b) as s1),
            STRUCT(3 as id, STRUCT(1 as a, 1 as b) as s1)
       ])
    """)
    df_2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, STRUCT(1 as a, 1 as c) as s1),
            STRUCT(2 as id, STRUCT(2 as a, 1 as c) as s1),
            STRUCT(3 as id, STRUCT(1 as a, 1 as c) as s1)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df_1, df_2, join_cols=["id"])
    expected_diff_stats = DiffStats(
        total=3, no_change=2, changed=1, in_left=3, in_right=3, only_in_left=0, only_in_right=0
    )
    assert diff_result.same_schema is False
    assert diff_result.is_ok is False
    assert diff_result.diff_stats == expected_diff_stats


def test_compare_df_with_arrays(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    # fmt: off
    df_1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, [1, 2, 3] as a),
            STRUCT(2 as id, [1, 2, 3] as a),
            STRUCT(3 as id, [1, 2, 3] as a)
       ])
    """)
    df_2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, [1, 2, 3] as a),
            STRUCT(2 as id, [3, 2, 1] as a),
            STRUCT(3 as id, [3, 1, 2] as a)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df_1, df_2)
    expected_diff_stats = DiffStats(
        total=3, no_change=3, changed=0, in_left=3, in_right=3, only_in_left=0, only_in_right=0
    )
    assert diff_result.same_schema is True
    assert diff_result.is_ok is True
    assert diff_result.diff_stats == expected_diff_stats


def test_compare_df_with_empty_and_null_arrays(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    """
    GIVEN two DataFrames, one with an empty array, the other with a null array
    WHEN we compare them
    THEN no difference should be found

    Explanation: even if the results are not identical, they will be AFTER they are persisted
    """
    # fmt: off
    df_1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, CAST([] AS ARRAY<INT>) as a)
       ])
    """)
    df_2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, CAST(NULL AS ARRAY<INT>) as a)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df_1, df_2)
    expected_diff_stats = DiffStats(
        total=1, no_change=1, changed=0, in_left=1, in_right=1, only_in_left=0, only_in_right=0
    )
    assert diff_result.same_schema is True
    assert diff_result.is_ok is True
    assert diff_result.diff_stats == expected_diff_stats


def test_compare_df_with_missing_empty_and_null_arrays(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    """
    GIVEN two DataFrames, one with a null and an empty array, the other without those rows
    WHEN we compare them
    THEN differences should be found

    Explanation: even if the results are not identical, they will be AFTER they are persisted
    """
    # fmt: off
    df_1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, [1] as a),
            STRUCT(2 as id, CAST([] AS ARRAY<INT>) as a),
            STRUCT(3 as id, CAST(NULL AS ARRAY<INT>) as a)
       ])
    """)
    df_2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, [2] as a)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df_1, df_2, join_cols=["id"])
    expected_diff_stats = DiffStats(
        total=3, no_change=0, changed=1, in_left=3, in_right=1, only_in_left=2, only_in_right=0
    )
    assert diff_result.same_schema is True
    assert diff_result.is_ok is False
    assert diff_result.diff_stats == expected_diff_stats
    analyzer = DiffResultAnalyzer(df_comparator.diff_format_options)
    diff_count_per_col_df = analyzer._get_diff_count_per_col_for_shards(diff_result.changed_df_shards, join_cols=["id"])
    assert diff_count_per_col_df.count() == 1


def test_compare_df_with_arrays_of_structs_ok(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    # fmt: off
    df_1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, [STRUCT(1 as a, "a" as b), STRUCT(2 as a, "a" as b), STRUCT(3 as a, "a" as b)] as a),
            STRUCT(2 as id, [STRUCT(1 as a, "b" as b), STRUCT(2 as a, "b" as b), STRUCT(3 as a, "b" as b)] as a),
            STRUCT(3 as id, [STRUCT(1 as a, "c" as b), STRUCT(2 as a, "c" as b), STRUCT(3 as a, "c" as b)] as a)
       ])
    """)
    df_2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, [STRUCT("a" as b, 3 as a), STRUCT("a" as b, 1 as a), STRUCT("a" as b, 2 as a)] as a),
            STRUCT(2 as id, [STRUCT("b" as b, 1 as a), STRUCT("b" as b, 3 as a), STRUCT("b" as b, 2 as a)] as a),
            STRUCT(3 as id, [STRUCT("c" as b, 3 as a), STRUCT("c" as b, 2 as a), STRUCT("c" as b, 1 as a)] as a)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df_1, df_2)
    expected_diff_stats = DiffStats(
        total=3, no_change=3, changed=0, in_left=3, in_right=3, only_in_left=0, only_in_right=0
    )
    assert diff_result.same_schema is False
    assert diff_result.same_data is True
    assert diff_result.is_ok is False
    assert diff_result.diff_stats == expected_diff_stats


def test_compare_df_with_arrays_of_structs_not_ok(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    # fmt: off
    df_1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, [STRUCT(1 as a, "a" as b), STRUCT(2 as a, "a" as b), STRUCT(3 as a, "a" as b)] as a),
            STRUCT(2 as id, [STRUCT(1 as a, "b" as b), STRUCT(2 as a, "b" as b), STRUCT(3 as a, "b" as b)] as a),
            STRUCT(3 as id, [STRUCT(1 as a, "c" as b), STRUCT(2 as a, "c" as b), STRUCT(3 as a, "c" as b)] as a)
       ])
    """)
    df_2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, [STRUCT("a" as b, 3 as a), STRUCT("a" as b, 1 as a), STRUCT("a" as b, 2 as a)] as a),
            STRUCT(2 as id, [STRUCT("b" as b, 1 as a), STRUCT("b" as b, 3 as a), STRUCT("b" as b, 2 as a)] as a),
            STRUCT(3 as id, [STRUCT("c" as b, 3 as a), STRUCT("c" as b, 2 as a), STRUCT("d" as b, 1 as a)] as a)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df_1, df_2)
    expected_diff_stats = DiffStats(
        total=3, no_change=2, changed=1, in_left=3, in_right=3, only_in_left=0, only_in_right=0
    )
    assert diff_result.same_schema is False
    assert diff_result.same_data is False
    assert diff_result.is_ok is False
    assert diff_result.diff_stats == expected_diff_stats


def test_compare_df_with_differing_types(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    # fmt: off
    df_1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, "a" as name),
            STRUCT(2 as id, "b" as name),
            STRUCT(3 as id, "c" as name)
       ])
    """)
    df_2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1.0 as id, "a" as name),
            STRUCT(2.0 as id, "b" as name),
            STRUCT(3.0 as id, "d" as name)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df_1, df_2)
    expected_diff_stats = DiffStats(
        total=3, no_change=2, changed=1, in_left=3, in_right=3, only_in_left=0, only_in_right=0
    )
    assert diff_result.same_schema is False
    assert diff_result.is_ok is False
    assert diff_result.diff_stats == expected_diff_stats


def test_compare_df_when_flattened_column_name_collision(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    """
    GIVEN a DataFrame with a nested column `s`.`a` and a column `s_a`
    WHEN we run a diff on it
    THEN it should not crash
    """
    # fmt: off
    df = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, STRUCT(1 as a) as s, 2 as s_a),
            STRUCT(2 as id, STRUCT(2 as a) as s, 3 as s_a),
            STRUCT(3 as id, STRUCT(3 as a) as s, 4 as s_a)
       ])
    """)
    # fmt: on
    diff_result: DiffResult = df_comparator.compare_df(df, df, join_cols=["id"])
    expected_diff_stats = DiffStats(
        total=3, no_change=3, changed=0, in_left=3, in_right=3, only_in_left=0, only_in_right=0
    )
    assert diff_result.same_schema is True
    assert diff_result.same_data is True
    assert diff_result.is_ok is True
    assert diff_result.diff_stats == expected_diff_stats


def test_compare_df_with_sharded_array_of_struct(bq: BigQueryBuilder, df_comparator: DataframeComparator):
    """
    GIVEN a DataFrame with a nested column `s`.`a` and a column `s_a`
    WHEN we run a diff on it
    THEN it should not crash
    """
    # fmt: off
    df1 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, [STRUCT(1 as a, 2 as b, 3 as c, 4 as d)] as my_array),
            STRUCT(2 as id, [STRUCT(1 as a, 2 as b, 3 as c, 4 as d)] as my_array),
            STRUCT(3 as id, [STRUCT(1 as a, 2 as b, 3 as c, 4 as d)] as my_array)
       ])
    """)
    df2 = bq.sql("""
        SELECT * FROM UNNEST ([
            STRUCT(1 as id, [STRUCT(1 as a, 2 as b, 3 as c, 4 as d)] as my_array),
            STRUCT(2 as id, [STRUCT(2 as a, 2 as b, 3 as c, 4 as d)] as my_array),
            STRUCT(4 as id, [STRUCT(1 as a, 2 as b, 3 as c, 4 as d)] as my_array)
       ])
    """)
    # fmt: on
    df_comparator = DataframeComparator(_shard_size=1)
    diff_result: DiffResult = df_comparator.compare_df(df1, df2, join_cols=["id"])
    expected_diff_stats = DiffStats(
        total=4, no_change=1, changed=1, in_left=3, in_right=3, only_in_left=1, only_in_right=1
    )
    assert diff_result.same_schema is True
    assert diff_result.same_data is False
    assert diff_result.is_ok is False
    assert diff_result.diff_stats == expected_diff_stats
    analyzer = DiffResultAnalyzer(df_comparator.diff_format_options)
    diff_count_per_col_df = analyzer._get_diff_count_per_col(diff_result.changed_df, join_cols=["id"])
    assert diff_count_per_col_df.count() == 1
