from functools import lru_cache
from typing import TYPE_CHECKING, List

from bigquery_frame import BigQueryBuilder, DataFrame
from bigquery_frame import functions as f
from bigquery_frame import nested
from bigquery_frame.column import cols_to_str
from bigquery_frame.special_characters import _restore_special_characters
from bigquery_frame.transformations import pivot
from bigquery_frame.utils import strip_margin

if TYPE_CHECKING:
    from bigquery_frame.data_diff.diff_result import DiffResult


def _get_col_df(columns: List[str], bq: BigQueryBuilder) -> DataFrame:
    """Create a DataFrame listing the column names with their column number

    Examples:

        >>> from bigquery_frame import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df = _get_col_df(["id", "c1", "c2__ARRAY__a"], bq)
        >>> df.printSchema()
        root
         |-- column_number: INTEGER (NULLABLE)
         |-- column_name: STRING (NULLABLE)
        <BLANKLINE>
        >>> df.show()
        +---------------+-------------+
        | column_number | column_name |
        +---------------+-------------+
        |             0 |          id |
        |             1 |          c1 |
        |             2 |        c2!a |
        +---------------+-------------+
    """
    column_structs = [
        f.struct(f.lit(index), f.lit(_restore_special_characters(col))) for index, col in enumerate(columns)
    ]

    col_df = bq.sql(
        strip_margin(
            f"""
        |SELECT
        |  s[0] as column_number, s[1] as column_name
        |FROM UNNEST([
        |{cols_to_str(column_structs, indentation=2)}
        |]) as s"""
        )
    )
    return col_df


def _get_pivoted_df(
    top_per_col_state_df: DataFrame,
    max_nb_rows_per_col_state: int,
) -> DataFrame:
    """Pivot the top_per_col_state_df

    Examples:

        >>> from bigquery_frame.data_diff.diff_result import _get_test_diff_result
        >>> diff_result = _get_test_diff_result()
        >>> diff_result.top_per_col_state_df.orderBy("column_name", "state", "left_value", "right_value").show(100)
        +-------------+---------------+------------+-------------+----+---------+
        | column_name |         state | left_value | right_value | nb | row_num |
        +-------------+---------------+------------+-------------+----+---------+
        |          c1 |     no_change |          a |           a |  1 |       2 |
        |          c1 |     no_change |          b |           b |  3 |       1 |
        |          c1 |  only_in_left |          c |        null |  1 |       1 |
        |          c1 | only_in_right |       null |           f |  1 |       1 |
        |          c2 |       changed |          2 |           3 |  1 |       2 |
        |          c2 |       changed |          2 |           4 |  2 |       1 |
        |          c2 |     no_change |          1 |           1 |  1 |       1 |
        |          c2 |  only_in_left |          3 |        null |  1 |       1 |
        |          c2 | only_in_right |       null |           3 |  1 |       1 |
        |          c3 |  only_in_left |          1 |        null |  2 |       1 |
        |          c3 |  only_in_left |          2 |        null |  2 |       2 |
        |          c3 |  only_in_left |          3 |        null |  1 |       3 |
        |          c4 | only_in_right |       null |           1 |  2 |       1 |
        |          c4 | only_in_right |       null |           2 |  2 |       2 |
        |          c4 | only_in_right |       null |           3 |  1 |       3 |
        |          id |     no_change |          1 |           1 |  1 |       1 |
        |          id |     no_change |          2 |           2 |  1 |       2 |
        |          id |     no_change |          3 |           3 |  1 |       3 |
        |          id |     no_change |          4 |           4 |  1 |       4 |
        |          id |  only_in_left |          5 |        null |  1 |       1 |
        |          id | only_in_right |       null |           6 |  1 |       1 |
        +-------------+---------------+------------+-------------+----+---------+

        >>> _get_pivoted_df(diff_result.top_per_col_state_df, max_nb_rows_per_col_state=10).orderBy("column_name").show(simplify_structs=True)
        +-------------+------------+------------------------+--------------+----------------------------------------------+-----------------+--------------------------------------------+------------------+--------------------------------------------+
        | column_name | nb_changed |           diff_changed | nb_no_change |                               diff_no_change | nb_only_in_left |                          diff_only_in_left | nb_only_in_right |                         diff_only_in_right |
        +-------------+------------+------------------------+--------------+----------------------------------------------+-----------------+--------------------------------------------+------------------+--------------------------------------------+
        |          c1 |       None |                     [] |            4 |                       [{a, a, 1}, {b, b, 3}] |               1 |                             [{c, None, 1}] |                1 |                             [{None, f, 1}] |
        |          c2 |          3 | [{2, 3, 1}, {2, 4, 2}] |            1 |                                  [{1, 1, 1}] |               1 |                             [{3, None, 1}] |                1 |                             [{None, 3, 1}] |
        |          c3 |       None |                     [] |         None |                                           [] |               5 | [{1, None, 2}, {2, None, 2}, {3, None, 1}] |             None |                                         [] |
        |          c4 |       None |                     [] |         None |                                           [] |            None |                                         [] |                5 | [{None, 1, 2}, {None, 2, 2}, {None, 3, 1}] |
        |          id |       None |                     [] |            4 | [{1, 1, 1}, {2, 2, 1}, {3, 3, 1}, {4, 4, 1}] |               1 |                             [{5, None, 1}] |                1 |                             [{None, 6, 1}] |
        +-------------+------------+------------------------+--------------+----------------------------------------------+-----------------+--------------------------------------------+------------------+--------------------------------------------+
    """  # noqa: E501
    pivoted_df = pivot(
        top_per_col_state_df,
        pivot_column="state",
        aggs=[
            f.sum("nb").alias("nb"),
            f.array_agg(
                f.when(
                    f.col("row_num") <= f.lit(max_nb_rows_per_col_state),
                    f.struct(f.col("left_value"), f.col("right_value"), f.col("nb")),
                ),
                ignore_nulls=True,
                order_by=[f.col("left_value"), f.col("right_value")],
            ).alias("diff"),
        ],
        pivoted_columns=["changed", "no_change", "only_in_left", "only_in_right"],
    )
    return pivoted_df


def _format_diff_per_col_df(pivoted_df: DataFrame, col_df: DataFrame) -> DataFrame:
    """

    Examples:
        >>> from bigquery_frame.data_diff.diff_result import _get_test_diff_result
        >>> diff_result = _get_test_diff_result()
        >>> bq = diff_result.top_per_col_state_df.bigquery
        >>> columns = diff_result.schema_diff_result.column_names
        >>> col_df = _get_col_df(columns, bq)
        >>> pivoted_df = _get_pivoted_df(diff_result.top_per_col_state_df, max_nb_rows_per_col_state=10)
        >>> col_df.show()
        +---------------+-------------+
        | column_number | column_name |
        +---------------+-------------+
        |             0 |          id |
        |             1 |          c1 |
        |             2 |          c2 |
        |             3 |          c3 |
        |             4 |          c4 |
        +---------------+-------------+
        >>> pivoted_df.orderBy("column_name").show(simplify_structs=True)
        +-------------+------------+------------------------+--------------+----------------------------------------------+-----------------+--------------------------------------------+------------------+--------------------------------------------+
        | column_name | nb_changed |           diff_changed | nb_no_change |                               diff_no_change | nb_only_in_left |                          diff_only_in_left | nb_only_in_right |                         diff_only_in_right |
        +-------------+------------+------------------------+--------------+----------------------------------------------+-----------------+--------------------------------------------+------------------+--------------------------------------------+
        |          c1 |       None |                     [] |            4 |                       [{a, a, 1}, {b, b, 3}] |               1 |                             [{c, None, 1}] |                1 |                             [{None, f, 1}] |
        |          c2 |          3 | [{2, 3, 1}, {2, 4, 2}] |            1 |                                  [{1, 1, 1}] |               1 |                             [{3, None, 1}] |                1 |                             [{None, 3, 1}] |
        |          c3 |       None |                     [] |         None |                                           [] |               5 | [{1, None, 2}, {2, None, 2}, {3, None, 1}] |             None |                                         [] |
        |          c4 |       None |                     [] |         None |                                           [] |            None |                                         [] |                5 | [{None, 1, 2}, {None, 2, 2}, {None, 3, 1}] |
        |          id |       None |                     [] |            4 | [{1, 1, 1}, {2, 2, 1}, {3, 3, 1}, {4, 4, 1}] |               1 |                             [{5, None, 1}] |                1 |                             [{None, 6, 1}] |
        +-------------+------------+------------------------+--------------+----------------------------------------------+-----------------+--------------------------------------------+------------------+--------------------------------------------+

        >>> diff_per_col_df = _format_diff_per_col_df(pivoted_df, col_df)
        >>> nested.print_schema(diff_per_col_df)
        root
         |-- column_number: INTEGER (nullable = true)
         |-- column_name: STRING (nullable = true)
         |-- counts.total: INTEGER (nullable = true)
         |-- counts.changed: INTEGER (nullable = true)
         |-- counts.no_change: INTEGER (nullable = true)
         |-- counts.only_in_left: INTEGER (nullable = true)
         |-- counts.only_in_right: INTEGER (nullable = true)
         |-- diff.changed!.left_value: STRING (nullable = true)
         |-- diff.changed!.right_value: STRING (nullable = true)
         |-- diff.changed!.nb: INTEGER (nullable = true)
         |-- diff.no_change!.value: STRING (nullable = true)
         |-- diff.no_change!.nb: INTEGER (nullable = true)
         |-- diff.only_in_left!.value: STRING (nullable = true)
         |-- diff.only_in_left!.nb: INTEGER (nullable = true)
         |-- diff.only_in_right!.value: STRING (nullable = true)
         |-- diff.only_in_right!.nb: INTEGER (nullable = true)
        <BLANKLINE>

        >>> diff_per_col_df.show(simplify_structs=True)
        +---------------+-------------+-----------------+------------------------------------------------------------+
        | column_number | column_name |          counts |                                                       diff |
        +---------------+-------------+-----------------+------------------------------------------------------------+
        |             0 |          id | {6, 0, 4, 1, 1} | {[], [{1, 1}, {2, 1}, {3, 1}, {4, 1}], [{5, 1}], [{6, 1}]} |
        |             1 |          c1 | {6, 0, 4, 1, 1} |                 {[], [{a, 1}, {b, 3}], [{c, 1}], [{f, 1}]} |
        |             2 |          c2 | {6, 3, 1, 1, 1} |     {[{2, 3, 1}, {2, 4, 2}], [{1, 1}], [{3, 1}], [{3, 1}]} |
        |             3 |          c3 | {5, 0, 0, 5, 0} |                     {[], [], [{1, 2}, {2, 2}, {3, 1}], []} |
        |             4 |          c4 | {5, 0, 0, 0, 5} |                     {[], [], [], [{1, 2}, {2, 2}, {3, 1}]} |
        +---------------+-------------+-----------------+------------------------------------------------------------+

    """  # noqa: E501
    coalesced_df = col_df.join(pivoted_df, "column_name", "left").select(
        col_df["column_name"],
        col_df["column_number"],
        f.coalesce(pivoted_df["nb_changed"], f.lit(0)).alias("changed_nb"),
        f.coalesce(pivoted_df["diff_changed"], f.array()).alias("changed_diff"),
        f.coalesce(pivoted_df["nb_no_change"], f.lit(0)).alias("no_change_nb"),
        f.coalesce(pivoted_df["diff_no_change"], f.array()).alias("no_change_diff"),
        f.coalesce(pivoted_df["nb_only_in_left"], f.lit(0)).alias("only_in_left_nb"),
        f.coalesce(pivoted_df["diff_only_in_left"], f.array()).alias("only_in_left_diff"),
        f.coalesce(pivoted_df["nb_only_in_right"], f.lit(0)).alias("only_in_right_nb"),
        f.coalesce(pivoted_df["diff_only_in_right"], f.array()).alias("only_in_right_diff"),
    )
    total_col = f.col("changed_nb") + f.col("no_change_nb") + f.col("only_in_left_nb") + f.col("only_in_right_nb")
    renamed_df = coalesced_df.select(
        f.col("column_number"),
        f.col("column_name"),
        f.struct(
            total_col.alias("total"),
            f.col("changed_nb").alias("changed"),
            f.col("no_change_nb").alias("no_change"),
            f.col("only_in_left_nb").alias("only_in_left"),
            f.col("only_in_right_nb").alias("only_in_right"),
        ).alias("counts"),
        f.struct(
            f.col("changed_diff").alias("changed"),
            f.col("no_change_diff").alias("no_change"),
            f.col("only_in_left_diff").alias("only_in_left"),
            f.col("only_in_right_diff").alias("only_in_right"),
        ).alias("diff"),
    ).orderBy("column_number")
    formatted_df = renamed_df.transform(
        nested.select,
        {
            "column_number": None,
            "column_name": None,
            "counts": None,
            "diff.changed": None,
            "diff.no_change!.value": lambda s: s["left_value"],
            "diff.no_change!.nb": lambda s: s["nb"],
            "diff.only_in_left!.value": lambda s: s["left_value"],
            "diff.only_in_left!.nb": lambda s: s["nb"],
            "diff.only_in_right!.value": lambda s: s["right_value"],
            "diff.only_in_right!.nb": lambda s: s["nb"],
        },
    )
    return formatted_df.orderBy("column_number")


@lru_cache()
def _get_diff_per_col_df_with_cache(diff_result: "DiffResult", max_nb_rows_per_col_state: int) -> DataFrame:
    return _get_diff_per_col_df(
        top_per_col_state_df=diff_result.top_per_col_state_df,
        columns=diff_result.schema_diff_result.column_names,
        max_nb_rows_per_col_state=max_nb_rows_per_col_state,
    ).persist()


def _get_diff_per_col_df(
    top_per_col_state_df: DataFrame,
    columns: List[str],
    max_nb_rows_per_col_state: int,
) -> DataFrame:
    """Given a top_per_col_state_df, return a DataFrame that gives for each column and each
    column state (changed, no_change, only_in_left, only_in_right) the total number of occurences
    and the most frequent occurrences.

    !!! warning
        The arrays contained in the field `diff` are NOT guaranteed to be sorted.

    Args:
        top_per_col_state_df: A DataFrame with the following columns
            - column_name
            - state
            - left_value
            - right_value
            - nb
            - row_num
        columns: The list of column names to use. The column ordering given by this list is preserved.
        max_nb_rows_per_col_state: The maximal size of the arrays in `diff`

    Returns:
        A DataFrame with the following schema:

        root
         |-- column_number: INTEGER (nullable = true)
         |-- column_name: STRING (nullable = true)
         |-- counts.total: INTEGER (nullable = true)
         |-- counts.changed: INTEGER (nullable = true)
         |-- counts.no_change: INTEGER (nullable = true)
         |-- counts.only_in_left: INTEGER (nullable = true)
         |-- counts.only_in_right: INTEGER (nullable = true)
         |-- diff.changed!.left_value: STRING (nullable = true)
         |-- diff.changed!.right_value: STRING (nullable = true)
         |-- diff.changed!.nb: INTEGER (nullable = true)
         |-- diff.no_change!.value: STRING (nullable = true)
         |-- diff.no_change!.nb: INTEGER (nullable = true)
         |-- diff.only_in_left!.value: STRING (nullable = true)
         |-- diff.only_in_left!.nb: INTEGER (nullable = true)
         |-- diff.only_in_right!.value: STRING (nullable = true)
         |-- diff.only_in_right!.nb: INTEGER (nullable = true)
        <BLANKLINE>

    Examples:
        >>> from bigquery_frame.data_diff.diff_result import _get_test_diff_result
        >>> diff_result = _get_test_diff_result()
        >>> diff_df = diff_result.diff_df_shards[""]
        >>> diff_df.show(simplify_structs=True)
        +-------------------------------+-------------------------------+-------------------------------+-----------------------------------+-----------------------------------+---------------+--------------+
        |                            id |                            c1 |                            c2 |                                c3 |                                c4 |    __EXISTS__ | __IS_EQUAL__ |
        +-------------------------------+-------------------------------+-------------------------------+-----------------------------------+-----------------------------------+---------------+--------------+
        |      {1, 1, True, True, True} |      {a, a, True, True, True} |      {1, 1, True, True, True} |     {1, None, False, True, False} |     {None, 1, False, False, True} |  {True, True} |         True |
        |      {2, 2, True, True, True} |      {b, b, True, True, True} |     {2, 3, False, True, True} |     {1, None, False, True, False} |     {None, 1, False, False, True} |  {True, True} |        False |
        |      {3, 3, True, True, True} |      {b, b, True, True, True} |     {2, 4, False, True, True} |     {2, None, False, True, False} |     {None, 2, False, False, True} |  {True, True} |        False |
        |      {4, 4, True, True, True} |      {b, b, True, True, True} |     {2, 4, False, True, True} |     {2, None, False, True, False} |     {None, 2, False, False, True} |  {True, True} |        False |
        | {5, None, False, True, False} | {c, None, False, True, False} | {3, None, False, True, False} |     {3, None, False, True, False} | {None, None, False, False, False} | {True, False} |        False |
        | {None, 6, False, False, True} | {None, f, False, False, True} | {None, 3, False, False, True} | {None, None, False, False, False} |     {None, 3, False, False, True} | {False, True} |        False |
        +-------------------------------+-------------------------------+-------------------------------+-----------------------------------+-----------------------------------+---------------+--------------+

        >>> diff_result.top_per_col_state_df.orderBy("column_name", "state", "left_value", "right_value").show(100)
        +-------------+---------------+------------+-------------+----+---------+
        | column_name |         state | left_value | right_value | nb | row_num |
        +-------------+---------------+------------+-------------+----+---------+
        |          c1 |     no_change |          a |           a |  1 |       2 |
        |          c1 |     no_change |          b |           b |  3 |       1 |
        |          c1 |  only_in_left |          c |        null |  1 |       1 |
        |          c1 | only_in_right |       null |           f |  1 |       1 |
        |          c2 |       changed |          2 |           3 |  1 |       2 |
        |          c2 |       changed |          2 |           4 |  2 |       1 |
        |          c2 |     no_change |          1 |           1 |  1 |       1 |
        |          c2 |  only_in_left |          3 |        null |  1 |       1 |
        |          c2 | only_in_right |       null |           3 |  1 |       1 |
        |          c3 |  only_in_left |          1 |        null |  2 |       1 |
        |          c3 |  only_in_left |          2 |        null |  2 |       2 |
        |          c3 |  only_in_left |          3 |        null |  1 |       3 |
        |          c4 | only_in_right |       null |           1 |  2 |       1 |
        |          c4 | only_in_right |       null |           2 |  2 |       2 |
        |          c4 | only_in_right |       null |           3 |  1 |       3 |
        |          id |     no_change |          1 |           1 |  1 |       1 |
        |          id |     no_change |          2 |           2 |  1 |       2 |
        |          id |     no_change |          3 |           3 |  1 |       3 |
        |          id |     no_change |          4 |           4 |  1 |       4 |
        |          id |  only_in_left |          5 |        null |  1 |       1 |
        |          id | only_in_right |       null |           6 |  1 |       1 |
        +-------------+---------------+------------+-------------+----+---------+

        >>> diff_per_col_df = _get_diff_per_col_df(
        ...     diff_result.top_per_col_state_df,
        ...     diff_result.schema_diff_result.column_names,
        ...     max_nb_rows_per_col_state=10)
        >>> from bigquery_frame import nested
        >>> nested.print_schema(diff_per_col_df)
        root
         |-- column_number: INTEGER (nullable = true)
         |-- column_name: STRING (nullable = true)
         |-- counts.total: INTEGER (nullable = true)
         |-- counts.changed: INTEGER (nullable = true)
         |-- counts.no_change: INTEGER (nullable = true)
         |-- counts.only_in_left: INTEGER (nullable = true)
         |-- counts.only_in_right: INTEGER (nullable = true)
         |-- diff.changed!.left_value: STRING (nullable = true)
         |-- diff.changed!.right_value: STRING (nullable = true)
         |-- diff.changed!.nb: INTEGER (nullable = true)
         |-- diff.no_change!.value: STRING (nullable = true)
         |-- diff.no_change!.nb: INTEGER (nullable = true)
         |-- diff.only_in_left!.value: STRING (nullable = true)
         |-- diff.only_in_left!.nb: INTEGER (nullable = true)
         |-- diff.only_in_right!.value: STRING (nullable = true)
         |-- diff.only_in_right!.nb: INTEGER (nullable = true)
        <BLANKLINE>

        >>> diff_per_col_df.show(simplify_structs=True)
        +---------------+-------------+-----------------+------------------------------------------------------------+
        | column_number | column_name |          counts |                                                       diff |
        +---------------+-------------+-----------------+------------------------------------------------------------+
        |             0 |          id | {6, 0, 4, 1, 1} | {[], [{1, 1}, {2, 1}, {3, 1}, {4, 1}], [{5, 1}], [{6, 1}]} |
        |             1 |          c1 | {6, 0, 4, 1, 1} |                 {[], [{a, 1}, {b, 3}], [{c, 1}], [{f, 1}]} |
        |             2 |          c2 | {6, 3, 1, 1, 1} |     {[{2, 3, 1}, {2, 4, 2}], [{1, 1}], [{3, 1}], [{3, 1}]} |
        |             3 |          c3 | {5, 0, 0, 5, 0} |                     {[], [], [{1, 2}, {2, 2}, {3, 1}], []} |
        |             4 |          c4 | {5, 0, 0, 0, 5} |                     {[], [], [], [{1, 2}, {2, 2}, {3, 1}]} |
        +---------------+-------------+-----------------+------------------------------------------------------------+

        The following test demonstrates that the arrays in `diff`
        are not guaranteed to be sorted by decreasing frequency
        >>> _get_diff_per_col_df(
        ...     diff_result.top_per_col_state_df.orderBy("nb"),
        ...     diff_result.schema_diff_result.column_names,
        ...     max_nb_rows_per_col_state=10
        ... ).show(simplify_structs=True)
        +---------------+-------------+-----------------+------------------------------------------------------------+
        | column_number | column_name |          counts |                                                       diff |
        +---------------+-------------+-----------------+------------------------------------------------------------+
        |             0 |          id | {6, 0, 4, 1, 1} | {[], [{1, 1}, {2, 1}, {3, 1}, {4, 1}], [{5, 1}], [{6, 1}]} |
        |             1 |          c1 | {6, 0, 4, 1, 1} |                 {[], [{a, 1}, {b, 3}], [{c, 1}], [{f, 1}]} |
        |             2 |          c2 | {6, 3, 1, 1, 1} |     {[{2, 3, 1}, {2, 4, 2}], [{1, 1}], [{3, 1}], [{3, 1}]} |
        |             3 |          c3 | {5, 0, 0, 5, 0} |                     {[], [], [{1, 2}, {2, 2}, {3, 1}], []} |
        |             4 |          c4 | {5, 0, 0, 0, 5} |                     {[], [], [], [{1, 2}, {2, 2}, {3, 1}]} |
        +---------------+-------------+-----------------+------------------------------------------------------------+
    """  # noqa: E501
    bq = top_per_col_state_df.bigquery
    pivoted_df = _get_pivoted_df(top_per_col_state_df, max_nb_rows_per_col_state)
    col_df = _get_col_df(columns, bq)
    df = _format_diff_per_col_df(pivoted_df, col_df)
    return df
