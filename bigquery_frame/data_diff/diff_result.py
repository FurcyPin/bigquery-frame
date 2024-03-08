from functools import cached_property
from typing import Dict, Generator, List, Optional

from bigquery_frame import DataFrame
from bigquery_frame import functions as f
from bigquery_frame import transformations
from bigquery_frame.conf import REPETITION_MARKER
from bigquery_frame.data_diff.diff_format_options import DiffFormatOptions
from bigquery_frame.data_diff.diff_per_col import _get_diff_per_col_df_with_cache
from bigquery_frame.data_diff.diff_stats import DiffStats
from bigquery_frame.data_diff.export import (
    DEFAULT_HTML_REPORT_ENCODING,
    DEFAULT_HTML_REPORT_OUTPUT_FILE_PATH,
    export_html_diff_report,
)
from bigquery_frame.data_diff.package import EXISTS_COL_NAME, IS_EQUAL_COL_NAME, PREDICATES, stringify_col
from bigquery_frame.data_diff.schema_diff import DiffPrefix, SchemaDiffResult
from bigquery_frame.field_utils import substring_before_last_occurrence
from bigquery_frame.special_characters import _restore_special_characters_from_col
from bigquery_frame.transformations import union_dataframes
from bigquery_frame.utils import quote


def _unpivot(diff_df: DataFrame) -> DataFrame:
    """Given a diff_df, builds an unpivoted version of it.
    All the values must be cast to STRING to make sure everything fits in the same column.

    Examples:
        >>> from bigquery_frame.data_diff.diff_result import _get_test_intersection_diff_df
        >>> diff_df = _get_test_intersection_diff_df()
        >>> diff_df.show()
        +-------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------+
        |                                                                                                    c1 |                                                                                                c2 |
        +-------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------+
        | {'left_value': 'a', 'right_value': 'd', 'is_equal': False, 'exists_left': True, 'exists_right': True} |  {'left_value': 1, 'right_value': 1, 'is_equal': True, 'exists_left': True, 'exists_right': True} |
        | {'left_value': 'b', 'right_value': 'a', 'is_equal': False, 'exists_left': True, 'exists_right': True} | {'left_value': 2, 'right_value': 4, 'is_equal': False, 'exists_left': True, 'exists_right': True} |
        +-------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------+
        >>> _unpivot(diff_df).orderBy('column_name').show()
        +-------------+-------------------------------------------------------------------------------------------------------+
        | column_name |                                                                                                  diff |
        +-------------+-------------------------------------------------------------------------------------------------------+
        |          c1 | {'left_value': 'b', 'right_value': 'a', 'is_equal': False, 'exists_left': True, 'exists_right': True} |
        |          c1 | {'left_value': 'a', 'right_value': 'd', 'is_equal': False, 'exists_left': True, 'exists_right': True} |
        |          c2 | {'left_value': '2', 'right_value': '4', 'is_equal': False, 'exists_left': True, 'exists_right': True} |
        |          c2 |  {'left_value': '1', 'right_value': '1', 'is_equal': True, 'exists_left': True, 'exists_right': True} |
        +-------------+-------------------------------------------------------------------------------------------------------+
    """  # noqa: E501

    diff_df = diff_df.select(
        *[
            f.struct(
                stringify_col(diff_df[field.name + ".left_value"], field.fields[0]).alias("left_value"),
                stringify_col(diff_df[field.name + ".right_value"], field.fields[1]).alias("right_value"),
                diff_df[quote(field.name) + ".is_equal"].alias("is_equal"),
                diff_df[quote(field.name) + ".exists_left"].alias("exists_left"),
                diff_df[quote(field.name) + ".exists_right"].alias("exists_right"),
            ).alias(field.name)
            for field in diff_df.schema
        ],
    )

    unpivoted_df = transformations.unpivot(
        diff_df, pivot_columns=[], key_alias="column_name", value_alias="diff", implem_version=1
    )
    unpivoted_df = unpivoted_df.withColumn(
        "column_name", _restore_special_characters_from_col(f.col("column_name")), replace=True
    )
    return unpivoted_df


class DiffResult:
    def __init__(
        self,
        schema_diff_result: SchemaDiffResult,
        diff_df_shards: Dict[str, DataFrame],
        join_cols: List[str],
    ) -> None:
        """Class containing the results of a diff between two DataFrames"""
        self.schema_diff_result: SchemaDiffResult = schema_diff_result
        self.diff_df_shards: Dict[str, DataFrame] = diff_df_shards
        """A dict containing one DataFrame for each level of granularity generated by the diff.

        The DataFrames have the following schema:

        - All fields from join_cols present at this level of granularity
        - For all other fields at this granularity level:
          a Column `col_name: STRUCT<left_value, right_value, is_equal>`
        - A Column `__EXISTS__: STRUCT<left_value, right_value>`
        - A Column `__IS_EQUAL__: BOOLEAN`

        In the simplest cases, there is only one granularity level, called the root level and represented
        by the string `""`. When comparing DataFrames containing arrays of structs, if the user passes a repeated
        field as join_cols (for example `"a!.id"`), then each level of granularity will be generated.
        In the example, there will be two: the root level `""` containing all root-level columns, and the
        level `"a!"` containing all the fields inside the exploded array `a!`; with one row per element inside `a!`.
        """
        self.join_cols: List[str] = join_cols
        """The list of column names to join"""

    @property
    def same_schema(self) -> bool:
        return self.schema_diff_result.same_schema

    @cached_property
    def same_data(self) -> bool:
        return (
            self.top_per_col_state_df.where(
                f.col("state") != f.lit("no_change"),
            ).count()
            == 0
        )

    @cached_property
    def total_nb_rows(self) -> int:
        a_join_col = next(col for col in self.join_cols if REPETITION_MARKER not in col)
        return self.top_per_col_state_df.where(
            f.col("column_name") == f.lit(a_join_col),
        ).count()

    @property
    def is_ok(self) -> bool:
        return self.same_schema and self.same_data

    @cached_property
    def diff_stats_shards(self) -> Dict[str, DiffStats]:
        return self._compute_diff_stats()

    @cached_property
    def top_per_col_state_df(self) -> DataFrame:
        def generate() -> Generator[DataFrame, None, None]:
            for key, diff_df in self.diff_df_shards.items():
                keep_cols = [
                    col_name
                    for col_name in self.schema_diff_result.column_names
                    if substring_before_last_occurrence(col_name, "!.") == key
                ]
                df = self._compute_top_per_col_state_df(diff_df)
                yield df.where(f.col("column_name").isin(*keep_cols))

        return union_dataframes(list(generate())).persist()

    def get_diff_per_col_df(self, max_nb_rows_per_col_state: int) -> DataFrame:
        """Return a Dict[str, int] that gives for each column and each column state (changed, no_change, only_in_left,
        only_in_right) the total number of occurences and the most frequent occurrences.

        The results returned by this method are cached to avoid unecessary recomputations.

        !!! warning
            The arrays contained in the field `diff` are NOT guaranteed to be sorted.

        Args:
            max_nb_rows_per_col_state: The maximal size of the arrays in `diff`

        Returns:
            A DataFrame with the following schema:

                root
                 |-- column_number: integer (nullable = true)
                 |-- column_name: string (nullable = true)
                 |-- counts.total: long (nullable = false)
                 |-- counts.changed: long (nullable = false)
                 |-- counts.no_change: long (nullable = false)
                 |-- counts.only_in_left: long (nullable = false)
                 |-- counts.only_in_right: long (nullable = false)
                 |-- diff.changed!.left_value: string (nullable = true)
                 |-- diff.changed!.right_value: string (nullable = true)
                 |-- diff.changed!.nb: long (nullable = false)
                 |-- diff.no_change!.value: string (nullable = true)
                 |-- diff.no_change!.nb: long (nullable = false)
                 |-- diff.only_in_left!.value: string (nullable = true)
                 |-- diff.only_in_left!.nb: long (nullable = false)
                 |-- diff.only_in_right!.value: string (nullable = true)
                 |-- diff.only_in_right!.nb: long (nullable = false)
                <BLANKLINE>

        Examples:
            >>> from bigquery_frame.data_diff.diff_result import _get_test_diff_result
            >>> diff_result = _get_test_diff_result()
            >>> diff_result.diff_df_shards[''].show(simplify_structs=True)
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

            >>> diff_result.top_per_col_state_df.orderBy("column_name", "state", "left_value", "right_value").show(100, simplify_structs=True)
            +-------------+---------------+------------+-------------+----+---------+
            | column_name |         state | left_value | right_value | nb | row_num |
            +-------------+---------------+------------+-------------+----+---------+
            |          c1 |     no_change |          a |           a |  1 |       2 |
            |          c1 |     no_change |          b |           b |  3 |       1 |
            |          c1 |  only_in_left |          c |        None |  1 |       1 |
            |          c1 | only_in_right |       None |           f |  1 |       1 |
            |          c2 |       changed |          2 |           3 |  1 |       2 |
            |          c2 |       changed |          2 |           4 |  2 |       1 |
            |          c2 |     no_change |          1 |           1 |  1 |       1 |
            |          c2 |  only_in_left |          3 |        None |  1 |       1 |
            |          c2 | only_in_right |       None |           3 |  1 |       1 |
            |          c3 |  only_in_left |          1 |        None |  2 |       1 |
            |          c3 |  only_in_left |          2 |        None |  2 |       2 |
            |          c3 |  only_in_left |          3 |        None |  1 |       3 |
            |          c4 | only_in_right |       None |           1 |  2 |       1 |
            |          c4 | only_in_right |       None |           2 |  2 |       2 |
            |          c4 | only_in_right |       None |           3 |  1 |       3 |
            |          id |     no_change |          1 |           1 |  1 |       1 |
            |          id |     no_change |          2 |           2 |  1 |       2 |
            |          id |     no_change |          3 |           3 |  1 |       3 |
            |          id |     no_change |          4 |           4 |  1 |       4 |
            |          id |  only_in_left |          5 |        None |  1 |       1 |
            |          id | only_in_right |       None |           6 |  1 |       1 |
            +-------------+---------------+------------+-------------+----+---------+

            >>> diff_per_col_df = diff_result.get_diff_per_col_df(max_nb_rows_per_col_state=10)
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
        """  # noqa: E501
        return _get_diff_per_col_df_with_cache(self, max_nb_rows_per_col_state)

    def _compute_diff_stats_shard(self, diff_df_shard: DataFrame) -> DiffStats:
        """Given a diff_df and its list of join_cols, return stats about the number of differing or missing rows

        >>> from bigquery_frame.data_diff.diff_result import _get_test_diff_result
        >>> diff_result = _get_test_diff_result()
        >>> diff_result.diff_df_shards[''].select('__EXISTS__', '__IS_EQUAL__').show(simplify_structs=True)
        +---------------+--------------+
        |    __EXISTS__ | __IS_EQUAL__ |
        +---------------+--------------+
        |  {True, True} |         True |
        |  {True, True} |        False |
        |  {True, True} |        False |
        |  {True, True} |        False |
        | {True, False} |        False |
        | {False, True} |        False |
        +---------------+--------------+

        >>> diff_result._compute_diff_stats()['']
        DiffStats(total=6, no_change=1, changed=3, in_left=5, in_right=5, only_in_left=1, only_in_right=1)
        """
        res_df = diff_df_shard.select(
            f.count(f.lit(1)).alias("total"),
            f.sum(f.when(PREDICATES.present_in_both & PREDICATES.row_is_equal, f.lit(1),).otherwise(f.lit(0)),).alias(
                "no_change",
            ),
            f.sum(f.when(PREDICATES.present_in_both & PREDICATES.row_changed, f.lit(1),).otherwise(f.lit(0)),).alias(
                "changed",
            ),
            f.sum(f.when(PREDICATES.in_left, f.lit(1)).otherwise(f.lit(0))).alias(
                "in_left",
            ),
            f.sum(f.when(PREDICATES.in_right, f.lit(1)).otherwise(f.lit(0))).alias(
                "in_right",
            ),
            f.sum(f.when(PREDICATES.only_in_left, f.lit(1)).otherwise(f.lit(0))).alias(
                "only_in_left",
            ),
            f.sum(f.when(PREDICATES.only_in_right, f.lit(1)).otherwise(f.lit(0))).alias(
                "only_in_right",
            ),
        )
        res = res_df.collect()
        return DiffStats(
            **{k: (v if v is not None else 0) for k, v in dict(res[0]).items()},
        )

    def _compute_diff_stats(self) -> Dict[str, DiffStats]:
        """Given a diff_df and its list of join_cols, return stats about the number of differing or missing rows

        >>> from bigquery_frame.data_diff.diff_result import _get_test_diff_result
        >>> diff_result = _get_test_diff_result()
        >>> diff_result.diff_df_shards[''].select('__EXISTS__', '__IS_EQUAL__').show(simplify_structs=True)
        +---------------+--------------+
        |    __EXISTS__ | __IS_EQUAL__ |
        +---------------+--------------+
        |  {True, True} |         True |
        |  {True, True} |        False |
        |  {True, True} |        False |
        |  {True, True} |        False |
        | {True, False} |        False |
        | {False, True} |        False |
        +---------------+--------------+

        >>> diff_result._compute_diff_stats()['']
        DiffStats(total=6, no_change=1, changed=3, in_left=5, in_right=5, only_in_left=1, only_in_right=1)
        """
        return {
            key: self._compute_diff_stats_shard(diff_df_shard) for key, diff_df_shard in self.diff_df_shards.items()
        }

    def _compute_top_per_col_state_df(self, diff_df: DataFrame) -> DataFrame:
        """Given a diff_df, return a DataFrame with the following properties:

        - One row per tuple (column_name, state, left_value, right_value)
          (where `state` can take the following values: "only_in_left", "only_in_right", "no_change", "changed")
        - A column `nb` that gives the number of occurrence of this specific tuple
        - At most `max_nb_rows_per_col_state` per tuple (column_name, state). Rows with the highest "nb" are kept first.

        Examples:
            >>> from bigquery_frame.data_diff.diff_result import _get_test_diff_result
            >>> _diff_result = _get_test_diff_result()
            >>> diff_df = _diff_result.diff_df_shards['']
            >>> diff_df.show()
            +-------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+--------------------------------------------+--------------+
            |                                                                                                    id |                                                                                                      c1 |                                                                                                    c2 |                                                                                                        c3 |                                                                                                        c4 |                                 __EXISTS__ | __IS_EQUAL__ |
            +-------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+--------------------------------------------+--------------+
            |      {'left_value': 1, 'right_value': 1, 'is_equal': True, 'exists_left': True, 'exists_right': True} |    {'left_value': 'a', 'right_value': 'a', 'is_equal': True, 'exists_left': True, 'exists_right': True} |      {'left_value': 1, 'right_value': 1, 'is_equal': True, 'exists_left': True, 'exists_right': True} |     {'left_value': 1, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} |     {'left_value': None, 'right_value': 1, 'is_equal': False, 'exists_left': False, 'exists_right': True} |  {'left_value': True, 'right_value': True} |         True |
            |      {'left_value': 2, 'right_value': 2, 'is_equal': True, 'exists_left': True, 'exists_right': True} |    {'left_value': 'b', 'right_value': 'b', 'is_equal': True, 'exists_left': True, 'exists_right': True} |     {'left_value': 2, 'right_value': 3, 'is_equal': False, 'exists_left': True, 'exists_right': True} |     {'left_value': 1, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} |     {'left_value': None, 'right_value': 1, 'is_equal': False, 'exists_left': False, 'exists_right': True} |  {'left_value': True, 'right_value': True} |        False |
            |      {'left_value': 3, 'right_value': 3, 'is_equal': True, 'exists_left': True, 'exists_right': True} |    {'left_value': 'b', 'right_value': 'b', 'is_equal': True, 'exists_left': True, 'exists_right': True} |     {'left_value': 2, 'right_value': 4, 'is_equal': False, 'exists_left': True, 'exists_right': True} |     {'left_value': 2, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} |     {'left_value': None, 'right_value': 2, 'is_equal': False, 'exists_left': False, 'exists_right': True} |  {'left_value': True, 'right_value': True} |        False |
            |      {'left_value': 4, 'right_value': 4, 'is_equal': True, 'exists_left': True, 'exists_right': True} |    {'left_value': 'b', 'right_value': 'b', 'is_equal': True, 'exists_left': True, 'exists_right': True} |     {'left_value': 2, 'right_value': 4, 'is_equal': False, 'exists_left': True, 'exists_right': True} |     {'left_value': 2, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} |     {'left_value': None, 'right_value': 2, 'is_equal': False, 'exists_left': False, 'exists_right': True} |  {'left_value': True, 'right_value': True} |        False |
            | {'left_value': 5, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} | {'left_value': 'c', 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} | {'left_value': 3, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} |     {'left_value': 3, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} | {'left_value': None, 'right_value': None, 'is_equal': False, 'exists_left': False, 'exists_right': False} | {'left_value': True, 'right_value': False} |        False |
            | {'left_value': None, 'right_value': 6, 'is_equal': False, 'exists_left': False, 'exists_right': True} | {'left_value': None, 'right_value': 'f', 'is_equal': False, 'exists_left': False, 'exists_right': True} | {'left_value': None, 'right_value': 3, 'is_equal': False, 'exists_left': False, 'exists_right': True} | {'left_value': None, 'right_value': None, 'is_equal': False, 'exists_left': False, 'exists_right': False} |     {'left_value': None, 'right_value': 3, 'is_equal': False, 'exists_left': False, 'exists_right': True} | {'left_value': False, 'right_value': True} |        False |
            +-------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+--------------------------------------------+--------------+
            >>> (_diff_result._compute_top_per_col_state_df(diff_df)
            ...  .orderBy("column_name", "state", "left_value", "right_value")
            ... ).show(100)
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
        """  # noqa: E501
        unpivoted_diff_df = _unpivot(diff_df.drop(IS_EQUAL_COL_NAME, EXISTS_COL_NAME))

        only_in_left = f.col("diff")["exists_left"] & ~f.col("diff")["exists_right"]
        only_in_right = ~f.col("diff")["exists_left"] & f.col("diff")["exists_right"]
        exists_in_left_or_right = f.col("diff")["exists_left"] | f.col("diff")["exists_right"]

        df_1 = unpivoted_diff_df.where(exists_in_left_or_right).select(
            "column_name",
            f.when(only_in_left, f.lit("only_in_left"))
            .when(only_in_right, f.lit("only_in_right"))
            .when(f.col("diff")["is_equal"], f.lit("no_change"))
            .otherwise(f.lit("changed"))
            .alias("state"),
            "diff.left_value",
            "diff.right_value",
        )

        df_2 = (
            df_1.groupBy("column_name", "state", "left_value", "right_value")
            .agg(f.count(f.lit(1)).alias("nb"))
            .withColumn(
                "row_num",
                f.expr("ROW_NUMBER() OVER (PARTITION BY column_name, state ORDER BY nb DESC, left_value, right_value)"),
            )
        )
        return df_2

    def display(
        self,
        show_examples: bool = False,
        diff_format_options: Optional[DiffFormatOptions] = None,
    ) -> None:
        """Print a summary of the results in the standard output

        Args:
            show_examples: If true, display example of rows for each type of change
            diff_format_options: Formatting options

        Examples:
            See [bigquery_frame.data_diff.compare_dataframes][bigquery_frame.data_diff.compare_dataframes]
            for more examples.

            >>> from bigquery_frame.data_diff.diff_result import _get_test_diff_result
            >>> diff_result = _get_test_diff_result()
            >>> diff_result.display()
            Schema: ok (0)
            <BLANKLINE>
            diff NOT ok
            <BLANKLINE>
            Row count ok: 5 rows
            <BLANKLINE>
            1 (16.67%) rows are identical
            3 (50.0%) rows have changed
            1 (16.67%) rows are only in 'left'
            1 (16.67%) rows are only in 'right
            <BLANKLINE>
            Found the following changes:
            +-------------+---------------+------------+-------------+----------------+
            | column_name | total_nb_diff | left_value | right_value | nb_differences |
            +-------------+---------------+------------+-------------+----------------+
            |          c2 |             3 |          2 |           4 |              2 |
            |          c2 |             3 |          2 |           3 |              1 |
            +-------------+---------------+------------+-------------+----------------+
            1 rows were only found in 'left' :
            Most frequent values in 'left' for each column :
            +-------------+-------+----+
            | column_name | value | nb |
            +-------------+-------+----+
            |          id |     5 |  1 |
            |          c1 |     c |  1 |
            |          c2 |     3 |  1 |
            |          c3 |     1 |  2 |
            |          c3 |     2 |  2 |
            |          c3 |     3 |  1 |
            +-------------+-------+----+
            1 rows were only found in 'right' :
            Most frequent values in 'right' for each column :
            +-------------+-------+----+
            | column_name | value | nb |
            +-------------+-------+----+
            |          id |     6 |  1 |
            |          c1 |     f |  1 |
            |          c2 |     3 |  1 |
            |          c4 |     1 |  2 |
            |          c4 |     2 |  2 |
            |          c4 |     3 |  1 |
            +-------------+-------+----+
        """
        if diff_format_options is None:
            diff_format_options = DiffFormatOptions()
        from bigquery_frame.data_diff.diff_result_analyzer import DiffResultAnalyzer

        self.schema_diff_result.display()
        analyzer = DiffResultAnalyzer(diff_format_options)
        analyzer.display_diff_results(self, show_examples)

    def export_to_html(
        self,
        title: Optional[str] = None,
        output_file_path: str = DEFAULT_HTML_REPORT_OUTPUT_FILE_PATH,
        encoding: str = DEFAULT_HTML_REPORT_ENCODING,
        diff_format_options: Optional[DiffFormatOptions] = None,
    ) -> None:
        """Generate an HTML report of this diff result.

        This generates a file named diff_report.html in the current working directory.
        It can be open directly with a web browser.

        Args:
            title: The title of the report
            encoding: Encoding used when writing the html report
            output_file_path: Path of the file to write to
            diff_format_options: Formatting options

        Examples:
            >>> from bigquery_frame.data_diff.diff_result import _get_test_diff_result
            >>> diff_result = _get_test_diff_result()
            >>> diff_result.export_to_html("Example Diff", "test_working_dir/example_diff.html")
            Report exported as test_working_dir/example_diff.html

            [Check out the exported report here](../diff_reports/example_diff.html)
        """
        if diff_format_options is None:
            diff_format_options = DiffFormatOptions()
        from bigquery_frame.data_diff.diff_result_analyzer import DiffResultAnalyzer

        analyzer = DiffResultAnalyzer(diff_format_options)
        diff_result_summary = analyzer.get_diff_result_summary(self)
        export_html_diff_report(
            diff_result_summary,
            title=title,
            output_file_path=output_file_path,
            encoding=encoding,
        )


def _get_test_diff_result() -> "DiffResult":
    from bigquery_frame.data_diff.package import _get_test_diff_df

    _diff_df = _get_test_diff_df()
    column_names_diff = {
        "id": DiffPrefix.UNCHANGED,
        "c1": DiffPrefix.UNCHANGED,
        "c2": DiffPrefix.UNCHANGED,
        "c3": DiffPrefix.REMOVED,
        "c4": DiffPrefix.ADDED,
    }
    schema_diff_result = SchemaDiffResult(
        same_schema=True,
        diff_str="",
        nb_cols=0,
        column_names_diff=column_names_diff,
    )
    return DiffResult(
        schema_diff_result,
        diff_df_shards={"": _diff_df},
        join_cols=["id"],
    )


def _get_test_intersection_diff_df() -> DataFrame:
    from bigquery_frame import BigQueryBuilder

    bq = BigQueryBuilder()
    diff_df = bq.sql(
        """
        SELECT * FROM UNNEST([
            STRUCT(
                STRUCT("a" as left_value, "d" as right_value, False as is_equal,
                       True as exists_left, True as exists_right
                ) as c1,
                STRUCT(1 as left_value, 1 as right_value, True as is_equal,
                       True as exists_left, True as exists_right
                ) as c2
            ),
            STRUCT(
                STRUCT("b" as left_value, "a" as right_value, False as is_equal,
                       True as exists_left, True as exists_right
                ) as c1,
                STRUCT(2 as left_value, 4 as right_value, False as is_equal,
                       True as exists_left, True as exists_right
                ) as c2
            )
        ])
    """,
    )
    return diff_df
