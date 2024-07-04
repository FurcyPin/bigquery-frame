from typing import Optional

from bigquery_frame import Column, DataFrame
from bigquery_frame import functions as f
from bigquery_frame.data_diff.diff_format_options import DiffFormatOptions
from bigquery_frame.data_diff.diff_per_col import _get_diff_per_col_df
from bigquery_frame.data_diff.diff_result import DiffResult
from bigquery_frame.data_diff.diff_result_summary import DiffResultSummary
from bigquery_frame.data_diff.diff_stats import print_diff_stats_shard
from bigquery_frame.data_diff.package import PREDICATES
from bigquery_frame.utils import MAX_JAVA_INT, quote, strip_margin


def _counts_changed_col() -> Column:
    """This method is used to make Sonar stop complaining about code duplicates"""
    return f.col("counts.changed")


def _diff_nb_col() -> Column:
    """This method is used to make Sonar stop complaining about code duplicates"""
    return f.col("diff.nb")


class DiffResultAnalyzer:
    def __init__(self, diff_format_options: Optional[DiffFormatOptions] = None) -> None:
        if diff_format_options is None:
            diff_format_options = DiffFormatOptions()
        self.diff_format_options = diff_format_options

    def _format_diff_df(self, join_cols: list[str], diff_df: DataFrame) -> DataFrame:
        """Given a diff DataFrame, rename the columns to prefix them with the left_df_alias and right_df_alias."""
        return diff_df.select(
            *[diff_df[quote(col_name)]["left_value"].alias(col_name) for col_name in join_cols],
            *[
                col
                for col_name in diff_df.columns
                if col_name not in join_cols
                for col in [
                    diff_df[quote(col_name)]["left_value"].alias(
                        f"{self.diff_format_options.left_df_alias}__{col_name}",
                    ),
                    diff_df[quote(col_name)]["right_value"].alias(
                        f"{self.diff_format_options.right_df_alias}__{col_name}",
                    ),
                ]
            ],
        )

    def _display_diff_examples(
        self,
        diff_df: DataFrame,
        diff_per_col_df: DataFrame,
        join_cols: list[str],
    ) -> None:
        """For each column that has differences, print examples of rows where such a difference occurs.

        Examples:
            >>> from bigquery_frame.data_diff.diff_result import _get_test_diff_result
            >>> diff_result = _get_test_diff_result()
            >>> analyzer = DiffResultAnalyzer(DiffFormatOptions(left_df_alias="before", right_df_alias="after"))
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

            >>> analyzer = DiffResultAnalyzer(DiffFormatOptions(left_df_alias="before", right_df_alias="after"))
            >>> diff_per_col_df = _get_test_diff_per_col_df()
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

            >>> analyzer._display_diff_examples(diff_df, diff_per_col_df, join_cols = ['id'])
            Detailed examples :
            'c2' : 3 rows
            +----+------------+-----------+
            | id | before__c2 | after__c2 |
            +----+------------+-----------+
            |  2 |          2 |         3 |
            |  3 |          2 |         4 |
            |  4 |          2 |         4 |
            +----+------------+-----------+

        """  # noqa: E501
        rows = (
            diff_per_col_df.where(~f.col("column_name").isin(*join_cols))
            .where(_counts_changed_col() > 0)
            .select("column_name", _counts_changed_col().alias("total_nb_differences"))
            .collect()
        )
        diff_count_per_col = [(r[0], r[1]) for r in rows]
        print("Detailed examples :")
        for col, nb in diff_count_per_col:
            print(f"'{col}' : {nb} rows")
            rows_that_changed_for_that_column = (
                diff_df.where(PREDICATES.present_in_both)
                .where(~f.col(quote(col))["is_equal"])
                .select(*join_cols, *[quote(r[0]) for r in rows])
            )
            self._format_diff_df(join_cols, rows_that_changed_for_that_column).show(
                self.diff_format_options.nb_diffed_rows,
            )

    @staticmethod
    def _display_changed(diff_per_col_df: DataFrame) -> None:
        """Displays the results of the diff analysis.

        We first display a summary of all columns that changed with the number of changes,
        then for each column, we display a summary of the most frequent changes and then
        we display examples of rows where this column changed, along with all the other columns
        that changed in this diff.

        Example:
        >>> diff_per_col_df = _get_test_diff_per_col_df()
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
        >>> DiffResultAnalyzer._display_changed(diff_per_col_df)
        +-------------+---------------+------------+-------------+----------------+
        | column_name | total_nb_diff | left_value | right_value | nb_differences |
        +-------------+---------------+------------+-------------+----------------+
        |          c2 |             3 |          2 |           4 |              2 |
        |          c2 |             3 |          2 |           3 |              1 |
        +-------------+---------------+------------+-------------+----------------+

        """
        df = diff_per_col_df.where(_counts_changed_col() > 0)

        explode_query = strip_margin(
            f"""
            |SELECT
            |  column_name,
            |  counts.changed as total_nb_diff,
            |  diff
            |FROM {quote(df._alias)}
            |JOIN UNNEST(diff.changed) as diff
            |ORDER BY column_number, diff.nb DESC
            |""",
        )
        df = df._apply_query(explode_query)
        df = df.select(
            "column_name",
            "total_nb_diff",
            "diff.left_value",
            "diff.right_value",
            _diff_nb_col().alias("nb_differences"),
        )
        df.show(MAX_JAVA_INT)

    @staticmethod
    def _display_only_in_left_or_right(
        diff_per_col_df: DataFrame,
        left_or_right: str,
    ) -> None:
        """Displays the results of the diff analysis.

        We first display a summary of all columns that changed with the number of changes,
        then for each column, we display a summary of the most frequent changes and then
        we display examples of rows where this column changed, along with all the other columns
        that changed in this diff.

        Example:
        >>> diff_per_col_df = _get_test_diff_per_col_df()
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
        >>> DiffResultAnalyzer._display_only_in_left_or_right(diff_per_col_df, "left")
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

        >>> DiffResultAnalyzer._display_only_in_left_or_right(diff_per_col_df, "right")
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
        explode_query = strip_margin(
            f"""
            |SELECT
            |  column_name,
            |  diff
            |FROM {quote(diff_per_col_df._alias)}
            |JOIN UNNEST(diff.only_in_{left_or_right}) as diff
            |""",
        )
        df = diff_per_col_df._apply_query(explode_query)

        df = df.select(
            "column_name",
            f.col("diff.value").alias("value"),
            _diff_nb_col().alias("nb"),
        )
        df.show(MAX_JAVA_INT)

    def display_diff_results(
        self,
        diff_result: DiffResult,
        show_examples: bool,
    ) -> None:
        join_cols = diff_result.join_cols
        diff_per_col_df = diff_result.get_diff_per_col_df(
            max_nb_rows_per_col_state=self.diff_format_options.nb_diffed_rows,
        )
        diff_stats_shards = diff_result.diff_stats_shards
        if diff_result.is_ok:
            print(f"\ndiff ok! ({diff_stats_shards[''].total} rows)\n")
            return
        print("\ndiff NOT ok\n")

        left_df_alias = self.diff_format_options.left_df_alias
        right_df_alias = self.diff_format_options.right_df_alias

        if len(diff_stats_shards) > 1:
            print(
                "WARNING: This diff has multiple granularity levels, "
                "we will print the results for each granularity level,\n"
                "         but we recommend to export the results to html for a much more digest result.\n",
            )

        for key, diff_stats_shard in diff_stats_shards.items():
            if len(diff_stats_shards) > 1:
                print("##############################################################")
                print(
                    f"Granularity : {'root' if key=='' else key} ({diff_stats_shard.total} rows)\n",
                )
            print_diff_stats_shard(diff_stats_shard, left_df_alias, right_df_alias)

            if diff_stats_shard.changed > 0:
                print("Found the following changes:")
                self._display_changed(diff_per_col_df)
                if show_examples:
                    self._display_diff_examples(
                        diff_result.diff_df_shards[key],
                        diff_per_col_df,
                        join_cols,
                    )
            if diff_stats_shard.only_in_left > 0:
                print(
                    f"{diff_stats_shard.only_in_left} rows were only found in '{left_df_alias}' :",
                )
                print(f"Most frequent values in '{left_df_alias}' for each column :")
                self._display_only_in_left_or_right(diff_per_col_df, "left")
            if diff_stats_shard.only_in_right > 0:
                print(
                    f"{diff_stats_shard.only_in_left} rows were only found in '{right_df_alias}' :",
                )
                print(f"Most frequent values in '{right_df_alias}' for each column :")
                self._display_only_in_left_or_right(diff_per_col_df, "right")

    def get_diff_result_summary(self, diff_result: DiffResult) -> DiffResultSummary:
        diff_per_col_df = diff_result.get_diff_per_col_df(
            max_nb_rows_per_col_state=self.diff_format_options.nb_diffed_rows,
        )
        summary = DiffResultSummary(
            left_df_alias=self.diff_format_options.left_df_alias,
            right_df_alias=self.diff_format_options.right_df_alias,
            diff_per_col_df=diff_per_col_df,
            schema_diff_result=diff_result.schema_diff_result,
            join_cols=diff_result.join_cols,
            same_schema=diff_result.same_schema,
            same_data=diff_result.same_data,
            total_nb_rows=diff_result.total_nb_rows,
        )
        return summary

    def get_diff_per_col_df(
        self,
        diff_result: DiffResult,
    ) -> DataFrame:
        return diff_result.get_diff_per_col_df(self.diff_format_options.nb_diffed_rows)


def _get_test_diff_per_col_df() -> DataFrame:
    """Return an example of diff_per_col_df for testing purposes.
    We intentionally sort top_per_col_state_df by increasing "nb" to simulate the fact that we don't have
    any way to guarantee that the diff arrays will be sorted by decreasing order of "nb" in the `diff` column.
    """
    from bigquery_frame.data_diff.diff_result import _get_test_diff_result

    diff_result = _get_test_diff_result()
    df = _get_diff_per_col_df(
        top_per_col_state_df=diff_result.top_per_col_state_df.orderBy("nb"),
        columns=diff_result.schema_diff_result.column_names,
        max_nb_rows_per_col_state=10,
    )
    return df
