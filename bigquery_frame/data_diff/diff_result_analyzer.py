from typing import List, Optional

from tqdm import tqdm

from bigquery_frame import DataFrame
from bigquery_frame import functions as f
from bigquery_frame import transformations as df_transformations
from bigquery_frame.data_diff.diff_format_options import DiffFormatOptions
from bigquery_frame.data_diff.diff_results import DiffResult
from bigquery_frame.data_diff.diff_stats import print_diff_stats
from bigquery_frame.data_diff.package import (
    EXISTS_COL_NAME,
    IS_EQUAL_COL_NAME,
    STRUCT_SEPARATOR_ALPHA,
    Predicates,
    canonize_col,
)
from bigquery_frame.transformations import analyze, union_dataframes
from bigquery_frame.utils import assert_true, quote, quote_columns, strip_margin


class DiffResultAnalyzer:
    def __init__(self, diff_format_options: Optional[DiffFormatOptions] = DiffFormatOptions()):
        self.diff_format_options = diff_format_options

    def _unpivot(self, diff_df: DataFrame, join_cols: List[str]):
        """Given a diff_df, builds an unpivoted version of it.
        All the values must be cast to STRING to make sure everything fits in the same column.

        >>> from bigquery_frame.data_diff.package import _get_test_intersection_diff_df
        >>> diff_df = _get_test_intersection_diff_df()
        >>> diff_df.show()  # noqa: E501
        +----+------------------------------------------------------------+--------------------------------------------------------+
        | id |                                                         c1 |                                                     c2 |
        +----+------------------------------------------------------------+--------------------------------------------------------+
        |  1 | {'left_value': 'a', 'right_value': 'd', 'is_equal': False} |  {'left_value': 1, 'right_value': 1, 'is_equal': True} |
        |  2 | {'left_value': 'b', 'right_value': 'a', 'is_equal': False} | {'left_value': 2, 'right_value': 4, 'is_equal': False} |
        +----+------------------------------------------------------------+--------------------------------------------------------+
        >>> DiffResultAnalyzer()._unpivot(diff_df, join_cols=['id']).orderBy('id', 'column').show()
        +----+------------------------------------------------------------+--------+
        | id |                                                       diff | column |
        +----+------------------------------------------------------------+--------+
        |  1 | {'left_value': 'a', 'right_value': 'd', 'is_equal': False} |     c1 |
        |  1 |  {'left_value': '1', 'right_value': '1', 'is_equal': True} |     c2 |
        |  2 | {'left_value': 'b', 'right_value': 'a', 'is_equal': False} |     c1 |
        |  2 | {'left_value': '2', 'right_value': '4', 'is_equal': False} |     c2 |
        +----+------------------------------------------------------------+--------+

        :param diff_df:
        :param join_cols:
        :return:
        """

        def truncate_string(col):
            return f.when(
                f.length(col) > f.lit(self.diff_format_options.max_string_length),
                f.concat(f.substring(col, 0, self.diff_format_options.max_string_length - 3), f.lit("...")),
            ).otherwise(col)

        diff_df = diff_df.select(
            *quote_columns(join_cols),
            *[
                f.struct(
                    truncate_string(
                        canonize_col(diff_df[field.name + ".left_value"], field.fields[0]).cast("STRING")
                    ).alias("left_value"),
                    truncate_string(
                        canonize_col(diff_df[field.name + ".right_value"], field.fields[0]).cast("STRING")
                    ).alias("right_value"),
                    diff_df[quote(field.name) + ".is_equal"].alias("is_equal"),
                ).alias(field.name)
                for field in diff_df.schema
                if field.name not in join_cols
            ],
        )

        unpivot = df_transformations.unpivot(
            diff_df, pivot_columns=join_cols, key_alias="column", value_alias="diff", implem_version=2
        )
        return unpivot

    @staticmethod
    def _build_diff_per_column_df(unpivoted_diff_df: DataFrame, nb_diffed_rows) -> DataFrame:
        """Given an `unpivoted_diff_df` DataFrame, builds a DataFrame that gives for each columns the N most frequent
        differences that are happening, where N = `nb_diffed_rows`.

        Example:

        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> unpivoted_diff_df = bq.sql('''  # noqa: E501
        ...     SELECT * FROM UNNEST([
        ...         STRUCT(1 as id, "c1" as column, STRUCT("a" as left_value, "d" as right_value, False as is_equal) as diff),
        ...         STRUCT(1 as id, "c2" as column, STRUCT("x" as left_value, "x" as right_value, True as is_equal) as diff),
        ...         STRUCT(1 as id, "c3" as column, STRUCT("1" as left_value, "1" as right_value, True as is_equal) as diff),
        ...         STRUCT(2 as id, "c1" as column, STRUCT("b" as left_value, "a" as right_value, False as is_equal) as diff),
        ...         STRUCT(2 as id, "c2" as column, STRUCT("y" as left_value, "y" as right_value, True as is_equal) as diff),
        ...         STRUCT(2 as id, "c3" as column, STRUCT("2" as left_value, "4" as right_value, False as is_equal) as diff),
        ...         STRUCT(3 as id, "c1" as column, STRUCT("c" as left_value, "f" as right_value, False as is_equal) as diff),
        ...         STRUCT(3 as id, "c2" as column, STRUCT("z" as left_value, "z" as right_value, True as is_equal) as diff),
        ...         STRUCT(3 as id, "c3" as column, STRUCT("3" as left_value, "3" as right_value, True as is_equal) as diff)
        ...     ])
        ... ''')
        >>> unpivoted_diff_df.show()
        +----+--------+------------------------------------------------------------+
        | id | column |                                                       diff |
        +----+--------+------------------------------------------------------------+
        |  1 |     c1 | {'left_value': 'a', 'right_value': 'd', 'is_equal': False} |
        |  1 |     c2 |  {'left_value': 'x', 'right_value': 'x', 'is_equal': True} |
        |  1 |     c3 |  {'left_value': '1', 'right_value': '1', 'is_equal': True} |
        |  2 |     c1 | {'left_value': 'b', 'right_value': 'a', 'is_equal': False} |
        |  2 |     c2 |  {'left_value': 'y', 'right_value': 'y', 'is_equal': True} |
        |  2 |     c3 | {'left_value': '2', 'right_value': '4', 'is_equal': False} |
        |  3 |     c1 | {'left_value': 'c', 'right_value': 'f', 'is_equal': False} |
        |  3 |     c2 |  {'left_value': 'z', 'right_value': 'z', 'is_equal': True} |
        |  3 |     c3 |  {'left_value': '3', 'right_value': '3', 'is_equal': True} |
        +----+--------+------------------------------------------------------------+
        >>> DiffResultAnalyzer._build_diff_per_column_df(unpivoted_diff_df, 1).orderBy('column').show()
        +--------+------------+-------------+----------------+----------------------+
        | column | left_value | right_value | nb_differences | total_nb_differences |
        +--------+------------+-------------+----------------+----------------------+
        |     c1 |          a |           d |              1 |                    3 |
        |     c3 |          2 |           4 |              1 |                    1 |
        +--------+------------+-------------+----------------+----------------------+

        :param unpivoted_diff_df: a diff DataFrame
        :return: a dict that gives for each column with differences the number of rows with different values
          for this column
        """
        # We must make sure to break ties on nb_differences when ordering to ensure a deterministic for unit tests.
        # window = Window.partitionBy(f.col("column")).orderBy(
        #     f.col("nb_differences").desc(), f.col("left_value"), f.col("right_value")
        # )
        #
        # df = (
        #     unpivoted_diff_df.filter("diff.is_equal = false")
        #     .groupBy("column", "diff.left_value", "diff.right_value")
        #     .agg(f.count(f.lit(1)).alias("nb_differences"))
        #     .withColumn("row_num", f.row_number().over(window))
        #     .withColumn("total_nb_differences", f.sum("nb_differences").over(Window.partitionBy("column")))
        #     .where(f.col("row_num") <= f.lit(nb_diffed_rows))
        #     .drop("row_num")
        # )
        query1 = strip_margin(
            f"""
        |SELECT
        |  column,
        |  diff.left_value as left_value,
        |  diff.right_value as right_value,
        |  COUNT(1) as nb_differences
        |FROM {unpivoted_diff_df._alias}
        |WHERE diff.is_equal = false
        |GROUP BY column, diff.left_value, diff.right_value
        |"""
        )
        df1 = unpivoted_diff_df._apply_query(query1)

        query2 = strip_margin(
            f"""
        |SELECT
        |  column,
        |  left_value,
        |  right_value,
        |  nb_differences,
        |  SUM(nb_differences) OVER (PARTITION BY column) as total_nb_differences
        |FROM {df1._alias}
        |QUALIFY
        |   ROW_NUMBER() OVER (PARTITION BY column ORDER BY nb_differences DESC, left_value, right_value)
        |   <= {nb_diffed_rows}
        |"""
        )
        df2 = df1._apply_query(query2)
        return df2.withColumn("column", f.replace(f.col("column"), STRUCT_SEPARATOR_ALPHA, "."), replace=True)

    def _get_diff_count_per_col(self, diff_df: DataFrame, join_cols: List[str]) -> DataFrame:
        """Given a diff_df and its list of join_cols, return a DataFrame with the following properties:

        - One row per "diff tuple" (col_name, col_value_left, col_value_right)
        - A column nb_differences that gives the number of occurrence of each "diff tuple"
        - A column total_nb_differences that gives total number of differences found for this col_name.

        >>> from bigquery_frame.data_diff.package import _get_test_intersection_diff_df
        >>> _diff_df = _get_test_intersection_diff_df()
        >>> _diff_df.show()  # noqa: E501
        +----+------------------------------------------------------------+--------------------------------------------------------+
        | id |                                                         c1 |                                                     c2 |
        +----+------------------------------------------------------------+--------------------------------------------------------+
        |  1 | {'left_value': 'a', 'right_value': 'd', 'is_equal': False} |  {'left_value': 1, 'right_value': 1, 'is_equal': True} |
        |  2 | {'left_value': 'b', 'right_value': 'a', 'is_equal': False} | {'left_value': 2, 'right_value': 4, 'is_equal': False} |
        +----+------------------------------------------------------------+--------------------------------------------------------+
        >>> analyzer = DiffResultAnalyzer(DiffFormatOptions(left_df_alias="before", right_df_alias="after"))
        >>> analyzer._get_diff_count_per_col(_diff_df, join_cols = ['id']).show()
        +--------+------------+-------------+----------------+----------------------+
        | column | left_value | right_value | nb_differences | total_nb_differences |
        +--------+------------+-------------+----------------+----------------------+
        |     c1 |          a |           d |              1 |                    2 |
        |     c1 |          b |           a |              1 |                    2 |
        |     c2 |          2 |           4 |              1 |                    1 |
        +--------+------------+-------------+----------------+----------------------+

        :param diff_df:
        :param join_cols:
        :return:
        """
        unpivoted_diff_df = self._unpivot(diff_df, join_cols)
        unpivoted_diff_df = unpivoted_diff_df.where("NOT COALESCE(diff.is_equal, FALSE)")
        diff_count_per_col_df = DiffResultAnalyzer._build_diff_per_column_df(
            unpivoted_diff_df, self.diff_format_options.nb_diffed_rows
        ).orderBy("column")
        return diff_count_per_col_df

    def _get_diff_count_per_col_for_shards(self, diff_shards: List[DataFrame], join_cols: List[str]) -> DataFrame:
        diff_count_per_col_df_shards = [
            self._get_diff_count_per_col(diff_df, join_cols).persist() for diff_df in tqdm(diff_shards)
        ]
        return union_dataframes(diff_count_per_col_df_shards)

    def _display_diff_count_per_col(self, diff_count_per_col_df: DataFrame):
        """Displays the results of the diff analysis.
        We first display the a summary of all columns that changed with the number of changes,
        then for each column, we display a summary of the most frequent changes and then
        we display examples of rows where this column changed, along with all the other columns
        that changed in this diff.

        Example:

        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> diff_count_per_col_df = bq.sql('''  # noqa: E501
        ...     SELECT * FROM UNNEST([
        ...         STRUCT('c1' as column, 'a' as left_value, 'd' as right_value, 1 as nb_differences, 3 as total_nb_differences),
        ...         STRUCT('c3' as column, '2' as left_value, '4' as right_value, 1 as nb_differences, 1 as total_nb_differences)
        ... ])''')
        >>> diff_count_per_col_df.show()
        +--------+------------+-------------+----------------+----------------------+
        | column | left_value | right_value | nb_differences | total_nb_differences |
        +--------+------------+-------------+----------------+----------------------+
        |     c1 |          a |           d |              1 |                    3 |
        |     c3 |          2 |           4 |              1 |                    1 |
        +--------+------------+-------------+----------------+----------------------+
        >>> analyzer = DiffResultAnalyzer(DiffFormatOptions(left_df_alias="before", right_df_alias="after"))
        >>> analyzer._display_diff_count_per_col(diff_count_per_col_df)
        Found the following differences:
        +-------------+---------------+--------+-------+----------------+
        | column_name | total_nb_diff | before | after | nb_differences |
        +-------------+---------------+--------+-------+----------------+
        |          c1 |             3 |      a |     d |              1 |
        |          c3 |             1 |      2 |     4 |              1 |
        +-------------+---------------+--------+-------+----------------+

        :param diff_count_per_col_df:
        :return:
        """
        print("Found the following differences:")
        diff_count_per_col_df.select(
            f.col("column").alias("`column_name`"),
            f.col("total_nb_differences").alias("`total_nb_diff`"),
            f.col("left_value").alias(quote(self.diff_format_options.left_df_alias)),
            f.col("right_value").alias(quote(self.diff_format_options.right_df_alias)),
            f.col("nb_differences").alias("`nb_differences`"),
        ).show(1000 * self.diff_format_options.nb_diffed_rows)

    def _format_diff_df(self, join_cols: List[str], diff_df: DataFrame) -> DataFrame:
        """Given a diff DataFrame, rename the columns to prefix them with the left_df_alias and right_df_alias.

        :param join_cols:
        :param diff_df:
        :return:
        """
        return diff_df.select(
            *quote_columns(join_cols),
            *[
                col
                for col_name in diff_df.columns
                if col_name not in join_cols
                for col in [
                    diff_df[quote(col_name)]["left_value"].alias(
                        f"{self.diff_format_options.left_df_alias}__{col_name}"
                    ),
                    diff_df[quote(col_name)]["right_value"].alias(
                        f"{self.diff_format_options.right_df_alias}__{col_name}"
                    ),
                ]
            ],
        )

    def _display_diff_examples(self, diff_df: DataFrame, diff_count_per_col_df: DataFrame, join_cols: List[str]):
        """For each column that has differences, print examples of rows where such a difference occurs.

        >>> from bigquery_frame.data_diff.package import _get_test_intersection_diff_df
        >>> _diff_df = _get_test_intersection_diff_df()
        >>> _diff_df.show()  # noqa: E501
        +----+------------------------------------------------------------+--------------------------------------------------------+
        | id |                                                         c1 |                                                     c2 |
        +----+------------------------------------------------------------+--------------------------------------------------------+
        |  1 | {'left_value': 'a', 'right_value': 'd', 'is_equal': False} |  {'left_value': 1, 'right_value': 1, 'is_equal': True} |
        |  2 | {'left_value': 'b', 'right_value': 'a', 'is_equal': False} | {'left_value': 2, 'right_value': 4, 'is_equal': False} |
        +----+------------------------------------------------------------+--------------------------------------------------------+
        >>> analyzer = DiffResultAnalyzer(DiffFormatOptions(left_df_alias="before", right_df_alias="after"))
        >>> _diff_count_per_col_df = analyzer._get_diff_count_per_col(_diff_df, join_cols = ['id'])
        >>> analyzer._display_diff_examples(_diff_df, _diff_count_per_col_df, join_cols = ['id'])
        Detailed examples :
        'c1' : 2 rows
        +----+------------+-----------+------------+-----------+
        | id | before__c1 | after__c1 | before__c2 | after__c2 |
        +----+------------+-----------+------------+-----------+
        |  1 |          a |         d |          1 |         1 |
        |  2 |          b |         a |          2 |         4 |
        +----+------------+-----------+------------+-----------+
        'c2' : 1 rows
        +----+------------+-----------+------------+-----------+
        | id | before__c1 | after__c1 | before__c2 | after__c2 |
        +----+------------+-----------+------------+-----------+
        |  2 |          b |         a |          2 |         4 |
        +----+------------+-----------+------------+-----------+

        :param diff_df:
        :param diff_count_per_col_df:
        :param join_cols:
        :return:
        """
        rows = diff_count_per_col_df.select("column", "total_nb_differences").distinct().collect()
        diff_count_per_col = [(r[0], r[1]) for r in rows]
        print("Detailed examples :")
        for col, nb in diff_count_per_col:
            print(f"'{col}' : {nb} rows")
            rows_that_changed_for_that_column = diff_df.filter(~diff_df[quote(col)]["is_equal"]).select(
                *join_cols, *[quote(r[0]) for r in rows]
            )
            self._format_diff_df(join_cols, rows_that_changed_for_that_column).show(
                self.diff_format_options.nb_diffed_rows
            )

    def _get_side_diff_df(self, diff_df: DataFrame, side: str, join_cols: List[str]) -> DataFrame:
        """Given a diff_df, compute the set of all values present only on the specified side.

        >>> from bigquery_frame.data_diff.package import _get_test_diff_df
        >>> _diff_df = _get_test_diff_df()
        >>> _diff_df.show()  # noqa: E501
        +----+-------------------------------------------------------------+-----------------------------------------------------------+--------------------------------------------+--------------+
        | id |                                                          c1 |                                                        c2 |                                 __EXISTS__ | __IS_EQUAL__ |
        +----+-------------------------------------------------------------+-----------------------------------------------------------+--------------------------------------------+--------------+
        |  1 |   {'left_value': 'a', 'right_value': 'a', 'is_equal': True} |     {'left_value': 1, 'right_value': 1, 'is_equal': True} |  {'left_value': True, 'right_value': True} |         True |
        |  2 |   {'left_value': 'b', 'right_value': 'b', 'is_equal': True} |    {'left_value': 2, 'right_value': 4, 'is_equal': False} |  {'left_value': True, 'right_value': True} |        False |
        |  3 | {'left_value': 'c', 'right_value': None, 'is_equal': False} | {'left_value': 3, 'right_value': None, 'is_equal': False} | {'left_value': True, 'right_value': False} |        False |
        |  4 | {'left_value': None, 'right_value': 'f', 'is_equal': False} | {'left_value': None, 'right_value': 3, 'is_equal': False} | {'left_value': False, 'right_value': True} |        False |
        +----+-------------------------------------------------------------+-----------------------------------------------------------+--------------------------------------------+--------------+
        >>> analyzer = DiffResultAnalyzer(DiffFormatOptions(left_df_alias="before", right_df_alias="after"))
        >>> analyzer._get_side_diff_df(_diff_df, side="left", join_cols=["id"]).show()
        +----+----+----+
        | id | c1 | c2 |
        +----+----+----+
        |  3 |  c |  3 |
        +----+----+----+
        >>> analyzer._get_side_diff_df(_diff_df, side="right", join_cols=["id"]).show()
        +----+----+----+
        | id | c1 | c2 |
        +----+----+----+
        |  4 |  f |  3 |
        +----+----+----+

        :param diff_df:
        :return:
        """
        assert_true(side in ["left", "right"])
        if side == "left":
            predicate = Predicates.only_in_left
        else:
            predicate = Predicates.only_in_right
        df = diff_df.filter(predicate).drop(EXISTS_COL_NAME, IS_EQUAL_COL_NAME)
        compared_cols = [
            f.col(f"{field.name}.{side}_value").alias(field.name) for field in df.schema if field.name not in join_cols
        ]
        return df.select(*join_cols, *compared_cols)

    def display_diff_results(self, diff_result: DiffResult, show_examples: bool):
        left_df_alias = self.diff_format_options.left_df_alias
        right_df_alias = self.diff_format_options.right_df_alias
        join_cols = diff_result.join_cols
        diff_stats = diff_result.diff_stats
        print_diff_stats(diff_result.diff_stats, left_df_alias, right_df_alias)
        if diff_stats.changed > 0:
            diff_count_per_col_df = self._get_diff_count_per_col_for_shards(diff_result.changed_df_shards, join_cols)
            self._display_diff_count_per_col(diff_count_per_col_df)
            if show_examples:
                self._display_diff_examples(diff_result.diff_df, diff_count_per_col_df, join_cols)
        if diff_stats.only_in_left > 0:
            left_only_df = self._get_side_diff_df(diff_result.diff_df, "left", join_cols).persist()
            print(f"{diff_stats.only_in_left} rows were only found in '{left_df_alias}' :")
            analyze(left_only_df).show(100000)
        if diff_stats.only_in_right > 0:
            right_only_df = self._get_side_diff_df(diff_result.diff_df, "right", join_cols).persist()
            print(f"{diff_stats.only_in_right} rows were only found in '{right_df_alias}':")
            analyze(right_only_df).show(100000)
