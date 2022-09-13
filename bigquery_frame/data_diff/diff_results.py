from typing import List, Optional

from bigquery_frame import DataFrame
from bigquery_frame import functions as f
from bigquery_frame.column import cols_to_str
from bigquery_frame.data_diff.diff_format_options import DiffFormatOptions
from bigquery_frame.data_diff.diff_stats import DiffStats
from bigquery_frame.data_diff.package import (
    EXISTS_COL_NAME,
    IS_EQUAL_COL_NAME,
    Predicates,
)
from bigquery_frame.utils import quote, str_to_col, strip_margin


class SchemaDiffResult:
    def __init__(self, same_schema: bool, diff_str: str, nb_cols: int):
        self.diff_str = diff_str
        self.nb_cols = nb_cols
        self.same_schema = same_schema

    def display(self):
        if not self.same_schema:
            print(f"Schema has changed:\n{self.diff_str}")
            print("WARNING: columns that do not match both sides will be ignored")
            return False
        else:
            print(f"Schema: ok (%s {self.nb_cols})")
            return True


class DiffResult:
    def __init__(
        self,
        schema_diff_result: SchemaDiffResult,
        diff_shards: List[DataFrame],
        join_cols: List[str],
        diff_format_options: Optional[DiffFormatOptions] = DiffFormatOptions(),
    ):
        self.schema_diff_result = schema_diff_result
        self.diff_shards = diff_shards
        self.join_cols = join_cols
        self.diff_format_options = diff_format_options
        self._changed_df_shards = None
        self._changed_df = None
        self._diff_df = None
        self._diff_stats = None

    @property
    def same_schema(self):
        return self.schema_diff_result.same_schema

    @property
    def diff_stats(self):
        if self._diff_stats is None:
            self._diff_stats = self._compute_diff_stats()
        return self._diff_stats

    @property
    def same_data(self):
        return self.diff_stats.same_data

    @property
    def is_ok(self):
        return self.same_schema and self.same_data

    @property
    def diff_df(self) -> DataFrame:
        """The DataFrame containing all rows that were found in both DataFrames.
        WARNING: for very wide tables (~1000 columns) using this DataFrame might crash, and it is recommended
        to handle each diff_shard separately"""
        if self._diff_df is None:
            self._diff_df = DiffResult._join_dataframes(*self.diff_shards, join_cols=self.join_cols)
        return self._diff_df

    @property
    def changed_df(self) -> DataFrame:
        """The DataFrame containing all rows that were found in both DataFrames but are not equal"""
        if self._changed_df is None:
            self._changed_df = self.diff_df.filter(Predicates.present_in_both & Predicates.row_changed).drop(
                EXISTS_COL_NAME, IS_EQUAL_COL_NAME
            )
        return self._changed_df

    @property
    def changed_df_shards(self) -> List[DataFrame]:
        """List of shards of the DataFrame containing all rows that were found in both DataFrames but are not equal"""
        if self._changed_df_shards is None:
            self._changed_df_shards = [
                df.filter(Predicates.present_in_both & Predicates.row_changed).drop(EXISTS_COL_NAME, IS_EQUAL_COL_NAME)
                for df in self.diff_shards
            ]
        return self._changed_df_shards

    @staticmethod
    def _join_dataframes(*dfs: DataFrame, join_cols: List[str]) -> DataFrame:
        """Optimized method that joins multiple DataFrames in on single select statement.
        Ideally, the default :func:`DataFrame.join` should be optimized to do this directly.

        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())  # noqa: E501
        >>> df_a = bq.sql('SELECT 1 as id, STRUCT(1 as left_value, 1 as right_value, TRUE as is_equal) as a, True as __EXISTS__, TRUE as __IS_EQUAL__')
        >>> df_b = bq.sql('SELECT 1 as id, STRUCT(1 as left_value, 1 as right_value, TRUE as is_equal) as b, True as __EXISTS__, TRUE as __IS_EQUAL__')
        >>> df_c = bq.sql('SELECT 1 as id, STRUCT(1 as left_value, 1 as right_value, TRUE as is_equal) as c, True as __EXISTS__, TRUE as __IS_EQUAL__')
        >>> DiffResult._join_dataframes(df_a, df_b, df_c, join_cols=['id']).show()
        +----+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+------------+--------------+
        | id |                                                     a |                                                     b |                                                     c | __EXISTS__ | __IS_EQUAL__ |
        +----+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+------------+--------------+
        |  1 | {'left_value': 1, 'right_value': 1, 'is_equal': True} | {'left_value': 1, 'right_value': 1, 'is_equal': True} | {'left_value': 1, 'right_value': 1, 'is_equal': True} |       True |         True |
        +----+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+------------+--------------+

        """
        if len(dfs) == 1:
            return dfs[0]
        first_df = dfs[0]
        other_dfs = dfs[1:]
        excluded_common_cols = join_cols + [EXISTS_COL_NAME, IS_EQUAL_COL_NAME]
        is_equal = f.lit(True)
        for df in dfs:
            is_equal = is_equal & df[IS_EQUAL_COL_NAME]
        selected_columns = (
            str_to_col(join_cols)
            + [f.expr(f"{df._alias}.* EXCEPT ({cols_to_str(excluded_common_cols)})") for df in dfs]
            + [first_df[EXISTS_COL_NAME], is_equal.alias(IS_EQUAL_COL_NAME)]
        )
        on_str = ", ".join(join_cols)
        join_str = "\nJOIN ".join([f"{quote(df._alias)} USING ({on_str})" for df in other_dfs])

        query = strip_margin(
            f"""
            |SELECT
            |{cols_to_str(selected_columns, 2)}
            |FROM {quote(first_df._alias)}
            |JOIN {join_str}"""
        )
        return first_df._apply_query(query, deps=[first_df, *other_dfs])

    def _compute_diff_stats(self) -> DiffStats:
        """Given a diff_df and its list of join_cols, return stats about the number of differing or missing rows

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
        >>> schema_diff_result = SchemaDiffResult(same_schema=True, diff_str="", nb_cols=0)
        >>> DiffResult(schema_diff_result, diff_shards=[_diff_df], join_cols=['id'])._compute_diff_stats()
        DiffStats(total=4, no_change=1, changed=1, in_left=3, in_right=3, only_in_left=1, only_in_right=1)
        >>> _diff_df_2 = _diff_df.select('id', f.col('c1').alias('c3'), f.col('c1').alias('c4'), EXISTS_COL_NAME, IS_EQUAL_COL_NAME)
        >>> DiffResult(schema_diff_result, diff_shards=[_diff_df, _diff_df_2], join_cols=['id'])._compute_diff_stats()
        DiffStats(total=4, no_change=1, changed=1, in_left=3, in_right=3, only_in_left=1, only_in_right=1)

        :return:
        """
        res = self.diff_df.select(
            f.count(f.lit(1)).alias("total"),
            f.sum(f.when(Predicates.present_in_both & Predicates.row_is_equal, f.lit(1)).otherwise(f.lit(0))).alias(
                "no_change"
            ),
            f.sum(f.when(Predicates.present_in_both & Predicates.row_changed, f.lit(1)).otherwise(f.lit(0))).alias(
                "changed"
            ),
            f.sum(f.when(Predicates.in_left, f.lit(1)).otherwise(f.lit(0))).alias("in_left"),
            f.sum(f.when(Predicates.in_right, f.lit(1)).otherwise(f.lit(0))).alias("in_right"),
            f.sum(f.when(Predicates.only_in_left, f.lit(1)).otherwise(f.lit(0))).alias("only_in_left"),
            f.sum(f.when(Predicates.only_in_right, f.lit(1)).otherwise(f.lit(0))).alias("only_in_right"),
        ).collect()
        return DiffStats(**{k: (v if v is not None else 0) for k, v in res[0].items()})

    def __eq__(self, other):
        if isinstance(other, DiffResult):
            return self.same_schema == other.same_schema and self.diff_shards == other.diff_shards
        else:
            return NotImplemented

    def __repr__(self):
        return f"DiffResult(same_schema={self.same_schema}, diff_shards={self.diff_shards})"

    def display(self, show_examples: bool = False):
        from bigquery_frame.data_diff.diff_result_analyzer import DiffResultAnalyzer

        self.schema_diff_result.display()
        analyzer = DiffResultAnalyzer(self.diff_format_options)
        analyzer.display_diff_results(self, show_examples)
