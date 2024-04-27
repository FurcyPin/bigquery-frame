from dataclasses import dataclass
from typing import List

from bigquery_frame import DataFrame
from bigquery_frame.data_diff.schema_diff import SchemaDiffResult


@dataclass
class DiffStatsForColumn:
    column_name: str
    """Name of the column"""
    total: int
    """Total number of rows after joining the two DataFrames"""
    no_change: int
    """Number of rows where this column is identical in both DataFrames"""
    changed: int
    """Number of rows that are present in both DataFrames but where this column has different values"""
    only_in_left: int
    """Number of rows that are only present in the left DataFrame"""
    only_in_right: int
    """Number of rows that are only present in the right DataFrame"""


@dataclass
class DiffResultSummary:
    left_df_alias: str
    right_df_alias: str
    diff_per_col_df: DataFrame

    schema_diff_result: SchemaDiffResult
    join_cols: List[str]
    same_schema: bool
    same_data: bool
    total_nb_rows: int
