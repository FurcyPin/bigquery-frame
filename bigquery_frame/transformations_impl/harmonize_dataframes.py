from typing import Dict, Optional, Tuple

from bigquery_frame import Column, DataFrame
from bigquery_frame import functions as f
from bigquery_frame.conf import REPETITION_MARKER, STRUCT_SEPARATOR
from bigquery_frame.data_type_utils import flatten_schema, get_common_columns


def harmonize_dataframes(
    left_df: DataFrame,
    right_df: DataFrame,
    common_columns: Optional[Dict[str, Optional[str]]] = None,
    keep_missing_columns: bool = False,
) -> Tuple[DataFrame, DataFrame]:
    """Given two DataFrames, returns two new corresponding DataFrames with the same schemas by applying the following
    changes:

    - Only common columns are kept
    - Columns are re-ordered to have the same ordering in both DataFrames
    - When matching columns have different types, their type is widened to their most narrow common type.
    This transformation is applied recursively on nested columns, including those inside
    repeated records (a.k.a. ARRAY<STRUCT<>>).

    Args:
        left_df: A DataFrame
        right_df: A DataFrame
        common_columns: A dict of (column name, type).
            Column names must appear in both DataFrames, and each column will be cast into the corresponding type.
        keep_missing_columns: If set to true, the root columns of each DataFrames that do not exist in the other
            one are kept.

    Returns:
        Two new DataFrames with the same schema

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df1 = bq.sql('SELECT 1 as id, STRUCT(1 as a, [STRUCT(2 as c, 3 as d)] as b, [4, 5] as e) as s')
        >>> df2 = bq.sql('SELECT 1 as id, STRUCT(2 as a, [STRUCT(3.0 as c, "4" as d)] as b, [5.0, 6.0] as e) as s')
        >>> df1.union(df2).show() # doctest: +ELLIPSIS
        Traceback (most recent call last):
          ...
        google.api_core.exceptions.BadRequest: 400 Column 2 in UNION ALL has incompatible types: ...
        >>> df1, df2 = harmonize_dataframes(df1, df2)
        >>> df1.union(df2).show()
        +----+--------------------------------------------------------+
        | id |                                                      s |
        +----+--------------------------------------------------------+
        |  1 | {'a': 1, 'b': [{'c': 2.0, 'd': '3'}], 'e': [4.0, 5.0]} |
        |  1 | {'a': 2, 'b': [{'c': 3.0, 'd': '4'}], 'e': [5.0, 6.0]} |
        +----+--------------------------------------------------------+
    """
    left_schema_flat = flatten_schema(left_df.schema, explode=True)
    right_schema_flat = flatten_schema(right_df.schema, explode=True)
    if common_columns is None:
        common_columns = get_common_columns(left_schema_flat, right_schema_flat)

    left_only_columns = {}
    right_only_columns = {}
    if keep_missing_columns:
        left_cols = [field.name for field in left_schema_flat]
        right_cols = [field.name for field in right_schema_flat]
        left_cols_set = set(left_cols)
        right_cols_set = set(right_cols)
        left_only_columns = {col: None for col in left_cols if col not in right_cols_set}
        right_only_columns = {col: None for col in right_cols if col not in left_cols_set}

    def build_col(col_name: str, col_type: Optional[str]) -> Column:
        if col_name[-1] == REPETITION_MARKER:
            if col_type is not None:
                return lambda col: col.cast(col_type)
            else:
                return lambda col: col
        else:
            col = f.col(col_name.split(REPETITION_MARKER + STRUCT_SEPARATOR)[-1])
            if col_type is not None:
                return col.cast(col_type)
            else:
                return col

    left_columns = {**common_columns, **left_only_columns}
    right_columns = {**common_columns, **right_only_columns}
    left_columns_dict = {col_name: build_col(col_name, col_type) for (col_name, col_type) in left_columns.items()}
    right_columns_dict = {col_name: build_col(col_name, col_type) for (col_name, col_type) in right_columns.items()}

    return left_df.select_nested_columns(left_columns_dict), right_df.select_nested_columns(right_columns_dict)
