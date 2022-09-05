from typing import List, Optional, Tuple

from bigquery_frame import DataFrame
from bigquery_frame import functions as f
from bigquery_frame.column import Column
from bigquery_frame.conf import ELEMENT_COL_NAME
from bigquery_frame.data_type_utils import get_common_columns
from bigquery_frame.nested import REPETITION_MARKER, STRUCT_SEPARATOR
from bigquery_frame.transformations_impl.flatten_schema import flatten_schema


def harmonize_dataframes(
    left_df: DataFrame, right_df: DataFrame, common_columns: Optional[List[Tuple[str, Optional[str]]]] = None
) -> Tuple[DataFrame, DataFrame]:
    """Given two DataFrames, returns two new corresponding DataFrames with the same schemas by applying the following
    changes:
    - Only common columns are kept
    - Columns are re-order to have the same ordering in both DataFrames
    - When matching columns have different types, their type is widened to their most narrow common type.
    This transformation is applied recursively on nested columns, including those inside
    repeated records (a.k.a. ARRAY<STRUCT<>>).

    >>> from bigquery_frame import BigQueryBuilder
    >>> from bigquery_frame.auth import get_bq_client
    >>> bq = BigQueryBuilder(get_bq_client())
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

    :return:
    """
    if common_columns is None:
        left_schema_flat = flatten_schema(left_df.schema, explode=True)
        right_schema_flat = flatten_schema(right_df.schema, explode=True)
        common_columns = get_common_columns(left_schema_flat, right_schema_flat)

    def build_col(col_name: str, col_type: Optional[str]) -> Column:
        if col_name[-1] == REPETITION_MARKER:
            col = f.col(ELEMENT_COL_NAME)
        else:
            col = f.col(col_name.split(REPETITION_MARKER + STRUCT_SEPARATOR)[-1])
        if col_type is not None:
            return col.cast(col_type)
        else:
            return col

    common_columns = {col_name: build_col(col_name, col_type) for (col_name, col_type) in common_columns}
    return left_df.select_nested_columns(common_columns), right_df.select_nested_columns(common_columns)
