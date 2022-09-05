from typing import List

from google.cloud.bigquery import SchemaField

from bigquery_frame import DataFrame
from bigquery_frame import functions as f
from bigquery_frame.auth import get_bq_client
from bigquery_frame.column import Column
from bigquery_frame.dataframe import BigQueryBuilder, cols_to_str, is_repeated
from bigquery_frame.utils import quote, quote_columns, str_to_col, strip_margin

MAGIC_HASH_COL_NAME = "__MAGIC_HASH__"
EXISTS_COL_NAME = "__EXISTS__"
IS_EQUAL_COL_NAME = "__IS_EQUAL__"
STRUCT_SEPARATOR_ALPHA = "__DOT__"


class Predicates:
    present_in_both = f.col(f"{EXISTS_COL_NAME}.left_value") & f.col(f"{EXISTS_COL_NAME}.right_value")
    in_left = f.col(f"{EXISTS_COL_NAME}.left_value")
    in_right = f.col(f"{EXISTS_COL_NAME}.right_value")
    only_in_left = f.col(f"{EXISTS_COL_NAME}.left_value") & (f.col(f"{EXISTS_COL_NAME}.right_value") == f.lit(False))
    only_in_right = (f.col(f"{EXISTS_COL_NAME}.left_value") == f.lit(False)) & f.col(f"{EXISTS_COL_NAME}.right_value")
    row_is_equal = f.col(IS_EQUAL_COL_NAME)
    row_changed = f.col(IS_EQUAL_COL_NAME) == f.lit(False)


def canonize_col(col: Column, schema_field: SchemaField) -> Column:
    """Applies a transformation on the specified field depending on its type.
    This ensures that the hash of the column will be constant despite it's ordering.

    :param col: the SchemaField object
    :param schema_field: the parent DataFrame
    :return: a Column
    """
    if is_repeated(schema_field):
        col = f.expr(f"IF({col} IS NULL, NULL, TO_JSON_STRING({col}))")
    return col


def join_dataframes(*dfs: DataFrame, join_cols: List[str]) -> DataFrame:
    """Optimized method that joins multiple DataFrames in on single select statement.
    Ideally, the default :func:`DataFrame.join` should be optimized to do this directly.
    """
    if len(dfs) == 1:
        return dfs[0]
    first_df = dfs[0]
    other_dfs = dfs[1:]
    excluded_common_cols = quote_columns(join_cols + [EXISTS_COL_NAME, IS_EQUAL_COL_NAME])
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


def _get_test_diff_df() -> DataFrame:
    bq = BigQueryBuilder(get_bq_client())
    diff_df = bq.sql(
        """
        SELECT * FROM UNNEST([
            STRUCT(
                1 as id,
                STRUCT("a" as left_value, "a" as right_value, True as is_equal) as c1,
                STRUCT(1 as left_value, 1 as right_value, True as is_equal) as c2,
                STRUCT(True as left_value, True as right_value) as __EXISTS__,
                TRUE as __IS_EQUAL__
            ),
            STRUCT(
                2 as id,
                STRUCT("b" as left_value, "b" as right_value, True as is_equal) as c1,
                STRUCT(2 as left_value, 4 as right_value, False as is_equal) as c2,
                STRUCT(True as left_value, True as right_value) as __EXISTS__,
                FALSE as __IS_EQUAL__
            ),
            STRUCT(
                3 as id,
                STRUCT("c" as left_value, NULL as right_value, False as is_equal) as c1,
                STRUCT(3 as left_value, NULL as right_value, False as is_equal) as c2,
                STRUCT(True as left_value, False as right_value) as __EXISTS__,
                FALSE as __IS_EQUAL__
            ),
            STRUCT(
                4 as id,
                STRUCT(NULL as left_value, "f" as right_value, False as is_equal) as c1,
                STRUCT(NULL as left_value, 3 as right_value, False as is_equal) as c2,
                STRUCT(False as left_value, True as right_value) as __EXISTS__,
                FALSE as __IS_EQUAL__
            )
        ])
    """
    )
    return diff_df


def _get_test_intersection_diff_df() -> DataFrame:
    bq = BigQueryBuilder(get_bq_client())
    diff_df = bq.sql(
        """
        SELECT * FROM UNNEST([
            STRUCT(
                1 as id,
                STRUCT("a" as left_value, "d" as right_value, False as is_equal) as c1,
                STRUCT(1 as left_value, 1 as right_value, True as is_equal) as c2
            ),
            STRUCT(
                2 as id,
                STRUCT("b" as left_value, "a" as right_value, False as is_equal) as c1,
                STRUCT(2 as left_value, 4 as right_value, False as is_equal) as c2
            )
        ])
    """
    )
    return diff_df
