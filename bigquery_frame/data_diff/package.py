from typing import TypeVar

from google.cloud.bigquery import SchemaField

from bigquery_frame import DataFrame
from bigquery_frame import functions as f
from bigquery_frame.auth import get_bq_client
from bigquery_frame.column import Column
from bigquery_frame.dataframe import BigQueryBuilder, is_repeated

MAGIC_HASH_COL_NAME = "__MAGIC_HASH__"
EXISTS_COL_NAME = "__EXISTS__"
IS_EQUAL_COL_NAME = "__IS_EQUAL__"
STRUCT_SEPARATOR_ALPHA = "__DOT__"

A = TypeVar("A")
B = TypeVar("B")


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
