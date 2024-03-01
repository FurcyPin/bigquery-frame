from typing import TypeVar

from google.cloud.bigquery import SchemaField

from bigquery_frame import BigQueryBuilder, Column, DataFrame
from bigquery_frame import functions as f
from bigquery_frame.auth import get_bq_client
from bigquery_frame.dataframe import is_repeated

MAGIC_HASH_COL_NAME = "__MAGIC_HASH__"
EXISTS_COL_NAME = "__EXISTS__"
IS_EQUAL_COL_NAME = "__IS_EQUAL__"

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


PREDICATES = Predicates()


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


def stringify_col(col: Column, schema_field: SchemaField) -> Column:
    """Applies a transformation on the specified field depending on its type.
    This ensures that the hash of the column will be constant despite it's ordering.

    :param col: the SchemaField object
    :param schema_field: the parent DataFrame
    :return: a Column
    """
    col = canonize_col(col, schema_field)
    if schema_field.field_type == "FLOAT" and not is_repeated(schema_field):
        col = f.concat(f.cast(col, "STRING"), f.when(col == f.cast(col, "INT"), f.lit(".0")).otherwise(f.lit("")))
    else:
        return f.cast(col, "STRING")
    return col


def _get_test_diff_df() -> DataFrame:
    """
    >>> from bigquery_frame.data_diff.package import _get_test_diff_df
    >>> _get_test_diff_df().show()
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
    """  # noqa: E501
    bq = BigQueryBuilder(get_bq_client())
    diff_df = bq.sql(
        """
        SELECT * FROM UNNEST([
            STRUCT(
                STRUCT(1 as left_value, 1 as right_value, True as is_equal,
                       True as exists_left, True as exists_right
                ) as id,
                STRUCT("a" as left_value, "a" as right_value, True as is_equal,
                       True as exists_left, True as exists_right
                ) as c1,
                STRUCT(1 as left_value, 1 as right_value, True as is_equal,
                       True as exists_left, True as exists_right
                ) as c2,
                STRUCT(1 as left_value, NULL as right_value, False as is_equal,
                       True as exists_left, False as exists_right
                ) as c3,
                STRUCT(NULL as left_value, 1 as right_value, False as is_equal,
                       False as exists_left, True as exists_right
                ) as c4,
                STRUCT(True as left_value, True as right_value) as __EXISTS__,
                TRUE as __IS_EQUAL__
            ),
            STRUCT(
                STRUCT(2 as left_value, 2 as right_value, True as is_equal,
                       True as exists_left, True as exists_right
                ) as id,
                STRUCT("b" as left_value, "b" as right_value, True as is_equal,
                       True as exists_left, True as exists_right
                ) as c1,
                STRUCT(2 as left_value, 3 as right_value, False as is_equal,
                       True as exists_left, True as exists_right
                ) as c2,
                STRUCT(1 as left_value, NULL as right_value, False as is_equal,
                       True as exists_left, False as exists_right
                ) as c3,
                STRUCT(NULL as left_value, 1 as right_value, False as is_equal,
                       False as exists_left, True as exists_right
                ) as c4,
                STRUCT(True as left_value, True as right_value) as __EXISTS__,
                FALSE as __IS_EQUAL__
            ),
            STRUCT(
                STRUCT(3 as left_value, 3 as right_value, True as is_equal,
                       True as exists_left, True as exists_right
                ) as id,
                STRUCT("b" as left_value, "b" as right_value, True as is_equal,
                       True as exists_left, True as exists_right
                ) as c1,
                STRUCT(2 as left_value, 4 as right_value, False as is_equal,
                       True as exists_left, True as exists_right
                ) as c2,
                STRUCT(2 as left_value, NULL as right_value, False as is_equal,
                       True as exists_left, False as exists_right
                ) as c3,
                STRUCT(NULL as left_value, 2 as right_value, False as is_equal,
                       False as exists_left, True as exists_right
                ) as c4,
                STRUCT(True as left_value, True as right_value) as __EXISTS__,
                FALSE as __IS_EQUAL__
            ),
            STRUCT(
                STRUCT(4 as left_value, 4 as right_value, True as is_equal,
                       True as exists_left, True as exists_right
                ) as id,
                STRUCT("b" as left_value, "b" as right_value, True as is_equal,
                       True as exists_left, True as exists_right
                ) as c1,
                STRUCT(2 as left_value, 4 as right_value, False as is_equal,
                       True as exists_left, True as exists_right
                ) as c2,
                STRUCT(2 as left_value, NULL as right_value, False as is_equal,
                       True as exists_left, False as exists_right
                ) as c3,
                STRUCT(NULL as left_value, 2 as right_value, False as is_equal,
                       False as exists_left, True as exists_right
                ) as c4,
                STRUCT(True as left_value, True as right_value) as __EXISTS__,
                FALSE as __IS_EQUAL__
            ),
            STRUCT(
                STRUCT(5 as left_value, NULL as right_value, False as is_equal,
                       True as exists_left, False as exists_right
                ) as id,
                STRUCT("c" as left_value, NULL as right_value, False as is_equal,
                       True as exists_left, False as exists_right
                ) as c1,
                STRUCT(3 as left_value, NULL as right_value, False as is_equal,
                       True as exists_left, False as exists_right
                ) as c2,
                STRUCT(3 as left_value, NULL as right_value, False as is_equal,
                       True as exists_left, False as exists_right
                ) as c3,
                STRUCT(NULL as left_value, NULL as right_value, False as is_equal,
                       False as exists_left, False as exists_right
                ) as c4,
                STRUCT(True as left_value, False as right_value) as __EXISTS__,
                FALSE as __IS_EQUAL__
            ),
            STRUCT(
                STRUCT(NULL as left_value, 6 as right_value, False as is_equal,
                       False as exists_left, True as exists_right
                ) as id,
                STRUCT(NULL as left_value, "f" as right_value, False as is_equal,
                       False as exists_left, True as exists_right
                ) as c1,
                STRUCT(NULL as left_value, 3 as right_value, False as is_equal,
                       False as exists_left, True as exists_right
                ) as c2,
                STRUCT(NULL as left_value, NULL as right_value, False as is_equal,
                       False as exists_left, False as exists_right
                ) as c3,
                STRUCT(NULL as left_value, 3 as right_value, False as is_equal,
                       False as exists_left, True as exists_right
                ) as c4,
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
