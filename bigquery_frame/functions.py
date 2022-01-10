from bigquery_frame import BigQueryBuilder
from bigquery_frame.auth import get_bq_client
from bigquery_frame.column import Column as StrictColumn
from bigquery_frame.dataframe import cols_to_str, DataFrame
from typing import Union, List, Iterable

Column = Union[str, StrictColumn]


def __str_to_col(args: Union[Iterable[Column], Column]) -> Union[List[StrictColumn], StrictColumn]:
    """Converts string arguments to Column types"""
    if isinstance(args, str):
        return StrictColumn(expr=args)
    elif isinstance(args, list):
        return [__str_to_col(arg) for arg in args]
    else:
        return args


def col(expr: str) -> StrictColumn:
    return StrictColumn(expr=expr)


def lit(val: object) -> StrictColumn:
    if val is None:
        return StrictColumn("NULL")
    if type(val) == str:
        return StrictColumn(f"'{val}'")
    if type(val) in [bool, int, float]:
        return StrictColumn(str(val))
    raise ValueError(f'lit({val}): The type {type(val)} is not supported yet.')


def struct(*cols: Column) -> Column:
    return StrictColumn(f"STRUCT({cols_to_str(cols)})")


def isnull(col: Column) -> Column:
    return StrictColumn(f"{col} IS NULL")


def count(col: Union[Column, str]):
    """Aggregate function: returns the number of rows where the specified column is not null

    >>> df = __get_test_df_1()
    >>> df.show()
    +------+------+
    | col1 | col2 |
    +------+------+
    |    1 |    a |
    |    1 |    b |
    |    2 | null |
    +------+------+
    >>> from bigquery_frame import functions as f
    >>> df.select(
    ...   f.count(f.lit(1)).alias('count_1'),
    ...   f.count('col1').alias('count_col1'),
    ...   f.count('col2').alias('count_col2'),
    ...   f.count('*').alias('count_star')
    ... ).show()
    +---------+------------+------------+------------+
    | count_1 | count_col1 | count_col2 | count_star |
    +---------+------------+------------+------------+
    |       3 |          3 |          2 |          3 |
    +---------+------------+------------+------------+

    """
    col = __str_to_col(col)
    return StrictColumn(f"COUNT({col.expr})")


def count_distinct(col: Union[Column, str]):
    """Aggregate function: returns the number of distinct non-null values

    >>> df = __get_test_df_1()
    >>> df.show()
    +------+------+
    | col1 | col2 |
    +------+------+
    |    1 |    a |
    |    1 |    b |
    |    2 | null |
    +------+------+
    >>> from bigquery_frame import functions as f
    >>> df.select(
    ...   f.count_distinct('col1').alias('count_distinct_col1'),
    ...   f.count_distinct('col2').alias('count_distinct_col2'),
    ... ).show()
    +---------------------+---------------------+
    | count_distinct_col1 | count_distinct_col2 |
    +---------------------+---------------------+
    |                   2 |                   2 |
    +---------------------+---------------------+

    """
    col = __str_to_col(col)
    return StrictColumn(f"COUNT(DISTINCT {col.expr})")


def min(col: Union[Column, str]):
    """Aggregate function: returns the minimum value of the expression in a group.

    >>> df = __get_test_df_1()
    >>> df.show()
    +------+------+
    | col1 | col2 |
    +------+------+
    |    1 |    a |
    |    1 |    b |
    |    2 | null |
    +------+------+
    >>> from bigquery_frame import functions as f
    >>> df.select(
    ...   f.min('col1').alias('min_col1'),
    ...   f.min('col2').alias('min_col2'),
    ... ).show()
    +----------+----------+
    | min_col1 | min_col2 |
    +----------+----------+
    |        1 |        a |
    +----------+----------+

    """
    col = __str_to_col(col)
    return StrictColumn(f"MIN({col.expr})")


def max(col: Union[Column, str]):
    """Aggregate function: returns the maximum value of the expression in a group.

    >>> df = __get_test_df_1()
    >>> df.show()
    +------+------+
    | col1 | col2 |
    +------+------+
    |    1 |    a |
    |    1 |    b |
    |    2 | null |
    +------+------+
    >>> from bigquery_frame import functions as f
    >>> df.select(
    ...   f.max('col1').alias('max_col1'),
    ...   f.max('col2').alias('max_col2'),
    ... ).show()
    +----------+----------+
    | max_col1 | max_col2 |
    +----------+----------+
    |        2 |        b |
    +----------+----------+

    """
    col = __str_to_col(col)
    return StrictColumn(f"MAX({col.expr})")


def expr(expr: str):
    """Parses the expression string into the column that it represents.

    >>> df = __get_test_df_1()
    >>> df.show()
    +------+------+
    | col1 | col2 |
    +------+------+
    |    1 |    a |
    |    1 |    b |
    |    2 | null |
    +------+------+
    >>> from bigquery_frame import functions as f
    >>> df.select(
    ...   f.expr('COALESCE(col2, CAST(col1 as STRING)) as new_col')
    ... ).show()
    +---------+
    | new_col |
    +---------+
    |       a |
    |       b |
    |       2 |
    +---------+

    """
    return StrictColumn(expr)


def __get_test_df_1() -> DataFrame:
    bq = BigQueryBuilder(get_bq_client())
    query = """
        SELECT * FROM UNNEST ([
            STRUCT(1 as col1, "a" as col2),
            STRUCT(1 as col1, "b" as col2),
            STRUCT(2 as col1, NULL as col2)
       ])
    """
    return bq.sql(query)

