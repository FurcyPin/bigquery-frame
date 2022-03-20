from bigquery_frame import BigQueryBuilder
from bigquery_frame.auth import get_bq_client
from bigquery_frame.column import Column
from bigquery_frame.dataframe import cols_to_str, DataFrame, quote
from typing import Union, List, Iterable

StringOrColumn = Union[str, Column]


def col(expr: str) -> Column:
    return Column(expr=quote(expr))


def __str_to_col(args: Union[Iterable[StringOrColumn], StringOrColumn]) -> Union[List[Column], Column]:
    """Converts string or Column arguments to Column types

    Examples:

    >>> __str_to_col("id")
    Column('`id`')
    >>> __str_to_col(["c1", "c2"])
    [Column('`c1`'), Column('`c2`')]
    >>> __str_to_col(expr("COUNT(1)"))
    Column('COUNT(1)')
    >>> __str_to_col("*")
    Column('*')

    """
    if isinstance(args, str):
        return col(args)
    elif isinstance(args, list):
        return [__str_to_col(arg) for arg in args]
    else:
        return args


def lit(val: object) -> Column:
    if val is None:
        return Column("NULL")
    if type(val) == str:
        return Column(f"'{val}'")
    if type(val) in [bool, int, float]:
        return Column(str(val))
    raise ValueError(f'lit({val}): The type {type(val)} is not supported yet.')


def struct(*cols: StringOrColumn) -> Column:
    return Column(f"STRUCT({cols_to_str(cols)})")


def isnull(col: StringOrColumn) -> Column:
    return Column(f"{col} IS NULL")


def count(col: StringOrColumn) -> Column:
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
    return Column(f"COUNT({col.expr})")


def count_distinct(col: StringOrColumn) -> Column:
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
    return Column(f"COUNT(DISTINCT {col.expr})")


def min(col: StringOrColumn) -> Column:
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
    return Column(f"MIN({col.expr})")


def max(col: StringOrColumn) -> Column:
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
    return Column(f"MAX({col.expr})")


def expr(expr: str) -> Column:
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
    return Column(expr)


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

