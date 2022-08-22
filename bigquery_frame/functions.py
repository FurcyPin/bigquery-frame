from typing import Optional, Union

from bigquery_frame import BigQueryBuilder
from bigquery_frame.auth import get_bq_client
from bigquery_frame.column import (
    Column,
    LitOrColumn,
    StringOrColumn,
    cols_to_str,
    literal_col,
)
from bigquery_frame.dataframe import DataFrame
from bigquery_frame.utils import quote, str_to_col


def _invoke_function_over_column(function_name: str, col: StringOrColumn):
    """Invoke a SQL function with 1 argument"""
    col = str_to_col(col)
    return Column(f"{function_name}({col.expr})")


def approx_count_distinct(col: StringOrColumn) -> Column:
    """Aggregate function: returns a new :class:`bigquery_frame.column.Column` for approximate distinct count
    of column `col`.

    >>> df = _get_test_df_1()
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
    ...   f.approx_count_distinct('col1').alias('count_distinct_col1'),
    ...   f.approx_count_distinct('col2').alias('count_distinct_col2'),
    ... ).show()
    +---------------------+---------------------+
    | count_distinct_col1 | count_distinct_col2 |
    +---------------------+---------------------+
    |                   2 |                   2 |
    +---------------------+---------------------+

    """
    return _invoke_function_over_column("APPROX_COUNT_DISTINCT", col)


def cast(col: StringOrColumn, tpe: str) -> Column:
    """Converts a column to the specified type.

    Available types are listed here:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions

    >>> df = _get_test_df_1()
    >>> df.show()
    +------+------+
    | col1 | col2 |
    +------+------+
    |    1 |    a |
    |    1 |    b |
    |    2 | null |
    +------+------+
    >>> from bigquery_frame import functions as f
    >>> df.withColumn("col1",  f.cast("col1", "float64"), replace=True).show()
    +------+------+
    | col1 | col2 |
    +------+------+
    |  1.0 |    a |
    |  1.0 |    b |
    |  2.0 | null |
    +------+------+

    """
    column = str_to_col(col)
    return Column(f"CAST({column.expr} as {tpe.upper()})")


def coalesce(*cols: StringOrColumn) -> Column:
    """Returns the first column that is not null.

    Available types are listed here:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_functions

    >>> df = _get_test_df_2()
    >>> df.show()
    +------+------+
    |    a |    b |
    +------+------+
    | null | null |
    |    1 | null |
    | null |    2 |
    +------+------+
    >>> from bigquery_frame import functions as f
    >>> df.withColumn("coalesce", f.coalesce("a", "b")).show()
    +------+------+----------+
    |    a |    b | coalesce |
    +------+------+----------+
    | null | null |     null |
    |    1 | null |        1 |
    | null |    2 |        2 |
    +------+------+----------+

    """
    cols = [col.expr for col in str_to_col(cols)]
    return Column(f"COALESCE({cols_to_str(cols)})")


def col(expr: str) -> Column:
    return Column(expr=quote(expr))


def concat(*cols: StringOrColumn) -> Column:
    """Concatenates one or more values into a single result. All values must be BYTES or data types
    that can be cast to STRING. The function returns NULL if any input argument is NULL.

    Examples
    --------
    >>> bq = BigQueryBuilder(get_bq_client())
    >>> df = bq.sql("SELECT 'abcd' as s, '123' as d")
    >>> df.select(concat(df['s'], df['d']).alias('s')).show()
    +---------+
    |       s |
    +---------+
    | abcd123 |
    +---------+

    :param cols:
    :return:
    """
    cols = [col.expr for col in str_to_col(cols)]
    return Column(f"CONCAT({cols_to_str(cols)})")


def count(col: StringOrColumn) -> Column:
    """Aggregate function: returns the number of rows where the specified column is not null

    >>> df = _get_test_df_1()
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
    return _invoke_function_over_column("COUNT", col)


def count_distinct(col: StringOrColumn) -> Column:
    """Aggregate function: returns the number of distinct non-null values

    >>> df = _get_test_df_1()
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
    col = str_to_col(col)
    return Column(f"COUNT(DISTINCT {col.expr})")


def expr(expr: str) -> Column:
    """Parses the expression string into the column that it represents.

    >>> from bigquery_frame import functions as f
    >>> df = _get_test_df_1()
    >>> df.select("col1", "col2", f.expr('COALESCE(col2, CAST(col1 as STRING)) as new_col')).show()
    +------+------+---------+
    | col1 | col2 | new_col |
    +------+------+---------+
    |    1 |    a |       a |
    |    1 |    b |       b |
    |    2 | null |       2 |
    +------+------+---------+

    """
    return Column(expr)


def hash(*cols: Union[str, Column]) -> Column:
    """Calculates the hash code of given columns, and returns the result as an int column.

    Examples
    --------
    >>> from bigquery_frame import functions as f
    >>> df = _get_test_df_1().withColumn('hash_col', f.hash('col1', 'col2'))
    >>> df.show()
    +------+------+----------------------+
    | col1 | col2 |             hash_col |
    +------+------+----------------------+
    |    1 |    a |  6206812198800083495 |
    |    1 |    b | -6785414452297021595 |
    |    2 | null |  1951453458346972811 |
    +------+------+----------------------+
    """
    cols = str_to_col(cols)
    return expr(f"FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({cols_to_str(cols)})))")


def isnull(col: StringOrColumn) -> Column:
    return Column(f"{col.expr} IS NULL")


def length(col: StringOrColumn) -> Column:
    """Computes the character length of string data or number of bytes of binary data.
    The length of character data includes the trailing spaces. The length of binary data
    includes binary zeros.

    Examples
    --------
    >>> bq = BigQueryBuilder(get_bq_client())
    >>> bq.sql("SELECT 'ABC ' as a").select(length('a').alias('length')).show()
    +--------+
    | length |
    +--------+
    |      4 |
    +--------+
    """

    return _invoke_function_over_column("LENGTH", col)


lit = literal_col


def min(col: StringOrColumn) -> Column:
    """Aggregate function: returns the minimum value of the expression in a group.

    >>> df = _get_test_df_1()
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
    return _invoke_function_over_column("MIN", col)


def max(col: StringOrColumn) -> Column:
    """Aggregate function: returns the maximum value of the expression in a group.

    >>> df = _get_test_df_1()
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
    return _invoke_function_over_column("MAX", col)


def sort_array(col: StringOrColumn, asc: bool = True) -> Column:
    """Collection function: sorts the input array in ascending or descending order according to the natural ordering
    of the array elements. Unlike in Spark, arrays cannot contain NULL element when they are serialized.

    Examples
    --------
    >>> bq = BigQueryBuilder(get_bq_client())
    >>> df = bq.sql('''
    ...     SELECT data FROM UNNEST ([
    ...         STRUCT([2, 1, 3] as data),
    ...         STRUCT([1] as data),
    ...         STRUCT([] as data)])
    ... ''')
    >>> df.select(sort_array(df['data']).alias('r')).show()
    +-----------+
    |         r |
    +-----------+
    | [1, 2, 3] |
    |       [1] |
    |        [] |
    +-----------+
    >>> df.select(sort_array(df['data'], asc=False).alias('r')).show()
    +-----------+
    |         r |
    +-----------+
    | [3, 2, 1] |
    |       [1] |
    |        [] |
    +-----------+

    :param col: `Column` or str name of column
    :param asc: bool, optional
    :return:
    """
    col = str_to_col(col)
    desc_str = ""
    if not asc:
        desc_str = " DESC"
    return Column(f"ARRAY(SELECT elem FROM UNNEST({col.expr}) elem ORDER BY elem{desc_str})")


def substring(col: StringOrColumn, pos: LitOrColumn, len: Optional[LitOrColumn] = None) -> Column:
    """Return the substring that starts at `pos` and is of length `len`.
    If `len` is not specified, returns the substring that starts at `pos` until the end of the string

    Notes
    -----
    The position is not zero based, but 1 based index.

    Examples
    --------
    >>> bq = BigQueryBuilder(get_bq_client())
    >>> df = bq.sql("SELECT 'abcd' as s")
    >>> df.select(substring(df['s'], 1, 2).alias('s')).show()
    +----+
    |  s |
    +----+
    | ab |
    +----+
    >>> df.select(substring(df['s'], 3).alias('s')).show()
    +----+
    |  s |
    +----+
    | cd |
    +----+

    :param col: `Column` or str name of column of type STRING or BYTES
    :param pos: starting position of the substring (1-based index)
    :param len: optional, the length of the substring if specified.
        If not, the substring runs until the end of the input string.
    :return: a column of same type
    """
    col = str_to_col(col)
    if not isinstance(pos, Column):
        pos = lit(pos)
    if len is not None:
        if not isinstance(len, Column):
            len = lit(len)
        return Column(f"SUBSTRING({col.expr}, {pos.expr}, {len.expr})")
    else:
        return Column(f"SUBSTRING({col.expr}, {pos.expr})")


def sum(col: StringOrColumn) -> Column:
    """Aggregate function: returns the number of rows where the specified column is not null

    >>> df = _get_test_df_1()
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
    ...   f.sum('col1').alias('sum_col1'),
    ... ).show()
    +----------+
    | sum_col1 |
    +----------+
    |        4 |
    +----------+

    """
    return _invoke_function_over_column("SUM", col)


def struct(*cols: StringOrColumn) -> Column:
    # Unlike other functions (e.g. coalesce) we keep the column aliases here.
    return Column(f"STRUCT({cols_to_str(cols)})")


def when(condition: Column, value: Column) -> Column:
    """Evaluates a list of conditions and returns one of multiple possible result expressions.
    If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

    Examples
    --------
    >>> from bigquery_frame import functions as f
    >>> df = _get_test_df_1()
    >>> df.select("col1", f.when(f.col("col1") > f.lit(1), f.lit("yes")).otherwise(f.lit("no"))).show()
    +------+-----+
    | col1 | f0_ |
    +------+-----+
    |    1 |  no |
    |    1 |  no |
    |    2 | yes |
    +------+-----+

    :param condition: a boolean :class:`Column` expression.
    :param value: a :class:`Column` expression.
    :return:
    """
    c = Column("")
    c._when_condition = [(condition, value)]
    return c


def _get_test_df_1() -> DataFrame:
    bq = BigQueryBuilder(get_bq_client())
    query = """
        SELECT * FROM UNNEST ([
            STRUCT(1 as col1, "a" as col2),
            STRUCT(1 as col1, "b" as col2),
            STRUCT(2 as col1, NULL as col2)
       ])
    """
    return bq.sql(query)


def _get_test_df_2() -> DataFrame:
    bq = BigQueryBuilder(get_bq_client())
    query = """
        SELECT * FROM UNNEST ([
            STRUCT(NULL as a, NULL as b),
            STRUCT(1 as a, NULL as b),
            STRUCT(NULL as a, 2 as b)
       ])
    """
    return bq.sql(query)
