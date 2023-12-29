import datetime
import decimal
import typing
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from bigquery_frame import BigQueryBuilder
from bigquery_frame.column import (
    Column,
    LitOrColumn,
    SortedArrayColumn,
    StringOrColumn,
    TransformedArrayColumn,
    WhenColumn,
    cols_to_str,
)
from bigquery_frame.dataframe import DataFrame
from bigquery_frame.exceptions import IllegalArgumentException
from bigquery_frame.utils import quote, str_to_col, str_to_cols

RawType = Union[str, bool, int, float, bytes, decimal.Decimal, datetime.date, datetime.time, datetime.datetime]
ComplexType = Union[RawType, List["ComplexType"], Dict[str, "ComplexType"]]


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


def array(*cols: Union[StringOrColumn, Sequence[StringOrColumn]]) -> Column:
    """Creates a new array column.

    Limitations
    -----------
    In BigQuery, arrays may not contain NULL values **when they are serialized**.
    This means that arrays may contain NULL values during the query computation for intermediary results, but
    when the query result is returned or written, an exception will occur if an array contains a NULL value.

    Examples
    --------
    >>> df = _get_test_df_1()
    >>> from bigquery_frame import functions as f
    >>> df.select(f.array(lit(0), 'col1').alias("struct")).show()
    +--------+
    | struct |
    +--------+
    | [0, 1] |
    | [0, 1] |
    | [0, 2] |
    +--------+
    >>> df.select(f.array([df['col1'], df['col1']]).alias("struct")).show()
    +--------+
    | struct |
    +--------+
    | [1, 1] |
    | [1, 1] |
    | [2, 2] |
    +--------+

    :param cols: a list or set of str (column names) or :class:`Column` that have the same data type.
    :return:
    """
    columns: Iterable[StringOrColumn]
    if len(cols) == 1 and isinstance(cols[0], (List, Set)):
        columns = cols[0]
    else:
        columns = typing.cast(Tuple[StringOrColumn], cols)
    str_cols = [col.expr for col in str_to_cols(columns)]
    return Column(f"[{cols_to_str(str_cols)}]")


def array_agg(
    col: StringOrColumn,
    order_by: Optional[Union[StringOrColumn, List[StringOrColumn]]] = None,
    distinct: bool = False,
    ignore_nulls: bool = False,
    limit: Optional[int] = None,
) -> Column:
    """
    Aggregates this column into an array of values.


    Examples
    --------
    >>> bq = BigQueryBuilder()
    >>> from bigquery_frame import functions as f
    >>> df = bq.sql("SELECT * FROM UNNEST([1, 2, 3, 2, 3, 3]) as c")
    >>> df.select(f.array_agg("c").alias("a")).show()
    +--------------------+
    |                  a |
    +--------------------+
    | [1, 2, 3, 2, 3, 3] |
    +--------------------+
    >>> df.select(f.array_agg("c", order_by=f.desc("c")).alias("a")).show()
    +--------------------+
    |                  a |
    +--------------------+
    | [3, 3, 3, 2, 2, 1] |
    +--------------------+
    >>> df.select(f.array_agg("c", order_by=f.desc("c"), limit=3).alias("a")).show()
    +-----------+
    |         a |
    +-----------+
    | [3, 3, 3] |
    +-----------+
    >>> df.select(f.array_agg("c", order_by="c", distinct=True).alias("a")).show()
    +-----------+
    |         a |
    +-----------+
    | [1, 2, 3] |
    +-----------+
    >>> df2 = df.withColumn("is_even", f.col("c") % 2 == 0)
    >>> null_if_even = f.when(f.col("is_even"), f.lit(None)).otherwise(f.col("c"))
    >>> df2.select(f.array_agg(null_if_even, order_by="c", ignore_nulls=True).alias("a")).show()
    +--------------+
    |            a |
    +--------------+
    | [1, 3, 3, 3] |
    +--------------+
    >>> df2.select(f.array_agg(f.col("c"), order_by=[f.col("is_even"), "c"]).alias("a")).show()
    +--------------------+
    |                  a |
    +--------------------+
    | [1, 3, 3, 3, 2, 2] |
    +--------------------+

    :param col: a str (column name) or :class:`Column`
    :param order_by: (optional) sort the resulting array according to this column
    :param distinct: (optional) only keep distinct values in the resulting array
    :param ignore_nulls: (optional) remove null values from the resulting array
    :param limit: (optional) only keep this number of values in the resulting array
    :return:
    """
    if order_by is None:
        order_by = []
    elif not isinstance(order_by, list):
        order_by = [order_by]
    distinct_str = ""
    ignore_nulls_str = ""
    limit_str = ""
    order_by_str = ""
    if distinct:
        distinct_str = "DISTINCT "
    if ignore_nulls:
        ignore_nulls_str = " IGNORE NULLS "
    if limit:
        limit_str = f" LIMIT {limit}"
    if len(order_by) != 0:
        order_by_str = f" ORDER BY {cols_to_str(str_to_cols(order_by))}"
    col_str = str_to_col(col).expr
    return Column(f"ARRAY_AGG({distinct_str}{col_str}{ignore_nulls_str}{order_by_str}{limit_str})")


def asc(col: StringOrColumn) -> Column:
    """Returns a sort expression based on the ascending order of the given column.

    >>> df = _get_test_df_3()
    >>> df.show()
    +------+
    | col1 |
    +------+
    |    2 |
    |    1 |
    | null |
    |    3 |
    +------+
    >>> df.sort(asc("col1")).show()
    +------+
    | col1 |
    +------+
    | null |
    |    1 |
    |    2 |
    |    3 |
    +------+
    """
    return Column(f"{str_to_col(col).expr} ASC")


def avg(col: StringOrColumn) -> Column:
    """Aggregate function: returns the average of the values in a group.

    >>> df = _get_test_df_3()
    >>> df.show()
    +------+
    | col1 |
    +------+
    |    2 |
    |    1 |
    | null |
    |    3 |
    +------+
    >>> from bigquery_frame import functions as f
    >>> df.select(f.avg("col1").alias("avg_col1")).show()
    +----------+
    | avg_col1 |
    +----------+
    |      2.0 |
    +----------+
    """
    return _invoke_function_over_column("AVG", col)


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
    str_col = str_to_col(col)
    return Column(f"CAST({str_col.expr} as {tpe.upper()})")


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
    >>> df.withColumn("coalesce", f.coalesce("a", f.col("b"))).show()
    +------+------+----------+
    |    a |    b | coalesce |
    +------+------+----------+
    | null | null |     null |
    |    1 | null |        1 |
    | null |    2 |        2 |
    +------+------+----------+

    """
    str_cols = [col.expr for col in str_to_cols(cols)]
    return Column(f"COALESCE({cols_to_str(str_cols)})")


def col(expr: str) -> Column:
    return Column(expr=quote(expr))


def concat(*cols: StringOrColumn) -> Column:
    """Concatenates one or more values into a single result. All values must be BYTES or data types
    that can be cast to STRING. The function returns NULL if any input argument is NULL.

    Examples
    --------
    >>> bq = BigQueryBuilder()
    >>> df = bq.sql("SELECT 'abcd' as s, '123' as d")
    >>> from bigquery_frame import functions as f
    >>> df.select(f.concat(df['s'], df['d']).alias('s')).show()
    +---------+
    |       s |
    +---------+
    | abcd123 |
    +---------+

    :param cols:
    :return:
    """
    str_cols = [col.expr for col in str_to_cols(cols)]
    return Column(f"CONCAT({cols_to_str(str_cols)})")


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
    str_col = str_to_col(col)
    return Column(f"COUNT(DISTINCT {str_col.expr})")


def desc(col: StringOrColumn) -> Column:
    """Returns a sort expression based on the descending order of the given column.

    >>> df = _get_test_df_3()
    >>> df.show()
    +------+
    | col1 |
    +------+
    |    2 |
    |    1 |
    | null |
    |    3 |
    +------+
    >>> from bigquery_frame import functions as f
    >>> df.sort(f.desc("col1")).show()
    +------+
    | col1 |
    +------+
    |    3 |
    |    2 |
    |    1 |
    | null |
    +------+
    """
    return Column(f"{str_to_col(col).expr} DESC")


def expr(expr: str) -> Column:
    """Parses the expression string into the column that it represents.

    >>> df = _get_test_df_1()
    >>> from bigquery_frame import functions as f
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


def from_base32(col: StringOrColumn) -> Column:
    """Converts the base32-encoded input string_expr into BYTES format.
    To convert BYTES to a base32-encoded STRING, use :func:`to_base32`.

    Examples
    --------
    >>> bq = BigQueryBuilder()
    >>> from bigquery_frame import functions as f
    >>> df = bq.sql(r"SELECT 'MFRGGZDF74======' as s")
    >>> df.select(f.from_base32('s').alias('byte_data')).show()
    +--------------+
    |    byte_data |
    +--------------+
    | b'abcde\xff' |
    +--------------+

    """
    return _invoke_function_over_column("FROM_BASE32", col)


def from_base64(col: StringOrColumn) -> Column:
    """Converts the base64-encoded input string_expr into BYTES format.
    To convert BYTES to a base64-encoded STRING, use :func:`to_base64`.

    There are several base64 encodings in common use that vary in exactly which alphabet of 65 ASCII characters
    are used to encode the 64 digits and padding. See RFC 4648 for details.
    This function expects the alphabet [A-Za-z0-9+/=].

    Examples
    --------
    >>> bq = BigQueryBuilder()
    >>> from bigquery_frame import functions as f
    >>> df = bq.sql(r"SELECT '/+A=' as s")
    >>> df.select(f.from_base64('s').alias('byte_data')).show()
    +-------------+
    |   byte_data |
    +-------------+
    | b'\xff\xe0' |
    +-------------+

    To work with an encoding using a different base64 alphabet, you might need to compose :func:`from_base64` with
    the :func:`replace` function. For instance, the base64url url-safe and filename-safe encoding commonly used
    in web programming uses `-_=` as the last characters rather than `+/=`.
    To decode a base64url-encoded string, replace `-` and `_` with `+` and `/` respectively.

    >>> df.select(f.from_base64(f.replace(f.replace(f.lit('_-A='), '-', '+'),  '_', '/')).alias('binary')).show()
    +-------------+
    |      binary |
    +-------------+
    | b'\xff\xe0' |
    +-------------+

    """
    return _invoke_function_over_column("FROM_BASE64", col)


def hash(*cols: Union[str, Column]) -> Column:
    """Calculates the hash code of given columns, and returns the result as an int column.

    Examples
    --------
    >>> df = _get_test_df_1()
    >>> from bigquery_frame import functions as f
    >>> df.withColumn('hash_col', f.hash('col1', 'col2')).show()
    +------+------+----------------------+
    | col1 | col2 |             hash_col |
    +------+------+----------------------+
    |    1 |    a |  6206812198800083495 |
    |    1 |    b | -6785414452297021595 |
    |    2 | null |  1951453458346972811 |
    +------+------+----------------------+
    """
    str_cols = str_to_cols(cols)
    return expr(f"FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({cols_to_str(str_cols)})))")


def isnull(col: StringOrColumn) -> Column:
    """An expression that returns true iff the column is null.

    >>> df = _get_test_df_1()
    >>> from bigquery_frame import functions as f
    >>> df.select('col2', f.isnull('col2').alias('is_null')).show()
    +------+---------+
    | col2 | is_null |
    +------+---------+
    |    a |   False |
    |    b |   False |
    | null |    True |
    +------+---------+
    """
    return Column(f"{str_to_col(col).expr} IS NULL")


def length(col: StringOrColumn) -> Column:
    """Computes the character length of string data or number of bytes of binary data.
    The length of character data includes the trailing spaces. The length of binary data
    includes binary zeros.

    Examples
    --------
    >>> bq = BigQueryBuilder()
    >>> from bigquery_frame import functions as f
    >>> bq.sql("SELECT 'ABC ' as a").select(f.length('a').alias('length')).show()
    +--------+
    | length |
    +--------+
    |      4 |
    +--------+
    """

    return _invoke_function_over_column("LENGTH", col)


def lit(col: Union[None, Column, RawType, ComplexType]) -> Column:
    """Creates a :class:`Column` of literal value.

    The data type mapping between Python types and BigQuery types is as follows:

    - `None` : `NULL`
    - `str` : `STRING`
    - `bool` : `BOOL`
    - `int` : `INTEGER`
    - `float` : `FLOAT64`
    - `bytes` : `BYTES`
    - `decimal`.Decimal : `BIGNUMERIC`
    - `datetime.date` : `DATE`
    - `datetime.time` : `TIME`
    - `datetime.timestamp` : `DATETIME`, or `TIMESTAMP` if a timezone is defined
    - `list[T]` : `ARRAY<T>`
    - `dict[K, V]` : `STRUCT<k1: V1, k2: V2, ...>`

    !!! note
        Type `NUMERIC` can not be generated with this method, as the corresponding Python type is `decimal.Decimal`
        which is converted into `BIGNUMERIC`.
        If you need NUMERIC type specifically, you can call `.cast("NUMERIC")` from the result.

    Args:
        col : A basic Python type to convert into BigQuery literal.
        If a column is passed, it returns the column as is.

    Returns:
        The literal instance.

    Raises:
        bigquery_frame.exceptions.IllegalArgumentException: if the argument's type is not supported

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame import functions as f
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql("SELECT 1 as id")
        >>> df.select(
        ...     f.lit(f.col("id")),
        ...     f.lit(None).alias("null_col"),
        ...     f.lit("a string").alias("string_col"),
        ...     f.lit(True).alias("bool_col"),
        ...     f.lit(1).alias("int_col"),
        ...     f.lit(1.0).alias("float_col"),
        ...     f.lit(b"\x01").alias("bytes_col"),
        ...     f.lit(decimal.Decimal("123456789.123456789e3")).alias("bignumeric_col"),
        ...     f.lit(datetime.date.fromisoformat("2024-01-01")).alias("date_col"),
        ...     f.lit(datetime.time.fromisoformat("13:37:00")).alias("time_col"),
        ...     f.lit(datetime.datetime.fromisoformat("2024-01-01T02:00:00")).alias("datetime_col"),
        ...     f.lit(datetime.datetime.fromisoformat("2024-01-01T12:34:56.789012+02:00")).alias("timestamp_col"),
        ... ).printSchema()
        root
         |-- id: INTEGER (NULLABLE)
         |-- null_col: INTEGER (NULLABLE)
         |-- string_col: STRING (NULLABLE)
         |-- bool_col: BOOLEAN (NULLABLE)
         |-- int_col: INTEGER (NULLABLE)
         |-- float_col: FLOAT (NULLABLE)
         |-- bytes_col: BYTES (NULLABLE)
         |-- bignumeric_col: BIGNUMERIC (NULLABLE)
         |-- date_col: DATE (NULLABLE)
         |-- time_col: TIME (NULLABLE)
         |-- datetime_col: DATETIME (NULLABLE)
         |-- timestamp_col: TIMESTAMP (NULLABLE)
        <BLANKLINE>

        Create a literal from a list.

        >>> df.select(f.lit([1, 2, 3]).alias("int_array_col")).show()
        +---------------+
        | int_array_col |
        +---------------+
        |     [1, 2, 3] |
        +---------------+

        Create a literal from a dict.

        >>> df.select(f.lit({"age": 2, "name": "Alice", "friends": ["Bob", "Joe"]}).alias("struct_col")).show()
        +--------------------------------------------------------+
        |                                             struct_col |
        +--------------------------------------------------------+
        | {'age': 2, 'name': 'Alice', 'friends': ['Bob', 'Joe']} |
        +--------------------------------------------------------+


        >>> df.select(f.lit({"age": 2, "name": "Alice", "friends": ["Bob", "Joe"]}).alias("struct_col")).show()
        +--------------------------------------------------------+
        |                                             struct_col |
        +--------------------------------------------------------+
        | {'age': 2, 'name': 'Alice', 'friends': ['Bob', 'Joe']} |
        +--------------------------------------------------------+

        >>> df.select(f.lit(range(0, 1)))
        Traceback (most recent call last):
            ...
        bigquery_frame.exceptions.IllegalArgumentException: lit(range(0, 1)): The type <class 'range'> is not supported.
    """
    if col is None:
        return Column("NULL")
    elif isinstance(col, list):
        return array(*[lit(c) for c in col])
    elif isinstance(col, dict):
        return struct(*[lit(c).alias(alias) for alias, c in col.items()])
    elif isinstance(col, Column):
        return col
    elif isinstance(col, str):
        safe_col = col.replace('"', r"\"")
        return Column(f'''r"""{safe_col}"""''')
    elif isinstance(col, (bool, int, float, bytes)):
        return Column(str(col))
    elif isinstance(col, decimal.Decimal):
        return Column(f"BIGNUMERIC '{str(col)}'")
    elif isinstance(col, datetime.datetime):
        if col.tzinfo is None:
            return Column(f"DATETIME '{col.isoformat()}'")
        else:
            return Column(f"TIMESTAMP '{col.isoformat()}'")
    elif isinstance(col, datetime.date):
        return Column(f"DATE '{col.isoformat()}'")
    elif isinstance(col, datetime.time):
        return Column(f"TIME '{col.isoformat()}'")
    raise IllegalArgumentException(f"lit({col}): The type {type(col)} is not supported.")


def lower(col: StringOrColumn) -> Column:
    """Converts a BYTES or STRING expression to lower case.

    For STRING arguments, returns the original string with all alphabetic characters in lowercase.
    Mapping between lowercase and uppercase is done according to the
    [Unicode Character Database](https://unicode.org/ucd/) without taking into account language-specific mappings.

    For BYTES arguments, the argument is treated as ASCII text, with all bytes greater than 127 left intact.

    Args:
        col: Target column to work on.

    Examples:
        >>> bq = BigQueryBuilder()
        >>> from bigquery_frame import functions as f
        >>> df = bq.sql('''SELECT item FROM UNNEST(['FOO', 'BAR', 'BAZ']) as item''')
        >>> df.show()
        +------+
        | item |
        +------+
        |  FOO |
        |  BAR |
        |  BAZ |
        +------+

        >>> df.select(f.lower("item").alias("item")).show()
        +------+
        | item |
        +------+
        |  foo |
        |  bar |
        |  baz |
        +------+
    """
    return _invoke_function_over_column("LOWER", col)


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


def mean(col: StringOrColumn) -> Column:
    """Aggregate function: returns the average of the values in a group.

    >>> df = _get_test_df_3()
    >>> df.show()
    +------+
    | col1 |
    +------+
    |    2 |
    |    1 |
    | null |
    |    3 |
    +------+
    >>> from bigquery_frame import functions as f
    >>> df.select(f.mean("col1").alias("mean_col1")).show()
    +-----------+
    | mean_col1 |
    +-----------+
    |       2.0 |
    +-----------+
    """
    return _invoke_function_over_column("AVG", col)


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


def regexp_replace(value: StringOrColumn, regexp: LitOrColumn, replacement: LitOrColumn) -> Column:
    r"""Returns a STRING where all substrings of `value` that match regular expression `regexp`
    are replaced with `replacement`.

    You can use backslashed-escaped digits (\1 to \9) within the `replacement` argument to insert text
    matching the corresponding parenthesized group in the `regexp` pattern. Use \0 to refer to the entire matching text.

    To add a backslash in your regular expression, you must first escape it.
    For example, `SELECT REGEXP_REPLACE('abc', 'b(.)', 'X\\1');` returns aXc.
    You can also use raw strings to remove one layer of escaping, for example
    `SELECT REGEXP_REPLACE('abc', 'b(.)', r'X\1');`.

    The REGEXP_REPLACE function only replaces non-overlapping matches.
    For example, replacing `ana` within `banana` results in only one replacement, not two.

    If the regexp argument is not a valid regular expression, this function returns an error.

    !!! note
        GoogleSQL provides regular expression support using the [re2](https://github.com/google/re2/wiki/Syntax)
        library; see that documentation for its regular expression syntax.


    >>> bq = BigQueryBuilder()
    >>> df = bq.sql('''
    ... SELECT '# Heading' as heading
    ... UNION ALL
    ... SELECT '# Another heading' as heading
    ... ''')
    >>> from bigquery_frame import functions as f
    >>> df.select(f.regexp_replace("heading", r'^# ([a-zA-Z0-9\s]+$)', r'<h1>\1</h1>').alias('s')).show()
    +--------------------------+
    |                        s |
    +--------------------------+
    |         <h1>Heading</h1> |
    | <h1>Another heading</h1> |
    +--------------------------+

    :param value: a :class:`Column` expression or a string column name
    :param regexp: a :class:`Column` expression or a string literal
    :param replacement: a :class:`Column` expression or a string literal
    :return: a :class:`Column` expression
    """
    value = str_to_col(value)
    if not isinstance(regexp, Column):
        regexp = lit(regexp)
    if not isinstance(replacement, Column):
        replacement = lit(replacement)
    return Column(f"REGEXP_REPLACE({value.expr}, {regexp.expr}, {replacement.expr})")


def replace(original_value: StringOrColumn, from_value: LitOrColumn, replace_value: LitOrColumn) -> Column:
    """Replaces all occurrences of `from_value` with `to_value` in `original_value`.
    If `from_value` is empty, no replacement is made.

    >>> bq = BigQueryBuilder()
    >>> df = bq.sql("SELECT 'a.b.c.d' as s, '.' as dot, '/' as slash")
    >>> from bigquery_frame import functions as f
    >>> df.select(f.replace('s', ".", "/").alias('s')).show()
    +---------+
    |       s |
    +---------+
    | a/b/c/d |
    +---------+
    >>> df.select(f.replace(df['s'], f.lit("."), f.lit("/")).alias('s')).show()
    +---------+
    |       s |
    +---------+
    | a/b/c/d |
    +---------+
    >>> df.select(f.replace(col("s"), f.col("dot"), f.col("slash")).alias('s')).show()
    +---------+
    |       s |
    +---------+
    | a/b/c/d |
    +---------+
    >>> df.select(f.replace("s", "dot", "slash").alias('s')).show()
    +---------+
    |       s |
    +---------+
    | a.b.c.d |
    +---------+

    :param original_value: a :class:`Column` expression or a string column name
    :param from_value: a :class:`Column` expression or a string literal
    :param replace_value: a :class:`Column` expression or a string literal
    :return: a :class:`Column` expression
    """
    original_value = str_to_col(original_value)
    if not isinstance(from_value, Column):
        from_value = lit(from_value)
    if not isinstance(replace_value, Column):
        replace_value = lit(replace_value)
    return Column(f"REPLACE({original_value.expr}, {from_value.expr}, {replace_value.expr})")


def sort_array(
    array: StringOrColumn,
    sort_keys: Optional[Callable[[Column], Union[Column, List[Column]]]] = None,
) -> Column:
    """Collection function: sorts the input array according to the natural ordering of the array elements,
    or, if `sort_keys` specified, according to the `sort_keys`.
    `sort_keys` is a function that takes as argument a Column representing the array's elements and returns
    the Column or the list of Columns used for sorting (`asc` and `desc` can be used here).

    Examples
    --------
    >>> bq = BigQueryBuilder()
    >>> df = bq.sql('''
    ...     SELECT data FROM UNNEST ([
    ...         STRUCT([2, 1, 3] as data),
    ...         STRUCT([1] as data),
    ...         STRUCT([] as data)])
    ... ''')
    >>> from bigquery_frame import functions as f
    >>> df.select(f.sort_array('data').alias('r')).show()
    +-----------+
    |         r |
    +-----------+
    | [1, 2, 3] |
    |       [1] |
    |        [] |
    +-----------+
    >>> df.select(f.sort_array(df['data'], lambda c: c.desc()).alias('r')).show()
    +-----------+
    |         r |
    +-----------+
    | [3, 2, 1] |
    |       [1] |
    |        [] |
    +-----------+

    >>> df = bq.sql('''SELECT [STRUCT(2 as a, "x" as b), STRUCT(1 as a, "z" as b), STRUCT(1 as a, "y" as b)] as s''')
    >>> df.show(simplify_structs=True)
    +--------------------------+
    |                        s |
    +--------------------------+
    | [{2, x}, {1, z}, {1, y}] |
    +--------------------------+
    >>> df.select(f.sort_array("s", lambda s: [s["a"], s["b"]]).alias("s")).show(simplify_structs=True)
    +--------------------------+
    |                        s |
    +--------------------------+
    | [{1, y}, {1, z}, {2, x}] |
    +--------------------------+

    :param array: `Column` or str name of column of type ARRAY
    :param sort_keys: zero or more functions that take a column corresponding to the elements of the array
        and return column expression used for ordering.
    :return: a `Column` expression
    """
    str_array = str_to_col(array)
    return SortedArrayColumn(str_array, sort_keys=sort_keys)


def substring(col: StringOrColumn, pos: LitOrColumn, len: Optional[LitOrColumn] = None) -> Column:
    """Return the substring that starts at `pos` and is of length `len`.
    If `len` is not specified, returns the substring that starts at `pos` until the end of the string

    Notes
    -----
    The position is not zero based, but 1 based index.

    Examples
    --------
    >>> bq = BigQueryBuilder()
    >>> df = bq.sql("SELECT 'abcd' as s")
    >>> from bigquery_frame import functions as f
    >>> df.select(f.substring(df['s'], 1, 2).alias('s')).show()
    +----+
    |  s |
    +----+
    | ab |
    +----+
    >>> df.select(f.substring(df['s'], 3).alias('s')).show()
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
    str_col = str_to_col(col)
    if not isinstance(pos, Column):
        pos = lit(pos)
    if len is not None:
        if not isinstance(len, Column):
            len = lit(len)
        return Column(f"SUBSTRING({str_col.expr}, {pos.expr}, {len.expr})")
    else:
        return Column(f"SUBSTRING({str_col.expr}, {pos.expr})")


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


def struct(*cols: Union[StringOrColumn, List[StringOrColumn], Set[StringOrColumn]]) -> Column:
    """Creates a new struct column.

    Examples
    --------
    >>> df = _get_test_df_1()
    >>> from bigquery_frame import functions as f
    >>> df.select(f.struct('col1', 'col2').alias("struct")).show()
    +---------------------------+
    |                    struct |
    +---------------------------+
    |  {'col1': 1, 'col2': 'a'} |
    |  {'col1': 1, 'col2': 'b'} |
    | {'col1': 2, 'col2': None} |
    +---------------------------+
    >>> df.select(f.struct([df['col1'], df['col2']]).alias("struct")).show()
    +---------------------------+
    |                    struct |
    +---------------------------+
    |  {'col1': 1, 'col2': 'a'} |
    |  {'col1': 1, 'col2': 'b'} |
    | {'col1': 2, 'col2': None} |
    +---------------------------+

    :param cols: a list or set of str (column names) or :class:`Column` to be added to the output struct.
    :return:
    """
    columns: Iterable[StringOrColumn]
    if len(cols) == 1 and isinstance(cols[0], (List, Set)):
        columns = cols[0]
    else:
        columns = typing.cast(Tuple[StringOrColumn], cols)
    # Unlike other functions (e.g. coalesce) we keep the column aliases here.
    return Column(f"STRUCT({cols_to_str(columns)})")


def to_base32(col: StringOrColumn) -> Column:
    """Converts a sequence of BYTES into a base32-encoded STRING.
    To convert a base32-encoded STRING into BYTES, use :func:`from_base32`.

    Examples
    --------
    >>> bq = BigQueryBuilder()
    >>> from bigquery_frame import functions as f
    >>> df = bq.sql(r"SELECT b'abcde\\xFF' as b")
    >>> df.select(f.to_base32('b').alias('base32_string')).show()
    +------------------+
    |    base32_string |
    +------------------+
    | MFRGGZDF74====== |
    +------------------+

    """
    return _invoke_function_over_column("TO_BASE32", col)


def to_base64(col: StringOrColumn) -> Column:
    """Converts a sequence of BYTES into a base64-encoded STRING.
    To convert a base64-encoded STRING into BYTES, use :func:`from_base64`.

    There are several base64 encodings in common use that vary in exactly which alphabet of 65 ASCII characters
    are used to encode the 64 digits and padding.
    See `RFC 4648 <https://tools.ietf.org/html/rfc4648#section-4>`_ for details.
    This function adds padding and uses the alphabet [A-Za-z0-9+/=].

    Examples
    --------
    >>> bq = BigQueryBuilder()
    >>> from bigquery_frame import functions as f
    >>> df = bq.sql(r"SELECT b'\\377\\340' as b")
    >>> df.select(f.to_base64('b').alias('base64_string')).show()
    +---------------+
    | base64_string |
    +---------------+
    |          /+A= |
    +---------------+

    To work with an encoding using a different base64 alphabet, you might need to compose :func:`to_base64` with the
    :func:`replace` function. For instance, the base64url url-safe and filename-safe encoding commonly used in
    web programming uses -_= as the last characters rather than +/=. To encode a base64url-encoded string,
    replace + and / with - and _ respectively.

    >>> df.select(f.replace(f.replace(f.to_base64('b'), '+', '-'), '/', '_').alias('websafe_base64')).show()
    +----------------+
    | websafe_base64 |
    +----------------+
    |           _-A= |
    +----------------+

    """
    return _invoke_function_over_column("TO_BASE64", col)


def transform(array: StringOrColumn, func: Callable[[Column], Column]) -> Column:
    """Return an array of elements after applying a transformation to each element in the input array.

    Examples
    --------
    >>> from bigquery_frame import functions as f
    >>> bq = BigQueryBuilder()
    >>> df = bq.sql("SELECT 1 as key, [1, 2, 3, 4] as values")
    >>> df.select(transform("values", lambda c: c * f.lit(2)).alias("doubled")).show()
    +--------------+
    |      doubled |
    +--------------+
    | [2, 4, 6, 8] |
    +--------------+

    >>> def alternate(x, i):
    ...     return when(i % 2 == 0, x).otherwise(-x)
    >>> df.select(transform("values", lambda x: f.when(x % 2 == 0, x).otherwise(-x)).alias("alternated")).show()
    +----------------+
    |     alternated |
    +----------------+
    | [-1, 2, -3, 4] |
    +----------------+

    >>> df = bq.sql("SELECT 1 as key, [STRUCT(1 as a, 2 as b), STRUCT(3 as a, 4 as b)] as s")
    >>> df.show()
    +-----+--------------------------------------+
    | key |                                    s |
    +-----+--------------------------------------+
    |   1 | [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}] |
    +-----+--------------------------------------+
    >>> df.select(transform("s",
    ...     lambda _ : f.struct(
    ...         f.expr("a * 2").alias("double_a"),
    ...         f.expr("b * 3").alias("triple_b")
    ...     )
    ... ).alias("s")).show()
    +-------------------------------------------------------------------+
    |                                                                 s |
    +-------------------------------------------------------------------+
    | [{'double_a': 2, 'triple_b': 6}, {'double_a': 6, 'triple_b': 12}] |
    +-------------------------------------------------------------------+

    :param array: `Column` or str name of column of type ARRAY
    :param func: `Column` or str specifying the transformation to apply.
        Array elements can be referred to as `_`. If the elements are structs, their fields can be referred to directly.
    :return:
    """
    str_array = str_to_col(array)
    return TransformedArrayColumn(str_array, func=func)


def upper(col: StringOrColumn) -> Column:
    """Converts a BYTES or STRING expression to upper case.

    For STRING arguments, returns the original string with all alphabetic characters in uppercase.
    Mapping between uppercase and uppercase is done according to the
    [Unicode Character Database](https://unicode.org/ucd/) without taking into account language-specific mappings.

    For BYTES arguments, the argument is treated as ASCII text, with all bytes greater than 127 left intact.

    Args:
        col: Target column to work on.

    Examples:
        >>> bq = BigQueryBuilder()
        >>> from bigquery_frame import functions as f
        >>> df = bq.sql('''SELECT item FROM UNNEST(['foo', 'bar', 'baz']) as item''')
        >>> df.show()
        +------+
        | item |
        +------+
        |  foo |
        |  bar |
        |  baz |
        +------+

        >>> df.select(f.upper("item").alias("item")).show()
        +------+
        | item |
        +------+
        |  FOO |
        |  BAR |
        |  BAZ |
        +------+
    """
    return _invoke_function_over_column("UPPER", col)


def when(condition: Column, value: Column) -> WhenColumn:
    """Evaluates a list of conditions and returns one of multiple possible result expressions.
    If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

    Examples
    --------
    >>> df = _get_test_df_1()
    >>> from bigquery_frame import functions as f
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
    return WhenColumn([(condition, value)])


def _get_test_df_1() -> DataFrame:
    bq = BigQueryBuilder()
    query = """
        SELECT * FROM UNNEST ([
            STRUCT(1 as col1, "a" as col2),
            STRUCT(1 as col1, "b" as col2),
            STRUCT(2 as col1, NULL as col2)
       ])
    """
    return bq.sql(query)


def _get_test_df_2() -> DataFrame:
    bq = BigQueryBuilder()
    query = """
        SELECT * FROM UNNEST ([
            STRUCT(NULL as a, NULL as b),
            STRUCT(1 as a, NULL as b),
            STRUCT(NULL as a, 2 as b)
       ])
    """
    return bq.sql(query)


def _get_test_df_3() -> DataFrame:
    bq = BigQueryBuilder()
    query = """
        SELECT * FROM UNNEST ([
            STRUCT(2 as col1),
            STRUCT(1 as col1),
            STRUCT(NULL as col1),
            STRUCT(3 as col1)
       ])
    """
    return bq.sql(query)
