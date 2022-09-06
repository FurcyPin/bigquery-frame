from typing import Callable, Iterable, List, Optional, Tuple, Union

from bigquery_frame.conf import ELEMENT_COL_NAME
from bigquery_frame.exceptions import IllegalArgumentException
from bigquery_frame.utils import indent, lit_to_col, quote, str_to_col, strip_margin

LitOrColumn = Union[object, "Column"]
StringOrColumn = Union[str, "Column"]


def cols_to_str(cols: Iterable[StringOrColumn], indentation: Optional[int] = None, sep: str = ",") -> str:
    cols = [str(col) for col in cols]
    if indentation is not None:
        return indent(f"{sep}\n".join(cols), indentation)
    else:
        return ", ".join(cols)


def literal_col(val: LitOrColumn) -> "Column":
    if val is None:
        return Column("NULL")
    if type(val) == str:
        return Column(f"'{val}'")
    if type(val) in [bool, int, float]:
        return Column(str(val))
    raise IllegalArgumentException(f"lit({val}): The type {type(val)} is not supported yet.")


def _bin_op(op: str) -> Callable[["Column", LitOrColumn], "Column"]:
    def fun(self, other: LitOrColumn) -> "Column":
        other = lit_to_col(other)
        return Column(f"({self.expr}) {op} ({other.expr})")

    return fun


def _reverse_bin_op(op: str) -> Callable[["Column", LitOrColumn], "Column"]:
    def fun(self, other: LitOrColumn) -> "Column":
        other = lit_to_col(other)
        return Column(f"({other.expr}) {op} ({self.expr})")

    return fun


def _func_op(op: str) -> Callable[["Column"], "Column"]:
    def fun(self) -> "Column":
        return Column(f"{op} ({self.expr})")

    return fun


class Column:
    def __init__(self, expr: str):
        self._expr: str = expr
        self._alias: Optional[str] = None

    @property
    def expr(self):
        return self._expr

    def __str__(self):
        res = self.expr
        if self._alias is not None:
            res += f" as {self._alias}"
        return res

    def __repr__(self):
        return f"Column('{self.expr}')"

    __add__: Callable[[LitOrColumn], "Column"] = _bin_op("+")
    __radd__: Callable[[LitOrColumn], "Column"] = _bin_op("+")
    __sub__: Callable[[LitOrColumn], "Column"] = _bin_op("-")
    __rsub__: Callable[[LitOrColumn], "Column"] = _reverse_bin_op("-")
    __neg__: Callable[[], "Column"] = _func_op("-")
    __mul__: Callable[[LitOrColumn], "Column"] = _bin_op("*")
    __rmul__: Callable[[LitOrColumn], "Column"] = _bin_op("*")
    __truediv__: Callable[[LitOrColumn], "Column"] = _bin_op("/")
    __rtruediv__: Callable[[LitOrColumn], "Column"] = _reverse_bin_op("/")
    __and__: Callable[[LitOrColumn], "Column"] = _bin_op("AND")
    __rand__: Callable[[LitOrColumn], "Column"] = _bin_op("AND")
    __or__: Callable[[LitOrColumn], "Column"] = _bin_op("OR")
    __ror__: Callable[[LitOrColumn], "Column"] = _bin_op("OR")
    __invert__ = _func_op("NOT")

    def __mod__(self, other: LitOrColumn) -> "Column":
        other = lit_to_col(other)
        return Column(f"MOD({self.expr}, {other.expr})")

    # logistic operators
    __eq__: Callable[[LitOrColumn], "Column"] = _bin_op("=")
    __ne__: Callable[[LitOrColumn], "Column"] = _bin_op("<>")
    __lt__: Callable[[LitOrColumn], "Column"] = _bin_op("<")
    __le__: Callable[[LitOrColumn], "Column"] = _bin_op("<=")
    __ge__: Callable[[LitOrColumn], "Column"] = _bin_op(">=")
    __gt__: Callable[[LitOrColumn], "Column"] = _bin_op(">")

    def __bool__(self):
        raise ValueError(
            "Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
            "'~' for 'not' when building DataFrame boolean expressions."
        )

    def __getitem__(self, item: Union[str, int]):
        """Returns the column as a :class:`Column`.

        Examples
        --------
        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> df = bq.sql('SELECT STRUCT([1, 2, 3] as a, "x" as b) s')
        >>> df.show()
        +----------------------------+
        |                          s |
        +----------------------------+
        | {'a': [1, 2, 3], 'b': 'x'} |
        +----------------------------+
        >>> df.select(df["s"]["b"]).show()
        +---+
        | b |
        +---+
        | x |
        +---+

        >>> df.select(df["s"]["a"][0].alias("a_0")).show()
        +-----+
        | a_0 |
        +-----+
        |   1 |
        +-----+
        """
        if isinstance(item, str):
            return Column(f"{self.expr}.{quote(item)}")
        elif isinstance(item, int):
            return Column(f"{self.expr}[OFFSET({item})]")
        else:
            raise TypeError("unexpected item type: %s" % type(item))

    def _copy_from(self, col: "Column", alias: Optional[str] = None):
        if alias is not None:
            self._alias = alias
        else:
            self._alias = col._alias
        return self

    def alias(self, alias: str) -> "Column":
        if alias is not None:
            alias = quote(alias)
        return Column(self.expr)._copy_from(self, alias)

    def asc(self) -> "Column":
        """Returns a sort expression based on the ascending order of the given column.

        >>> from bigquery_frame.functions import _get_test_df_3
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
        >>> df.sort(df["col1"].asc()).show()
        +------+
        | col1 |
        +------+
        | null |
        |    1 |
        |    2 |
        |    3 |
        +------+
        """
        return Column(f"{self.expr} ASC").alias(self._alias)

    def cast(self, col_type: str):
        """Casts the column into the specified
        `BigQuery type <https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules>`_

        Examples
        --------
        >>> from bigquery_frame import functions as f
        >>> df = f._get_test_df_1()
        >>> df.show()
        +------+------+
        | col1 | col2 |
        +------+------+
        |    1 |    a |
        |    1 |    b |
        |    2 | null |
        +------+------+
        >>> df.select(df['col1'].cast('float64').alias("col1_float"), 'col2').show()
        +------------+------+
        | col1_float | col2 |
        +------------+------+
        |        1.0 |    a |
        |        1.0 |    b |
        |        2.0 | null |
        +------------+------+

        :param col_type: a string representing a BigQuery type
        :return: a :class:`Column` expression.
        """
        return Column(f"CAST({self.expr} as {col_type.upper()})").alias(self._alias)

    def desc(self) -> "Column":
        """Returns a sort expression based on the descending order of the given column.

        >>> from bigquery_frame.functions import _get_test_df_3
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
        >>> df.sort(df["col1"].desc()).show()
        +------+
        | col1 |
        +------+
        |    3 |
        |    2 |
        |    1 |
        | null |
        +------+
        """
        return Column(f"{self.expr} DESC").alias(self._alias)

    def eqNullSafe(self, other: LitOrColumn) -> "Column":
        """Equality test that is safe for null values.

        Examples
        --------
        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> from bigquery_frame import functions as f
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> df = bq.sql('''
        ...     SELECT * FROM UNNEST ([
        ...         STRUCT("a" as col1, "a" as col2),
        ...         STRUCT("a" as col1, "b" as col2),
        ...         STRUCT("a" as col1, NULL as col2),
        ...         STRUCT(NULL as col1, "c" as col2),
        ...         STRUCT(NULL as col1, NULL as col2)
        ...    ])
        ... ''')
        >>> df.show()
        +------+------+
        | col1 | col2 |
        +------+------+
        |    a |    a |
        |    a |    b |
        |    a | null |
        | null |    c |
        | null | null |
        +------+------+
        >>> (df.withColumn("equality", f.col('col1') == f.col('col2'))
        ...    .withColumn("eqNullSafe", f.col('col1').eqNullSafe(f.col('col2')))).show()
        +------+------+----------+------------+
        | col1 | col2 | equality | eqNullSafe |
        +------+------+----------+------------+
        |    a |    a |     True |       True |
        |    a |    b |    False |      False |
        |    a | null |     null |      False |
        | null |    c |     null |      False |
        | null | null |     null |       True |
        +------+------+----------+------------+

        Warning: literals are converted to strings
        >>> (df.withColumn("lit", f.col('col1').eqNullSafe('col2'))
        ...    .withColumn("col", f.col('col1').eqNullSafe(f.col('col2')))).show()
        +------+------+-------+-------+
        | col1 | col2 |   lit |   col |
        +------+------+-------+-------+
        |    a |    a | False |  True |
        |    a |    b | False | False |
        |    a | null | False | False |
        | null |    c | False | False |
        | null | null | False |  True |
        +------+------+-------+-------+

        :param other: a :class:`Column` expression or a literal.
        :return: a :class:`Column` expression.
        """
        if not isinstance(other, Column):
            other = literal_col(other)
        return (self.isNull() & other.isNull()) | (self.isNotNull() & other.isNotNull() & (self == other))

    def isNull(self) -> "Column":
        """True if the current expression is null.

        Examples
        --------
        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> df = bq.sql('''
        ...     SELECT * FROM UNNEST ([
        ...         STRUCT("a" as col1, "a" as col2),
        ...         STRUCT("a" as col1, "b" as col2),
        ...         STRUCT("a" as col1, NULL as col2),
        ...         STRUCT(NULL as col1, "c" as col2),
        ...         STRUCT(NULL as col1, NULL as col2)
        ...    ])
        ... ''')
        >>> df.show()
        +------+------+
        | col1 | col2 |
        +------+------+
        |    a |    a |
        |    a |    b |
        |    a | null |
        | null |    c |
        | null | null |
        +------+------+
        >>> df.filter(df["col1"].isNull()).show()
        +------+------+
        | col1 | col2 |
        +------+------+
        | null |    c |
        | null | null |
        +------+------+

        :return: a :class:`Column` expression.
        """
        return Column(f"(({self.expr}) IS NULL)")

    def isNotNull(self) -> "Column":
        """True if the current expression is NOT null.

        Examples
        --------
        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> df = bq.sql('''
        ...     SELECT * FROM UNNEST ([
        ...         STRUCT("a" as col1, "a" as col2),
        ...         STRUCT("a" as col1, "b" as col2),
        ...         STRUCT("a" as col1, NULL as col2),
        ...         STRUCT(NULL as col1, "c" as col2),
        ...         STRUCT(NULL as col1, NULL as col2)
        ...    ])
        ... ''')
        >>> df.show()
        +------+------+
        | col1 | col2 |
        +------+------+
        |    a |    a |
        |    a |    b |
        |    a | null |
        | null |    c |
        | null | null |
        +------+------+
        >>> df.filter(df["col1"].isNotNull()).show()
        +------+------+
        | col1 | col2 |
        +------+------+
        |    a |    a |
        |    a |    b |
        |    a | null |
        +------+------+

        :return: a :class:`Column` expression.
        """
        return Column(f"(({self.expr}) IS NOT NULL)")

    def get_alias(self) -> str:
        return self._alias

    asType = cast


class WhenColumn(Column):
    def __init__(self, when_condition: List[Tuple["Column", "Column"]]):
        super().__init__("")
        self._when_condition: List[Tuple["Column", "Column"]] = when_condition

    def _compile(self, when_default: Optional[Column] = None):
        conditions_str = [f"WHEN {condition} THEN {value}" for condition, value in self._when_condition]
        if when_default is not None:
            default_str = f"\n  ELSE {when_default}"
        else:
            default_str = ""
        res = strip_margin(
            f"""
            |CASE
            |{cols_to_str(conditions_str, indentation=2, sep="")}{default_str}
            |END"""
        )
        return res

    @property
    def expr(self):
        return self._compile()

    def when(self, condition: "Column", value: "Column") -> "WhenColumn":
        """Evaluates a list of conditions and returns one of multiple possible result expressions.
        If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

        Examples
        --------
        >>> from bigquery_frame import functions as f
        >>> df = f._get_test_df_1()
        >>> df.select("col1", f.when(f.col("col1") > f.lit(1), f.lit("yes")).otherwise(f.lit("no"))).show()
        +------+-----+
        | col1 | f0_ |
        +------+-----+
        |    1 |  no |
        |    1 |  no |
        |    2 | yes |
        +------+-----+
        >>> df.select("col1", f.when(f.col("col1") > f.lit(1), f.lit("yes"))).show()
        +------+------+
        | col1 |  f0_ |
        +------+------+
        |    1 | null |
        |    1 | null |
        |    2 |  yes |
        +------+------+

        See Also
        --------
        bigquery_frame.functions.when

        :param condition: a boolean :class:`Column` expression.
        :param value: a :class:`Column` expression.
        :return:
        """
        return WhenColumn([*self._when_condition, (condition, value)])._copy_from(self)

    def otherwise(self, value: "Column") -> "Column":
        """Evaluates a list of conditions and returns one of multiple possible result expressions.
        If :func:`Column.otherwise` is not invoked, None is returned for unmatched conditions.

        Examples
        --------
        >>> from bigquery_frame import functions as f
        >>> df = f._get_test_df_1()
        >>> df.select("col1", f.when(f.col("col1") > f.lit(1), f.lit("yes")).otherwise(f.lit("no"))).show()
        +------+-----+
        | col1 | f0_ |
        +------+-----+
        |    1 |  no |
        |    1 |  no |
        |    2 | yes |
        +------+-----+

        See Also
        --------
        bigquery_frame.functions.when

        :param value: a literal value, or a :class:`Column` expression.
        :return:
        """
        return Column(self._compile(value)).alias(self._alias)


class ArrayColumn(Column):
    def __init__(
        self, array: Column, transform_col: Column = Column("*"), sort_cols: Optional[Union[List[Column]]] = None
    ) -> None:
        super().__init__("")
        self._array: Column = array
        self._transform_col: Optional[Column] = transform_col
        self._sort_cols: Optional[List[Column]] = sort_cols

    def _compile(self):
        array = str_to_col(self._array)
        sort_str = ""
        if self._sort_cols is not None:
            sort_str = f"\n  ORDER BY {', '.join([col.expr for col in self._sort_cols])}"

        return strip_margin(
            f"""
            |ARRAY(
            |  SELECT
            |    {self._transform_col}
            |  FROM UNNEST({array}) as {quote(ELEMENT_COL_NAME)}{sort_str}
            |)"""
        )

    def _copy(
        self, transform_col: Optional[Column] = None, sort_cols: Optional[Union[List[Column]]] = None
    ) -> "ArrayColumn":
        c = ArrayColumn(self._array)._copy_from(self)
        if transform_col is not None:
            c._transform_col = transform_col
        else:
            c._transform_col = self._transform_col
        if sort_cols is not None:
            c._sort_cols = sort_cols
        else:
            c._sort_cols = self._sort_cols
        return c

    @property
    def expr(self):
        return self._compile()
