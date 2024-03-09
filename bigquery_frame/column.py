from typing import Callable, List, Optional, Tuple, Union

from bigquery_frame.exceptions import AnalysisException, UnexpectedException
from bigquery_frame.temp_names import _get_temp_column_name
from bigquery_frame.utils import assert_true, indent, lit_to_col, lit_to_cols, quote, str_to_col, strip_margin

LitOrColumn = Union[object, "Column"]
ColumnOrName = Union[str, "Column"]


def cols_to_str(cols: List[ColumnOrName], indentation: Optional[int] = None, sep: str = ",") -> str:
    assert_true(isinstance(cols, (list, tuple)), UnexpectedException("Incorrect type"))
    str_cols = [str(col) for col in cols]
    if indentation is not None:
        return indent(f"{sep}\n".join(str_cols), indentation)
    else:
        return ", ".join(str_cols)


def _bin_op(op: str) -> Callable[["Column", LitOrColumn], "Column"]:
    def fun(self: "Column", other: LitOrColumn) -> "Column":
        other_col = lit_to_col(other)
        return Column(f"({self.expr}) {op} ({other_col.expr})")

    return fun


def _reverse_bin_op(op: str) -> Callable[["Column", LitOrColumn], "Column"]:
    def fun(self: "Column", other: LitOrColumn) -> "Column":
        other_col = lit_to_col(other)
        return Column(f"({other_col.expr}) {op} ({self.expr})")

    return fun


def _func_op(op: str) -> Callable[["Column"], "Column"]:
    def fun(self) -> "Column":
        return Column(f"{op} ({self.expr})")

    return fun


class Column:
    """Expression representing a column from a DataFrame or the result of an operation on such columns."""

    def __init__(self, expr: str):
        self._expr: str = expr
        self._alias: Optional[str] = None

    @property
    def expr(self):
        return self._expr

    def __str__(self):
        res = self.expr
        if self._alias is not None:
            res += f" AS {self._alias}"
        return res

    def __repr__(self):
        return f"Column<'{str(self)}'>"

    __add__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("+")
    __radd__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("+")
    __sub__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("-")
    __rsub__: Callable[["Column", LitOrColumn], "Column"] = _reverse_bin_op("-")
    __neg__: Callable[["Column"], "Column"] = _func_op("-")
    __mul__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("*")
    __rmul__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("*")
    __truediv__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("/")
    __rtruediv__: Callable[["Column", LitOrColumn], "Column"] = _reverse_bin_op("/")
    __and__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("AND")
    __rand__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("AND")
    __or__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("OR")
    __ror__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("OR")
    __invert__ = _func_op("NOT")

    def __mod__(self, other: LitOrColumn) -> "Column":
        other_col = lit_to_col(other)
        return Column(f"MOD({self.expr}, {other_col.expr})")

    # logistic operators
    __eq__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("=")
    __ne__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("<>")
    __lt__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("<")
    __le__: Callable[["Column", LitOrColumn], "Column"] = _bin_op("<=")
    __ge__: Callable[["Column", LitOrColumn], "Column"] = _bin_op(">=")
    __gt__: Callable[["Column", LitOrColumn], "Column"] = _bin_op(">")

    def __bool__(self):
        raise ValueError(
            "Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
            "'~' for 'not' when building DataFrame boolean expressions."
        )

    def __getitem__(self, item: Union[str, int]):
        """Returns the column as a :class:`Column`.

        Examples:
            >>> from bigquery_frame import BigQueryBuilder
            >>> bq = BigQueryBuilder()
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

    def alias(self, alias: Optional[str]) -> "Column":
        if alias is not None:
            alias = quote(alias)
        return Column(self.expr)._copy_from(self, alias)

    def asc(self) -> "Column":
        """Returns a sort expression based on the ascending order of the given column.

        Examples:
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

        Args:
            col_type: A string representing a BigQuery type.

        Returns:
            a Column expression.

        Examples:
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
        """
        return Column(f"CAST({self.expr} as {col_type.upper()})").alias(self._alias)

    def desc(self) -> "Column":
        """Returns a sort expression based on the descending order of the given column.

        Examples:
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

        Args:
            other: A Column expression or a literal.

        Returns:
            A Column expression.

        Examples:
            >>> from bigquery_frame import BigQueryBuilder
            >>> from bigquery_frame import functions as f
            >>> bq = BigQueryBuilder()
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
        """
        from bigquery_frame import functions as f

        if not isinstance(other, Column):
            other = f.lit(other)
        return (self.isNull() & other.isNull()) | (self.isNotNull() & other.isNotNull() & (self == other))

    def isin(self, *cols: LitOrColumn) -> "Column":
        """A boolean expression that is evaluated to true if the value of this
        expression is contained by the evaluated values of the arguments.

        Args:
            *cols: One or more column expression to compare this column against

        Returns:
            A Column expression of Boolean type that evaluates to true when the value of this column matches
            one of the values of `cols`.

        Examples:
            >>> from bigquery_frame import BigQueryBuilder
            >>> bq = BigQueryBuilder()
            >>> df = bq.sql('''
            ...     SELECT * FROM UNNEST ([
            ...         STRUCT("Alice" as name, 2 as age),
            ...         STRUCT("Bob" as name, 5 as age)
            ...    ])
            ... ''')
            >>> df.show()
            +-------+-----+
            |  name | age |
            +-------+-----+
            | Alice |   2 |
            |   Bob |   5 |
            +-------+-----+
            >>> from bigquery_frame import functions as f
            >>> df.filter(df["name"].isin("Bob", "Mike")).show()
            +------+-----+
            | name | age |
            +------+-----+
            |  Bob |   5 |
            +------+-----+
        """
        cols = lit_to_cols(cols)
        return Column(f"(({self.expr}) IN ({cols_to_str(cols)}))")

    def isNull(self) -> "Column":
        """True if the current expression is null.

        Returns:
            A Column expression.

        Examples:
            >>> from bigquery_frame import BigQueryBuilder
            >>> bq = BigQueryBuilder()
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
        """
        return Column(f"(({self.expr}) IS NULL)")

    def isNotNull(self) -> "Column":
        """True if the current expression is NOT null.

        Returns:
            A Column expression.

        Examples:
            >>> from bigquery_frame import BigQueryBuilder
            >>> bq = BigQueryBuilder()
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
        """
        return Column(f"(({self.expr}) IS NOT NULL)")

    def get_alias(self) -> Optional[str]:
        return self._alias

    asType = cast


class WhenColumn(Column):
    """Special type of [Column][bigquery_frame.Column] returned by
    [bigquery_frame.functions.when][bigquery_frame.functions.when]
    """

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
        If [otherwise][bigquery_frame.column.Column.otherwise] is not invoked, None is returned for
        unmatched conditions.

        See Also:
            - [bigquery_frame.functions.when][bigquery_frame.functions.when]

        Args:
            condition: A Column expression.
            value: A Column expression.

        Returns:
            A Column expression.

        Examples:
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
        """
        return WhenColumn([*self._when_condition, (condition, value)])._copy_from(self)

    def otherwise(self, value: "Column") -> "Column":
        """Evaluates a list of conditions and returns one of multiple possible result expressions.
        If [otherwise][bigquery_frame.column.Column.otherwise] is not invoked, None is returned for
        unmatched conditions.

        See Also:
            - [bigquery_frame.functions.when][bigquery_frame.functions.when]

        Args:
            value: A Column expression.

        Returns:
            A Column expression.

        Examples:
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
        """
        return Column(self._compile(value)).alias(self._alias)


class SortedArrayColumn(Column):
    def __init__(
        self,
        array: Column,
        sort_keys: Optional[Callable[[Column], Union[Column, List[Column]]]],
    ) -> None:
        super().__init__("")
        self._array: Column = array
        self._sort_keys: Optional[Callable[[Column], Union[Column, List[Column]]]] = sort_keys

    def _compile(self):
        temp_col_alias = quote(_get_temp_column_name())
        temp_col = Column(expr=temp_col_alias)
        array = str_to_col(self._array)
        if self._sort_keys is None:
            sort_keys = [temp_col]
        else:
            sort_keys = self._sort_keys(temp_col)
            if not isinstance(sort_keys, list):
                sort_keys = [sort_keys]
        sort_str = f"\n  ORDER BY {cols_to_str(sort_keys)}"

        return strip_margin(
            f"""
            |ARRAY(
            |  SELECT
            |    {temp_col_alias}
            |  FROM UNNEST({array}) as {temp_col_alias}
            |  {sort_str}
            |)"""
        )

    @property
    def expr(self):
        return self._compile()


class TransformedArrayColumn(Column):
    def __init__(self, array: Column, func: Callable[[Column], Column]) -> None:
        super().__init__("")
        self._array: Column = array
        self._func: Callable[[Column], Column] = func

    def _compile(self):
        temp_col_alias = quote(_get_temp_column_name())
        temp_col = Column(expr=temp_col_alias)
        array = str_to_col(self._array)

        return strip_margin(
            f"""
            |ARRAY(
            |  SELECT
            |    {self._func(temp_col)}
            |  FROM UNNEST({array}) as {temp_col_alias}
            |)"""
        )

    @property
    def expr(self):
        return self._compile()


class ExplodedColumn(Column):
    def __init__(self, exploded_col: Column, with_index: bool, outer: bool) -> None:
        super().__init__("")
        self.exploded_col: Column = exploded_col
        self.with_index: bool = with_index
        self.outer: bool = outer

    @property
    def expr(self):
        raise AnalysisException("Exploded columns cannot be transformed in the same select clause.")

    def alias(self, alias: Optional[str]) -> "ExplodedColumn":
        if alias is not None:
            alias = quote(alias)
        return ExplodedColumn(self.exploded_col, self.with_index, self.outer)._copy_from(self, alias)

    def __str__(self):
        return f"UNNEST({self.exploded_col})"
