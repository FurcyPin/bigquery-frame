from typing import Callable, Iterable, List, Optional, Tuple, Union

from bigquery_frame.exceptions import IllegalArgumentException
from bigquery_frame.utils import indent, strip_margin

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
        if not isinstance(other, Column):
            other = literal_col(other)
        return Column(f"({self.expr}) {op} ({other.expr})")

    return fun


def _reverse_bin_op(op: str) -> Callable[["Column", LitOrColumn], "Column"]:
    def fun(self, other: LitOrColumn) -> "Column":
        if not isinstance(other, Column):
            other = literal_col(other)
        return Column(f"({other.expr}) {op} ({self.expr})")

    return fun


def _func_op(op: str) -> Callable[["Column"], "Column"]:
    def fun(self) -> "Column":
        return Column(f"{op} ({self.expr})")

    return fun


class Column:
    def __init__(self, expr: str, alias: Optional[str] = None):
        self.expr = expr
        self._alias = alias
        self._when_condition: Optional[List[Tuple["Column", "Column"]]] = None
        self._when_default: Optional["Column"] = None

    def __str__(self):
        if self._when_condition is not None:
            conditions_str = [f"WHEN {condition} THEN {value}" for condition, value in self._when_condition]
            if self._when_default is not None:
                default_str = f"\n  ELSE {self._when_default}"
            else:
                default_str = ""
            res = strip_margin(
                f"""
                |CASE
                |{cols_to_str(conditions_str, indentation=2, sep="")}{default_str}
                |END"""
            )
        else:
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

    def alias(self, alias: str) -> "Column":
        return Column(self.expr, alias)

    def asType(self, col_type: str) -> "Column":
        return Column(expr=f"CAST({self.expr} as {col_type})", alias=self._alias)

    def when(self, condition: "Column", value: "Column") -> "Column":
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

        :param condition: a boolean :class:`Column` expression.
        :param value: a :class:`Column` expression.
        :return:
        """
        if self._when_condition is None:
            raise IllegalArgumentException("when() can only be applied on a Column previously generated by when()")
        else:
            c = Column(expr=self.expr, alias=self._alias)
            c._alias = self._alias
            c._when_condition = [*self._when_condition, (condition, value)]
            return c

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
        if self._when_default is not None:
            raise IllegalArgumentException(
                "otherwise() can only be applied once on a Column previously generated by when()"
            )
        else:
            c = Column(expr=self.expr, alias=self._alias)
            c._when_condition = self._when_condition
            c._when_default = value
            return c
