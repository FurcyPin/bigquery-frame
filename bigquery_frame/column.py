from typing import Optional, Callable


def _bin_op(op: str) -> Callable[['Column', 'Column'], 'Column']:
    def fun(self, other: 'Column') -> 'Column':
        return Column(f"{self.expr} {op} {other.expr}")

    return fun


class Column:

    def __init__(self, expr: str, alias: Optional[str] = None):
        self.expr = expr
        self._alias = alias

    def __str__(self):
        if self._alias is None:
            return self.expr
        else:
            return f"{self.expr} as {self._alias}"

    def __repr__(self):
        return f"Column('{self.expr}')"

    __add__: Callable[['Column'], 'Column'] = _bin_op("+")
    __sub__: Callable[['Column'], 'Column'] = _bin_op("-")
    __mul__: Callable[['Column'], 'Column'] = _bin_op("*")
    __truediv__: Callable[['Column'], 'Column'] = _bin_op("/")
    __and__: Callable[['Column'], 'Column'] = _bin_op("AND")
    __or__: Callable[['Column'], 'Column'] = _bin_op("OR")

    # logistic operators
    __eq__: Callable[['Column'], 'Column'] = _bin_op("=")
    __ne__: Callable[['Column'], 'Column'] = _bin_op("<>")
    __lt__: Callable[['Column'], 'Column'] = _bin_op("<")
    __le__: Callable[['Column'], 'Column'] = _bin_op("<=")
    __ge__: Callable[['Column'], 'Column'] = _bin_op(">=")
    __gt__: Callable[['Column'], 'Column'] = _bin_op(">")

    def alias(self, alias: str) -> "Column":
        return Column(self.expr, alias)

    def asType(self, col_type: str) -> "Column":
        return Column(expr=f"CAST({self.expr} as {col_type})", alias=self._alias)
