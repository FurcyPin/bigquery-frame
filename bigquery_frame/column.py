from typing import Optional


def _bin_op(op: str):
    def fun(self, other: 'Column'):
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

    __sub__ = _bin_op("-")
    __add__ = _bin_op("+")
    __mul__ = _bin_op("*")
    __div__ = _bin_op("/")

    def alias(self, alias: str) -> "Column":
        return Column(self.expr, alias)

    def asType(self, col_type: str) -> "Column":
        return Column(expr=f"CAST({self.expr} as {col_type})", alias=self._alias)
