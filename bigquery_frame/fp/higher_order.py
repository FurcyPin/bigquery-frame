from typing import Any, Callable, List, Optional

from bigquery_frame import DataFrame, fp
from bigquery_frame import functions as f
from bigquery_frame.column import Column
from bigquery_frame.fp import PrintableFunction
from bigquery_frame.utils import quote
from bigquery_frame.utils import str_to_col as _str_to_col


def alias(name: str) -> PrintableFunction:
    """Return a PrintableFunction version of the `bigquery_frame.Column.alias` method"""
    return PrintableFunction(lambda s: s.alias(name), lambda s: str(s) + f".alias({name!r})")


identity = PrintableFunction(lambda s: s, lambda s: str(s))
struct = PrintableFunction(lambda x: f.struct(x), lambda x: f"f.struct({x})")
str_to_col = PrintableFunction(lambda x: _str_to_col(x), lambda s: str(s))


def struct_get(key: str) -> PrintableFunction:
    """Return a PrintableFunction that gets a struct's subfield, unless the struct is None,
    in which case it returns a Column expression for the field itself.

    Get a column's subfield, unless the column is None, in which case it returns
    a Column expression for the field itself.

    Examples:
        >>> struct_get("c")
        lambda x: x['c']
        >>> struct_get("c").alias(None)
        "f.col('c')"
    """

    def _safe_struct_get(s: Optional[Column], field: str) -> Column:
        if s is None:
            return f.col(field)
        else:
            if ("." in field or "!" in field) and isinstance(s, DataFrame):
                return s[quote(field)]
            else:
                return s[field]

    def _safe_struct_get_alias(s: Optional[str], field: str) -> str:
        if s is None:
            return f"f.col({field!r})"
        else:
            return f"{s}[{field!r}]"

    return PrintableFunction(lambda s: _safe_struct_get(s, key), lambda s: _safe_struct_get_alias(s, key))


def recursive_struct_get(keys: List[str]) -> PrintableFunction:
    """Return a PrintableFunction that recursively applies get to a nested structure.

    Examples:
        >>> recursive_struct_get([])
        lambda x: x
        >>> recursive_struct_get(["a", "b", "c"])
        lambda x: x['a']['b']['c']
        >>> recursive_struct_get(["a", "b", "c"]).alias(None)
        "f.col('a')['b']['c']"
    """
    if len(keys) == 0:
        return identity
    else:
        return fp.compose(recursive_struct_get(keys[1:]), struct_get(keys[0]))


def transform(transformation: PrintableFunction) -> PrintableFunction:
    """Return a PrintableFunction version of the `bigquery_frame.functions.transform` method,
    which applies the given transformation to any array column.
    """
    return PrintableFunction(
        lambda x: f.transform(x, transformation.func),
        lambda x: f"f.transform({x}, lambda x: {transformation.alias('x')})",
    )


def _partial_box_right(func: Callable, args: Any) -> Callable:
    """Given a function and an array of arguments, return a new function that takes an argument, add it to the
    array, and pass it to the original function."""
    if isinstance(args, str):
        args = [args]
    return lambda a: func([*args, a])


def boxed_transform(transformation: PrintableFunction, parents: List[str]) -> PrintableFunction:
    """Return a PrintableFunction version of the `bigquery_frame.functions.transform` method,
    which applies the given transformation to any array column.
    """
    return PrintableFunction(
        lambda x: f.transform(recursive_struct_get(parents)(x[-1]), _partial_box_right(transformation.func, x)),
        lambda x: f"f.transform({recursive_struct_get(parents).alias(x[-1])}, "
        f"lambda x: {_partial_box_right(transformation.alias, x)('x')})",
    )
