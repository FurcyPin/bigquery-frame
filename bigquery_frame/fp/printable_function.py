import inspect
from typing import Any, Callable, cast


class PrintableFunction:
    """Wrapper for anonymous functions with a short description making them much human-friendly when printed.

    Very useful when debugging, useless otherwise.

    Args:
        func: A function that takes a Column and return a Column.
        alias: A string or a function that takes a string and return a string.

    Examples:
        >>> print(PrintableFunction(lambda s: s["c"], "my_function"))
        my_function

        >>> print(PrintableFunction(lambda s: s["c"], lambda s: f'{s}["c"]'))
        lambda x: x["c"]

        >>> func = PrintableFunction(lambda s: s.cast("Double"), lambda s: f'{s}.cast("Double")')
        >>> print(func.alias("s"))
        s.cast("Double")

        Composition:

        >>> f1 = PrintableFunction(lambda s: s.cast("Double"), lambda s: f'{s}.cast("Double")')
        >>> f2 = PrintableFunction(lambda s: s * s, lambda s: f'{f"({s} * {s})"}')
        >>> f2_then_f1 = PrintableFunction(lambda s: f1(f2(s)), lambda s: f1.alias(f2.alias(s)))
        >>> print(f2_then_f1)
        lambda x: (x * x).cast("Double")
    """

    def __init__(self, func: Callable[[Any], Any], alias: Callable[[Any], str]) -> None:
        self.func: Callable[[Any], Any] = func
        self.alias: Callable[[Any], str] = alias

    def __repr__(self) -> str:
        if callable(self.alias):
            return f"lambda x: {cast(Callable, self.alias)('x')}"
        else:
            return cast(str, self.alias)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)

    def boxed(self) -> "PrintableFunction":
        """Return a boxed version of this method."""
        return PrintableFunction(box(self.func), box(self.alias))


def arity(function: Callable) -> int:
    """Return the arity of the given function"""
    sig = inspect.signature(function)
    arity = len(sig.parameters)
    return arity


def box(func: Callable) -> Callable:
    """Transform a constant or function that takes any number `n` of arguments into a function that takes
    a single argument of type array and passes the `n` right-most arguments to that function.

    Examples:
        >>> func = lambda a, b, c: f"{a}.{b}.{c}"
        >>> boxed_func = box(func)
        >>> boxed_func(["1", "2", "3"])
        '1.2.3'
        >>> boxed_func(["1", "2", "3", "4", "5"])
        '3.4.5'

        >>> func_no_arg = lambda: "a"
        >>> boxed_func_no_arg = box(func_no_arg)
        >>> boxed_func_no_arg(["1", "2"])
        'a'

        >>> constant = "a"
        >>> boxed_constant = box(constant)
        >>> boxed_constant(["1", "2", "3", "4", "5"])
        'a'

    """
    if not callable(func):
        return lambda x: func
    n = arity(func)
    if n > 0:
        return lambda x: func(*x[-n:])
    else:
        return lambda x: func()
