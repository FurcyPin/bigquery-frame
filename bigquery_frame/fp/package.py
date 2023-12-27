from typing import Any, Callable, cast

from bigquery_frame.fp.printable_function import PrintableFunction


def __compose(f1: PrintableFunction, f2: PrintableFunction) -> PrintableFunction:
    """Composes together two PrintableFunctions.

    For instance, if `h = compose(g, f)`, then for every `x`, `h(x) = g(f(x))`.

    Args:
        f1: A PrintableFunction
        f2: A PrintableFunction

    Returns:
        The composition of f1 with f2.

    Examples:
        >>> f = PrintableFunction(lambda x: x+1, lambda s: f'{s} + 1')
        >>> g = PrintableFunction(lambda x: x.cast("Double"), lambda s: f'({s}).cast("Double")')
        >>> __compose(g, f)
        lambda x: (x + 1).cast("Double")
        >>> __compose(f, g)
        lambda x: (x).cast("Double") + 1

        >>> h = PrintableFunction(lambda x: x*2, "h")
        >>> __compose(h, f)
        lambda x: h(x + 1)
        >>> __compose(f, h)
        h + 1
        >>> __compose(h, h)
        h(h)
    """

    def f1f2(s: Any) -> Any:
        return f1.func(f2.func(s))

    if callable(f1.alias) and callable(f2.alias):
        c1 = cast(Callable, f1.alias)
        c2 = cast(Callable, f2.alias)
        return PrintableFunction(f1f2, lambda s: c1(c2(s)))
    elif callable(f1.alias) and not callable(f2.alias):
        c1 = cast(Callable, f1.alias)
        a2 = str(f2.alias)
        return PrintableFunction(f1f2, c1(a2))
    elif not callable(f1.alias) and callable(f2.alias):
        a1 = str(f1.alias)
        c2 = cast(Callable, f2.alias)
        return PrintableFunction(f1f2, lambda s: f"{a1}({c2(s)})")
    else:
        a1 = str(f1.alias)
        a2 = str(f2.alias)
        return PrintableFunction(f1f2, f"{a1}({a2})")


def compose(f1: PrintableFunction, f2: PrintableFunction, *f3: PrintableFunction) -> PrintableFunction:
    """Composes together two or more PrintableFunctions.
    For instance, if `h = compose(g, f)`, then for every `x`, `h(x) = g(f(x))`.

    Args:
        f1: A PrintableFunction
        f2: A PrintableFunction

    Returns:
        The composition of f1 with f2.

    Examples:
        >>> f = PrintableFunction(lambda x: x+1, lambda s: f'{s} + 1')
        >>> g = PrintableFunction(lambda x: x.cast("Double"), lambda s: f'({s}).cast("Double")')
        >>> h = PrintableFunction(lambda x: x*2, lambda x: f"{x}*2")
        >>> compose(f, g, h)
        lambda x: (x*2).cast("Double") + 1

    """
    res = __compose(f1, f2)
    for f in f3:
        res = __compose(res, f)
    return res
