import math
import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, TypeVar, Union

if TYPE_CHECKING:
    from bigquery_frame import Column
    from bigquery_frame.column import ColumnOrName, LitOrColumn

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")

MAX_JAVA_INT = 2147483647


def strip_margin(text: str):
    """For every line in this string, strip a leading prefix consisting of whitespace, tabs and carriage returns
    followed by | from the line.

    If the first character is a newline, it is also removed.
    This method is inspired from Scala's String.stripMargin.

    Args:
        text: A multi-line string

    Returns:
        A stripped string

    Examples:
        >>> print(strip_margin('''
        ...     |a
        ...     |b
        ...     |c'''))
        a
        b
        c
        >>> print(strip_margin('''a
        ... |b
        ...   |c
        ...     |d'''))
        a
        b
        c
        d
    """
    s = re.sub(r"\n[ \t\r]*\|", "\n", text)
    if s.startswith("\n"):
        return s[1:]
    else:
        return s


def indent(str, nb) -> str:
    return " " * nb + str.replace("\n", "\n" + " " * nb)


def group_by_key(items: Iterable[tuple[K, V]]) -> dict[K, list[V]]:
    """Group the values of a list of tuples by their key.

    Args:
        items: An iterable of tuples (key, value).

    Returns:
        A dictionary where the keys are the keys from the input tuples,
        and the values are lists of the corresponding values.

    Examples:
        >>> items = [('a', 1), ('b', 2), ('a', 3), ('c', 4), ('b', 5)]
        >>> group_by_key(items)
        {'a': [1, 3], 'b': [2, 5], 'c': [4]}
        >>> group_by_key([])
        {}
    """
    result: dict[K, list[V]] = {}
    for key, value in items:
        if key in result:
            result[key].append(value)
        else:
            result[key] = [value]
    return result


def quote(string) -> str:
    """Add quotes around a column or table names to prevent collision with SQL keywords.
    This method is idempotent: it does not add new quotes to an already quoted string.
    If the column name is a reference to a nested column (i.e. if it contains dots), each part is quoted separately.

    Examples:
    >>> quote("table")
    '`table`'
    >>> quote("`table`")
    '`table`'
    >>> quote("column.name")
    '`column`.`name`'
    >>> quote("*")
    '*'

    """
    return ".".join(["`" + s + "`" if s != "*" else "*" for s in string.replace("`", "").split(".")])


def quote_columns(columns: list[str]) -> list[str]:
    """Puts every column name of the given list into quotes."""
    return [quote(col) for col in columns]


def str_to_col(args: "ColumnOrName") -> "Column":
    """Converts string or Column argument to Column types

    Examples:
    >>> str_to_col("id")
    Column<'`id`'>
    >>> from bigquery_frame import functions as f
    >>> str_to_col(f.expr("COUNT(1)"))
    Column<'COUNT(1)'>
    >>> str_to_col("*")
    Column<'*'>

    """
    from bigquery_frame import functions as f

    if isinstance(args, str):
        return f.col(args)
    else:
        return args


def str_to_cols(args: Iterable["ColumnOrName"]) -> list["Column"]:
    """Converts string or Column arguments to Column types

    Examples:
        >>> str_to_cols(["c1", "c2"])
        [Column<'`c1`'>, Column<'`c2`'>]
        >>> from bigquery_frame import functions as f
        >>> str_to_col(f.expr("COUNT(1)"))
        Column<'COUNT(1)'>
        >>> str_to_col("*")
        Column<'*'>
    """
    return [str_to_col(arg) for arg in args]


def lit_to_col(args: "LitOrColumn") -> "Column":
    """Converts literal string or Column argument to Column type

    Examples:
        >>> lit_to_col("id")
        Column<'r\"\"\"id\"\"\"'>
        >>> from bigquery_frame import functions as f
        >>> lit_to_col(f.expr("COUNT(1)"))
        Column<'COUNT(1)'>
        >>> lit_to_col("*")
        Column<'r\"\"\"*\"\"\"'>
    """
    from bigquery_frame import Column
    from bigquery_frame import functions as f

    if isinstance(args, Column):
        return args
    else:
        return f.lit(args)


def lit_to_cols(args: Iterable["LitOrColumn"]) -> list["Column"]:
    """Converts literal string or Column argument to Column type

    Examples:
        >>> lit_to_cols(["id", "c"])
        [Column<'r\"\"\"id\"\"\"'>, Column<'r\"\"\"c\"\"\"'>]
        >>> from bigquery_frame import functions as f
        >>> lit_to_cols([f.expr("COUNT(1)"), "*"])
        [Column<'COUNT(1)'>, Column<'r\"\"\"*\"\"\"'>]
    """
    return [lit_to_col(arg) for arg in args]


def number_lines(string: str, starting_index: int = 1) -> str:
    """Given a multi-line string, return a new string where each line is prepended with its number

    Example:
    >>> print(number_lines('Hello\\nWorld!'))
    1: Hello
    2: World!
    """
    lines = string.split("\n")
    max_index = starting_index + len(lines) - 1
    nb_zeroes = int(math.log10(max_index)) + 1
    numbered_lines = [str(index + starting_index).zfill(nb_zeroes) + ": " + line for index, line in enumerate(lines)]
    return "\n".join(numbered_lines)


def assert_true(assertion: bool, error: Union[str, BaseException] = None) -> None:
    """Raise an Exception with the given error_message if the assertion passed is false.

    !!! tip
        This method is especially useful to get 100% coverage more easily, without having to write tests for every
        single assertion to cover the cases when they fail (which are generally just there to provide a more helpful
        error message to users when something that is not supposed to happen does happen)

    Args:
        assertion: The boolean result of an assertion
        error: An Exception or a message string (in which case an AssertError with this message will be raised)

    >>> assert_true(3==3, "3 <> 4")
    >>> assert_true(3==4, "3 <> 4")
    Traceback (most recent call last):
    ...
    AssertionError: 3 <> 4
    >>> assert_true(3==4, ValueError("3 <> 4"))
    Traceback (most recent call last):
    ...
    ValueError: 3 <> 4
    >>> assert_true(3==4)
    Traceback (most recent call last):
    ...
    AssertionError
    """
    if not assertion:
        if isinstance(error, BaseException):
            raise error
        elif isinstance(error, str):
            raise AssertionError(error)
        else:
            raise AssertionError


def list_or_tuple_to_list(*columns: Union[list[T], T]) -> list[T]:
    """Convert a list or a tuple to a list

    >>> list_or_tuple_to_list()
    []
    >>> list_or_tuple_to_list(1, 2)
    [1, 2]
    >>> list_or_tuple_to_list([1, 2])
    [1, 2]
    >>> list_or_tuple_to_list([1, 2], [4, 5])
    Traceback (most recent call last):
        ...
    TypeError: Wrong argument type: <class 'tuple'>
    """
    assert_true(isinstance(columns, (list, tuple)), TypeError(f"Wrong argument type: {type(columns)}"))
    if len(columns) == 0:
        return []
    if isinstance(columns[0], list):
        if len(columns) == 1:
            return columns[0]
        else:
            raise TypeError(f"Wrong argument type: {type(columns)}")
    else:
        return list(columns)


def _ref(_: object) -> None:
    """Dummy function used to prevent 'optimize import' from dropping the methods imported"""
