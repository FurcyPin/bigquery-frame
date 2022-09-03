import math
import re
from typing import TYPE_CHECKING, Iterable, List, Union

if TYPE_CHECKING:
    from bigquery_frame.column import Column, LitOrColumn, StringOrColumn


def strip_margin(text):
    s = re.sub(r"\n[ \t]*\|", "\n", text)
    if s.startswith("\n"):
        return s[1:]
    else:
        return s


def indent(str, nb) -> str:
    return " " * nb + str.replace("\n", "\n" + " " * nb)


def quote(str) -> str:
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
    return ".".join(["`" + s + "`" if s != "*" else "*" for s in str.replace("`", "").split(".")])


def quote_columns(columns: List[str]) -> List[str]:
    """Puts every column name of the given list into quotes."""
    return [quote(col) for col in columns]


def str_to_col(args: Union[List["StringOrColumn"], "StringOrColumn"]) -> Union[List["Column"], "Column"]:
    """Converts string or Column arguments to Column types

    Examples:

    >>> str_to_col("id")
    Column('`id`')
    >>> str_to_col(["c1", "c2"])
    [Column('`c1`'), Column('`c2`')]
    >>> from bigquery_frame import functions as f
    >>> str_to_col(f.expr("COUNT(1)"))
    Column('COUNT(1)')
    >>> str_to_col("*")
    Column('*')

    """
    from bigquery_frame import functions as f

    if isinstance(args, str):
        return f.col(args)
    elif isinstance(args, Iterable):
        return [str_to_col(arg) for arg in args]
    else:
        return args


def lit_to_col(args: Union[Iterable["LitOrColumn"], "LitOrColumn"]) -> Union[List["Column"], "Column"]:
    """Converts literal string or Column arguments to Column types

    Examples:

    >>> lit_to_col("id")
    Column(''id'')
    >>> lit_to_col(["c1", "c2"])
    [Column(''c1''), Column(''c2'')]
    >>> from bigquery_frame import functions as f
    >>> lit_to_col(f.expr("COUNT(1)"))
    Column('COUNT(1)')
    >>> lit_to_col("*")
    Column(''*'')

    """
    from bigquery_frame import functions as f
    from bigquery_frame.column import Column

    if isinstance(args, List):
        return [lit_to_col(arg) for arg in args]
    elif not isinstance(args, Column):
        return f.lit(args)
    else:
        return args


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


def assert_true(assertion: bool, error_message: str = None) -> None:
    """Raise a ValueError with the given error_message if the assertion passed is false

    >>> assert_true(3==4, "3 <> 4")
    Traceback (most recent call last):
    ...
    ValueError: 3 <> 4

    >>> assert_true(3==3, "3 <> 4")

    :param assertion: assertion that will be checked
    :param error_message: error message to display if the assertion is false
    """
    if not assertion:
        if error_message is None:
            raise ValueError()
        else:
            raise ValueError(error_message)
