import re
from typing import Optional


def strip_margin(text):
    s = re.sub('\n[ \t]*\|', '\n', text)
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
    return '.'.join(['`' + s + '`' if s != '*' else '*' for s in str.replace('`', '').split('.')])


def cols_to_str(cols, indentation: Optional[int] = None, sep: str = ",") -> str:
    cols = [str(col) for col in cols]
    if indentation is not None:
        return indent(f"{sep}\n".join(cols), indentation)
    else:
        return ", ".join(cols)
