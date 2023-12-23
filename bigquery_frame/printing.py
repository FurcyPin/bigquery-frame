from typing import Any

from google.cloud.bigquery.table import RowIterator
from tabulate import tabulate


def _struct_to_string_without_field_names(s: Any) -> str:
    """Transform an object into a string, but do not display the field names of dicts.

    Args:
        s: The object to transform into a string

    Returns:
        A string

    Examples:
        >>> _struct_to_string_without_field_names({"a": 1, "b": 2})
        '{1, 2}'
        >>> _struct_to_string_without_field_names({"a": [{"s": {"b": 1, "c": 2}}]})
        '{[{{1, 2}}]}'
    """
    if isinstance(s, list):
        return "[" + ", ".join(_struct_to_string_without_field_names(item) for item in s) + "]"
    elif isinstance(s, dict):
        return "{" + ", ".join(_struct_to_string_without_field_names(item) for item in s.values()) + "}"
    else:
        return str(s)


def tabulate_results(it: RowIterator, format_args: dict = None, limit=None, simplify_structs=False) -> str:
    if format_args is None:
        format_args = {
            "tablefmt": "pretty",
            "missingval": "null",
            "stralign": "right",
        }
    headers = {field.name: field.name for field in it.schema}
    rows = list(it)
    nb_rows = len(rows)
    rows = rows[0:limit]
    if simplify_structs:
        rows = [[_struct_to_string_without_field_names(field) for field in row] for row in rows]
    res = tabulate(rows, headers=headers, **format_args)
    if nb_rows > limit:
        plural = "s" if limit > 1 else ""
        res += f"\nonly showing top {limit} row{plural}"
    return res
