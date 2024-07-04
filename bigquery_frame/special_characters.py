from bigquery_frame import Column
from bigquery_frame import functions as f
from bigquery_frame.conf import (
    REPETITION_MARKER,
    REPETITION_MARKER_REPLACEMENT,
    STRUCT_SEPARATOR,
    STRUCT_SEPARATOR_REPLACEMENT,
)

_replacement_mapping = {
    STRUCT_SEPARATOR: STRUCT_SEPARATOR_REPLACEMENT,
    REPETITION_MARKER: REPETITION_MARKER_REPLACEMENT,
}
_replacements = str.maketrans(_replacement_mapping)


def _replace_special_characters_except_last_granularity(col_name: str) -> str:
    """Replace special characters except for the ones at the last granularity

    >>> _replace_special_characters_except_last_granularity("a.b.c")
    'a.b.c'
    >>> _replace_special_characters_except_last_granularity("a!.b!.c.d")
    'a__ARRAY____STRUCT__b__ARRAY__.c.d'

    """
    index = col_name.rfind(REPETITION_MARKER)
    if index == -1:
        return col_name
    else:
        return _replace_special_characters(col_name[: index + 1]) + col_name[index + 1 :]


def _replace_special_characters(col_name: str) -> str:
    """Replace special characters

    >>> _replace_special_characters("a.b!.c")
    'a__STRUCT__b__ARRAY____STRUCT__c'
    """
    return col_name.translate(_replacements)


def _restore_special_characters(col_name: str) -> str:
    """Restore special characters

    >>> _restore_special_characters("a__STRUCT__b__ARRAY____STRUCT__c")
    'a.b!.c'
    """
    result = col_name
    for value, replacement in _replacement_mapping.items():
        result = result.replace(replacement, value)
    return result


def _restore_special_characters_from_col(col: Column) -> Column:
    return f.regexp_replace(
        f.regexp_replace(col, STRUCT_SEPARATOR_REPLACEMENT, STRUCT_SEPARATOR),
        REPETITION_MARKER_REPLACEMENT,
        REPETITION_MARKER,
    )
