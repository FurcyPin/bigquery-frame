DEFAULT_ALIAS_NAME = "_default_alias_{num}"
DEFAULT_TEMP_COLUMN_NAME = "_default_temp_column_{num}"
DEFAULT_TEMP_TABLE_NAME = "_default_temp_table_{num}"


_alias_count = 0
_temp_column_count = 0
_temp_table_count = 0


def _get_alias() -> str:
    global _alias_count
    _alias_count += 1
    return "{" + DEFAULT_ALIAS_NAME.format(num=_alias_count) + "}"


def _get_temp_column_name() -> str:
    global _temp_column_count
    _temp_column_count += 1
    return DEFAULT_TEMP_COLUMN_NAME.format(num=_temp_column_count)


def _get_temp_table_name() -> str:
    global _temp_table_count
    _temp_table_count += 1
    return DEFAULT_TEMP_TABLE_NAME.format(num=_temp_table_count)
