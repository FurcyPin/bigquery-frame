from dataclasses import dataclass

DEFAULT_NB_DIFFED_ROWS = 10
DEFAULT_MAX_STRING_LENGTH = 30
DEFAULT_LEFT_DF_ALIAS = "left"
DEFAULT_RIGHT_DF_ALIAS = "right"


@dataclass
class DiffFormatOptions:
    nb_diffed_rows: int = DEFAULT_NB_DIFFED_ROWS
    max_string_length: int = DEFAULT_MAX_STRING_LENGTH
    left_df_alias: str = DEFAULT_LEFT_DF_ALIAS
    right_df_alias: str = DEFAULT_RIGHT_DF_ALIAS
