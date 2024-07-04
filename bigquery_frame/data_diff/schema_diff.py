import difflib
from dataclasses import dataclass
from enum import Enum

from google.cloud.bigquery import SchemaField

from bigquery_frame import DataFrame
from bigquery_frame.conf import REPETITION_MARKER
from bigquery_frame.data_type_utils import flatten_schema
from bigquery_frame.dataframe import is_nullable, is_repeated, is_struct
from bigquery_frame.field_utils import has_same_granularity_as_any, is_parent_field_of_any


class DiffPrefix(str, Enum):
    ADDED = "+"
    REMOVED = "-"
    UNCHANGED = " "

    def __repr__(self) -> str:
        return f"'{self.value}'"


@dataclass
class SchemaDiffResult:
    same_schema: bool
    diff_str: str
    nb_cols: int
    column_names_diff: dict[str, DiffPrefix]
    """The diff per column names.
    Used to determine which columns appeared or disappeared and the order in which the columns shall be displayed"""

    def display(self) -> None:
        if not self.same_schema:
            print(f"Schema has changed:\n{self.diff_str}")
            print("WARNING: columns that do not match both sides will be ignored")
        else:
            print(f"Schema: ok ({self.nb_cols})")

    @property
    def column_names(self) -> list[str]:
        return list(self.column_names_diff.keys())


def _schema_to_string(
    schema: list[SchemaField], include_nullable: bool = False, include_description: bool = False,
) -> list[str]:
    """Return a list of strings representing the schema

    Args:
        schema: A DataFrame's schema
        include_nullable: Indicate for each field if it is nullable
        include_description: Add field description

    Returns:

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''SELECT 1 as id, "a" as c1, 1 as c2''')
        >>> print('\\n'.join(_schema_to_string(df.schema)))
        id INTEGER
        c1 STRING
        c2 INTEGER
        >>> print('\\n'.join(_schema_to_string(df.schema, include_nullable=True)))
        id INTEGER (nullable)
        c1 STRING (nullable)
        c2 INTEGER (nullable)
        >>> schema = [
        ...     SchemaField(name='id', field_type='INTEGER', mode='NULLABLE', description='An id'),
        ...     SchemaField(name='c1', field_type='STRING', mode='REQUIRED', description='A string column'),
        ...     SchemaField(name='c2', field_type='INTEGER', mode='NULLABLE', description='An int column')
        ... ]
        >>> print('\\n'.join(_schema_to_string(schema, include_nullable=True, include_description=True)))
        id INTEGER (nullable) An id
        c1 STRING (required) A string column
        c2 INTEGER (nullable) An int column
        >>> df = bq.sql('''SELECT 1 as id, STRUCT(2 as a, [STRUCT(3 as c, 4 as d, [5] as e)] as b) as s''')
        >>> print('\\n'.join(_schema_to_string(df.schema)))
        id INTEGER
        s STRUCT<a:INTEGER,b:ARRAY<STRUCT<c:INTEGER,d:INTEGER,e:ARRAY<INTEGER>>>>
    """

    def field_to_string(field: SchemaField, sep=":") -> str:
        if is_struct(field):
            tpe = f"""STRUCT<{",".join(field_to_string(f) for f in field.fields)}>"""
            if is_repeated(field):
                tpe = f"""ARRAY<{tpe}>"""
        elif is_repeated(field):
            tpe = f"ARRAY<{field.field_type}>"
        else:
            tpe = f"{field.field_type}"
        return f"""{field.name}{sep}{tpe}"""

    def meta_str(field) -> str:
        s = ""
        if include_nullable:
            if is_nullable(field):
                s += " (nullable)"
            else:
                s += " (required)"
        if include_description:
            s += f" {field.description}"
        return s

    return [field_to_string(field, sep=" ") + meta_str(field) for field in schema]


def diff_dataframe_schemas(left_df: DataFrame, right_df: DataFrame, join_cols: list[str]) -> SchemaDiffResult:
    """Compares two DataFrames schemas and print out the differences.
    Ignore the nullable and comment attributes.

    Args:
        left_df: A DataFrame
        right_df: Another DataFrame
        join_cols: The list of column names that will be used for joining the two DataFrames together

    Returns:
        A SchemaDiffResult object

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> left_df = bq.sql('''SELECT 1 as id, "" as c1, "" as c2, [STRUCT(2 as a, "" as b)] as c4''')
        >>> right_df = bq.sql('''SELECT 1 as id, 2 as c1, "" as c3, [STRUCT(3 as a, "" as d)] as c4''')
        >>> schema_diff_result = diff_dataframe_schemas(left_df, right_df, ["id"])
        >>> schema_diff_result.display()
        Schema has changed:
        @@ -1,4 +1,4 @@
        <BLANKLINE>
         id INTEGER
        -c1 STRING
        -c2 STRING
        -c4 ARRAY<STRUCT<a:INTEGER,b:STRING>>
        +c1 INTEGER
        +c3 STRING
        +c4 ARRAY<STRUCT<a:INTEGER,d:STRING>>
        WARNING: columns that do not match both sides will be ignored
        >>> schema_diff_result.same_schema
        False
        >>> schema_diff_result.column_names_diff
        {'id': ' ', 'c1': ' ', 'c2': '-', 'c3': '+', 'c4': ' '}

        >>> schema_diff_result = diff_dataframe_schemas(left_df, right_df, ["id", "c4!.a"])
        >>> schema_diff_result.display()
        Schema has changed:
        @@ -1,5 +1,5 @@
        <BLANKLINE>
         id INTEGER
        -c1 STRING
        -c2 STRING
        +c1 INTEGER
        +c3 STRING
         c4!.a INTEGER
        -c4!.b STRING
        +c4!.d STRING
        WARNING: columns that do not match both sides will be ignored
        >>> schema_diff_result.same_schema
        False
        >>> schema_diff_result.column_names_diff
        {'id': ' ', 'c1': ' ', 'c2': '-', 'c3': '+', 'c4!.a': ' ', 'c4!.b': '-', 'c4!.d': '+'}
    """

    def explode_schema_according_to_join_cols(schema: list[SchemaField]) -> list[SchemaField]:
        exploded_schema = flatten_schema(schema, explode=True, keep_non_leaf_fields=True)
        return [
            field
            for field in exploded_schema
            if has_same_granularity_as_any(field.name, join_cols)
            and not is_parent_field_of_any(field.name, join_cols)
            and not (is_struct(field) and not is_repeated(field))
            and not field.name.endswith(REPETITION_MARKER)
        ]

    left_schema_flat_exploded = explode_schema_according_to_join_cols(left_df.schema)
    right_schema_flat_exploded = explode_schema_according_to_join_cols(right_df.schema)

    left_schema: list[str] = _schema_to_string(left_schema_flat_exploded)
    right_schema: list[str] = _schema_to_string(right_schema_flat_exploded)
    left_columns_flat: list[str] = [field.name for field in left_schema_flat_exploded]
    right_columns_flat: list[str] = [field.name for field in right_schema_flat_exploded]

    diff_str = list(difflib.unified_diff(left_schema, right_schema, n=10000))[2:]
    column_names_diff = _diff_dataframe_column_names(left_columns_flat, right_columns_flat)
    same_schema = len(diff_str) == 0
    if same_schema:
        diff_str = left_schema
    return SchemaDiffResult(
        same_schema=same_schema,
        diff_str="\n".join(diff_str),
        nb_cols=len(left_df.columns),
        column_names_diff=column_names_diff,
    )


def _remove_potential_duplicates_from_diff(diff: list[str]) -> list[str]:
    """In some cases (e.g. swapping the order of two columns), the difflib.unified_diff produces results
    where a column is added and then removed. This method replaces such duplicates with a single occurrence
    of the column marked as unchanged. We keep the column ordering of the left side.

    Examples:
        >>> _remove_potential_duplicates_from_diff([' id', ' col1', '+col4', '+col3', ' col2', '-col3', '-col4'])
        [' id', ' col1', ' col2', ' col3', ' col4']

    """
    plus = {row[1:] for row in diff if row[0] == DiffPrefix.ADDED}
    minus = {row[1:] for row in diff if row[0] == DiffPrefix.REMOVED}
    both = plus.intersection(minus)
    return [
        DiffPrefix.UNCHANGED + row[1:] if row[1:] in both else row
        for row in diff
        if (row[1:] not in both) or (row[0] == DiffPrefix.REMOVED)
    ]


def _diff_dataframe_column_names(left_col_names: list[str], right_col_names: list[str]) -> dict[str, DiffPrefix]:
    """Compares the column names of two DataFrames.

    Returns a list of column names that preserves the ordering of the left DataFrame when possible.
    The columns names are prefixed by a character according to the following convention:

    - ' ' if the column exists in both DataFrame
    - '-' if it only exists in the left DataFrame
    - '+' if it only exists in the right DataFrame

    Args:
        left_col_names: A list
        right_col_names: Another DataFrame

    Returns:
        A list of column names prefixed with a character: ' ', '+' or '-'

    Examples:
        >>> left_cols = ["id", "col1", "col2", "col3"]
        >>> right_cols = ["id", "col1", "col4", "col3"]
        >>> _diff_dataframe_column_names(left_cols, right_cols)
        {'id': ' ', 'col1': ' ', 'col2': '-', 'col4': '+', 'col3': ' '}
        >>> _diff_dataframe_column_names(left_cols, left_cols)
        {'id': ' ', 'col1': ' ', 'col2': ' ', 'col3': ' '}

        >>> left_cols = ["id", "col1", "col2", "col3", "col4"]
        >>> right_cols = ["id", "col1", "col4", "col3", "col2"]
        >>> _diff_dataframe_column_names(left_cols, right_cols)
        {'id': ' ', 'col1': ' ', 'col2': ' ', 'col3': ' ', 'col4': ' '}

    """
    diff = list(difflib.unified_diff(left_col_names, right_col_names, n=10000))[2:]
    same_schema = len(diff) == 0
    if same_schema:
        list_result = [DiffPrefix.UNCHANGED + s for s in left_col_names]
    else:
        list_result = _remove_potential_duplicates_from_diff(diff[1:])
    return {s[1:]: DiffPrefix(s[0]) for s in list_result}
