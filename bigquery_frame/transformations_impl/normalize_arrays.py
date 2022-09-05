from bigquery_frame import DataFrame
from bigquery_frame.conf import ELEMENT_COL_NAME, REPETITION_MARKER, STRUCT_SEPARATOR
from bigquery_frame.nested import resolve_nested_columns
from bigquery_frame.transformations_impl.flatten_schema import flatten_schema


def normalize_arrays(df: DataFrame) -> DataFrame:
    """Given a DataFrame, sort all columns of type arrays (even nested ones) in a canonical order,
    making them comparable

    >>> from bigquery_frame import BigQueryBuilder
    >>> from bigquery_frame.auth import get_bq_client
    >>> bq = BigQueryBuilder(get_bq_client())
    >>> df = bq.sql('SELECT [3, 2, 1] as a')
    >>> df.show()
    +-----------+
    |         a |
    +-----------+
    | [3, 2, 1] |
    +-----------+
    >>> normalize_arrays(df).show()
    +-----------+
    |         a |
    +-----------+
    | [1, 2, 3] |
    +-----------+

    >>> df = bq.sql('SELECT [STRUCT(2 as a, 1 as b), STRUCT(1 as a, 2 as b), STRUCT(1 as a, 1 as b)] as s')
    >>> df.show()
    +--------------------------------------------------------+
    |                                                      s |
    +--------------------------------------------------------+
    | [{'a': 2, 'b': 1}, {'a': 1, 'b': 2}, {'a': 1, 'b': 1}] |
    +--------------------------------------------------------+
    >>> normalize_arrays(df).show()
    +--------------------------------------------------------+
    |                                                      s |
    +--------------------------------------------------------+
    | [{'a': 1, 'b': 1}, {'a': 1, 'b': 2}, {'a': 2, 'b': 1}] |
    +--------------------------------------------------------+

    >>> df = bq.sql('''SELECT [
    ...         STRUCT([STRUCT(2 as a, 2 as b), STRUCT(2 as a, 1 as b)] as l2),
    ...         STRUCT([STRUCT(1 as a, 2 as b), STRUCT(1 as a, 1 as b)] as l2)
    ...     ] as l1
    ... ''')
    >>> df.show()
    +----------------------------------------------------------------------------------------------+
    |                                                                                           l1 |
    +----------------------------------------------------------------------------------------------+
    | [{'l2': [{'a': 2, 'b': 2}, {'a': 2, 'b': 1}]}, {'l2': [{'a': 1, 'b': 2}, {'a': 1, 'b': 1}]}] |
    +----------------------------------------------------------------------------------------------+
    >>> normalize_arrays(df).show()
    +----------------------------------------------------------------------------------------------+
    |                                                                                           l1 |
    +----------------------------------------------------------------------------------------------+
    | [{'l2': [{'a': 1, 'b': 1}, {'a': 1, 'b': 2}]}, {'l2': [{'a': 2, 'b': 1}, {'a': 2, 'b': 2}]}] |
    +----------------------------------------------------------------------------------------------+

    >>> df = bq.sql('''SELECT [
    ...         STRUCT(STRUCT(2 as a, 2 as b) as s),
    ...         STRUCT(STRUCT(1 as a, 2 as b) as s)
    ...     ] as l1
    ... ''')
    >>> df.show()
    +----------------------------------------------------+
    |                                                 l1 |
    +----------------------------------------------------+
    | [{'s': {'a': 2, 'b': 2}}, {'s': {'a': 1, 'b': 2}}] |
    +----------------------------------------------------+
    >>> normalize_arrays(df).show()
    +----------------------------------------------------+
    |                                                 l1 |
    +----------------------------------------------------+
    | [{'s': {'a': 1, 'b': 2}}, {'s': {'a': 2, 'b': 2}}] |
    +----------------------------------------------------+

    :return:
    """
    schema_flat = flatten_schema(
        df.schema, explode=True, struct_separator=STRUCT_SEPARATOR, repetition_marker=REPETITION_MARKER
    )

    def get_col_short_name(col: str):
        if col[-1] == REPETITION_MARKER:
            return ELEMENT_COL_NAME
        else:
            return col.split(REPETITION_MARKER + STRUCT_SEPARATOR)[-1]

    columns = {field.name: get_col_short_name(field.name) for field in schema_flat}
    return df.select(*resolve_nested_columns(columns, sort=True))
