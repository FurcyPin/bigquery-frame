from typing import Optional

from google.cloud.bigquery import SchemaField

from bigquery_frame import Column, DataFrame
from bigquery_frame import functions as f
from bigquery_frame.dataframe import is_repeated, is_struct
from bigquery_frame.transformations_impl.transform_all_fields import transform_all_fields


def sort_all_arrays(df: DataFrame) -> DataFrame:
    """Given a DataFrame, sort all fields of type `ARRAY` in a canonical order, making them comparable.
    This also applies to nested fields, even those inside other arrays.

    Args:
        df: A DataFrame

    Returns:
        A new DataFrame where all arrays have been sorted.

    Examples: Example 1: with a simple `ARRAY<INT>`
        >>> from bigquery_frame import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('SELECT 1 as id, [3, 2, 1] as a')
        >>> df.show()
        +----+-----------+
        | id |         a |
        +----+-----------+
        |  1 | [3, 2, 1] |
        +----+-----------+

        >>> sort_all_arrays(df).show()
        +----+-----------+
        | id |         a |
        +----+-----------+
        |  1 | [1, 2, 3] |
        +----+-----------+

    Examples: Example 2: with an `ARRAY<STRUCT<...>>`
        >>> df = bq.sql('SELECT [STRUCT(2 as a, 1 as b), STRUCT(1 as a, 2 as b), STRUCT(1 as a, 1 as b)] as s')
        >>> df.show(simplify_structs=True)
        +--------------------------+
        |                        s |
        +--------------------------+
        | [{2, 1}, {1, 2}, {1, 1}] |
        +--------------------------+

        >>> df.transform(sort_all_arrays).show(simplify_structs=True)
        +--------------------------+
        |                        s |
        +--------------------------+
        | [{1, 1}, {1, 2}, {2, 1}] |
        +--------------------------+

    Examples: Example 3: with an `ARRAY<STRUCT<STRUCT<...>>>`
        >>> df = bq.sql('''SELECT [
        ...         STRUCT(STRUCT(2 as a, 2 as b) as s),
        ...         STRUCT(STRUCT(1 as a, 2 as b) as s)
        ...     ] as l1
        ... ''')
        >>> df.show(simplify_structs=True)
        +----------------------+
        |                   l1 |
        +----------------------+
        | [{{2, 2}}, {{1, 2}}] |
        +----------------------+

        >>> df.transform(sort_all_arrays).show(simplify_structs=True)
        +----------------------+
        |                   l1 |
        +----------------------+
        | [{{1, 2}}, {{2, 2}}] |
        +----------------------+

    Examples: Example 4: with an `ARRAY<ARRAY<ARRAY<INT>>>`
        As this example shows, the innermost arrays are sorted before the outermost arrays.

        >>> df = bq.sql('''SELECT [
        ...         STRUCT([STRUCT([4, 1] as b), STRUCT([3, 2] as b)] as a),
        ...         STRUCT([STRUCT([2, 2] as b), STRUCT([2, 1] as b)] as a)
        ...     ] as l1
        ... ''')
        >>> df.show(simplify_structs=True)
        +--------------------------------------------------+
        |                                               l1 |
        +--------------------------------------------------+
        | [{[{[4, 1]}, {[3, 2]}]}, {[{[2, 2]}, {[2, 1]}]}] |
        +--------------------------------------------------+

        >>> df.transform(sort_all_arrays).show(simplify_structs=True)
        +--------------------------------------------------+
        |                                               l1 |
        +--------------------------------------------------+
        | [{[{[1, 2]}, {[2, 2]}]}, {[{[1, 4]}, {[2, 3]}]}] |
        +--------------------------------------------------+
    """

    def sort_array(col: Column, field: SchemaField) -> Optional[Column]:
        def json_if_not_sortable(col: Column, _field: SchemaField) -> Column:
            if is_struct(_field) or is_repeated(_field):
                return f.expr(f"TO_JSON_STRING({col.expr})")
            else:
                return col

        if is_repeated(field):
            if is_struct(field):
                return f.sort_array(
                    col, lambda c: [json_if_not_sortable(c[_field.name], _field) for _field in field.fields]
                )
            else:
                return f.sort_array(col)
        else:
            return None

    return transform_all_fields(df, sort_array)
