from typing import Optional

from bigquery_frame import DataFrame
from bigquery_frame.nested_impl.package import unnest_fields


def unnest_field(df: DataFrame, field_name: str, keep_columns: Optional[list[str]] = None) -> DataFrame:
    """Given a DataFrame, return a new DataFrame where the specified column has been recursively
    unnested (a.k.a. exploded).

    !!! warning "Limitation: BigQuery does not support dots and exclamation marks in column names"
        For this reason, dots are replaced with the string "__ARRAY__" and exclamation mark are replaced
        with "__STRUCT__". When manipulating column names, you can use the utils methods from the
        module `bigquery_frame.special_characters` to reverse the replacement.

    Args:
        df: A Spark DataFrame
        field_name: The name of a nested column to unnest
        keep_columns: List of column names to keep while unnesting

    Returns:
        A new DataFrame

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame import nested
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''
        ...     SELECT
        ...         1 as id,
        ...         [STRUCT([1, 2] as a), STRUCT([3, 4] as a)] as arr
        ... ''')
        >>> df.show(simplify_structs=True)
        +----+----------------------+
        | id |                  arr |
        +----+----------------------+
        |  1 | [{[1, 2]}, {[3, 4]}] |
        +----+----------------------+

        >>> nested.fields(df)
        ['id', 'arr!.a!']
        >>> nested.unnest_field(df, 'arr!').show(simplify_structs=True)
        +--------------+
        | arr__ARRAY__ |
        +--------------+
        |     {[1, 2]} |
        |     {[3, 4]} |
        +--------------+

        >>> nested.unnest_field(df, 'arr!.a!').show(simplify_structs=True)
        +----------------------------------+
        | arr__ARRAY____STRUCT__a__ARRAY__ |
        +----------------------------------+
        |                                1 |
        |                                2 |
        |                                3 |
        |                                4 |
        +----------------------------------+

        >>> nested.unnest_field(df, 'arr!.a!', keep_columns=["id"]).show(simplify_structs=True)
        +----+----------------------------------+
        | id | arr__ARRAY____STRUCT__a__ARRAY__ |
        +----+----------------------------------+
        |  1 |                                1 |
        |  1 |                                2 |
        |  1 |                                3 |
        |  1 |                                4 |
        +----+----------------------------------+

        >>> df = bq.sql('''
        ...     SELECT
        ...         1 as id,
        ...         [
        ...             STRUCT([STRUCT("a1" as a, "b1" as b), STRUCT("a2" as a, "b1" as b)] as s2),
        ...             STRUCT([STRUCT("a3" as a, "b3" as b)] as s2)
        ...         ] as s1
        ... ''')
        >>> df.show(simplify_structs=True)
        +----+----------------------------------------+
        | id |                                     s1 |
        +----+----------------------------------------+
        |  1 | [{[{a1, b1}, {a2, b1}]}, {[{a3, b3}]}] |
        +----+----------------------------------------+

        >>> nested.fields(df)
        ['id', 's1!.s2!.a', 's1!.s2!.b']
        >>> nested.unnest_field(df, 's1!.s2!').show(simplify_structs=True)
        +----------------------------------+
        | s1__ARRAY____STRUCT__s2__ARRAY__ |
        +----------------------------------+
        |                         {a1, b1} |
        |                         {a2, b1} |
        |                         {a3, b3} |
        +----------------------------------+
    """
    if keep_columns is None:
        keep_columns = []
    return next(iter(unnest_fields(df, field_name, keep_fields=keep_columns).values()))
