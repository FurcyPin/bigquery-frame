from bigquery_frame import DataFrame
from bigquery_frame.nested_impl.schema_string import schema_string


def print_schema(df: DataFrame) -> None:
    """Print the DataFrame's flattened schema to the standard output.

    - Structs are flattened with a `.` after their name.
    - Arrays are flattened with a `!` character after their name.

    Args:
        df: A DataFrame

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame import nested
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''SELECT
        ...     1 as id,
        ...     [STRUCT(2 as a, [STRUCT(3 as c, 4 as d)] as b, [5, 6] as e)] as s1,
        ...     STRUCT(7 as f) as s2
        ... ''')
        >>> df.printSchema()
        root
         |-- id: INTEGER (NULLABLE)
         |-- s1: RECORD (REPEATED)
         |    |-- a: INTEGER (NULLABLE)
         |    |-- b: RECORD (REPEATED)
         |    |    |-- c: INTEGER (NULLABLE)
         |    |    |-- d: INTEGER (NULLABLE)
         |    |-- e: INTEGER (REPEATED)
         |-- s2: RECORD (NULLABLE)
         |    |-- f: INTEGER (NULLABLE)
        <BLANKLINE>
        >>> nested.print_schema(df)
        root
         |-- id: INTEGER (nullable = true)
         |-- s1!.a: INTEGER (nullable = true)
         |-- s1!.b!.c: INTEGER (nullable = true)
         |-- s1!.b!.d: INTEGER (nullable = true)
         |-- s1!.e!: INTEGER (nullable = false)
         |-- s2.f: INTEGER (nullable = true)
        <BLANKLINE>
    """
    print(schema_string(df))
