from typing import List

from bigquery_frame import DataFrame
from bigquery_frame.data_type_utils import flatten_schema


def fields(df: DataFrame, keep_non_leaf_fields: bool = False) -> List[str]:
    """Return the name of all the fields (including nested sub-fields) in the given DataFrame.

    - Structs are flattened with a `.` after their name.
    - Arrays are flattened with a `!` character after their name.

    Args:
        df: A DataFrame
        keep_non_leaf_fields: If set, the fields of type array or struct are also included in the result

    Returns:
        The list of all flattened field names in this DataFrame

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''SELECT
        ...     1 as id,
        ...     [STRUCT(2 as a, [STRUCT(3 as c, 4 as d)] as b, [5, 6] as e)] as s1,
        ...     STRUCT(7 as f) as s2,
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
        >>> for field in fields(df): print(field)
        id
        s1!.a
        s1!.b!.c
        s1!.b!.d
        s1!.e!
        s2.f
        >>> for field in fields(df, keep_non_leaf_fields = True): print(field)
        id
        s1
        s1!
        s1!.a
        s1!.b
        s1!.b!
        s1!.b!.c
        s1!.b!.d
        s1!.e
        s1!.e!
        s2
        s2.f
    """
    return [field.name for field in flatten_schema(df.schema, explode=True, keep_non_leaf_fields=keep_non_leaf_fields)]
