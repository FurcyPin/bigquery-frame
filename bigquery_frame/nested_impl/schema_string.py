
from google.cloud.bigquery import SchemaField

from bigquery_frame import DataFrame
from bigquery_frame.data_type_utils import flatten_schema
from bigquery_frame.dataframe import is_nullable


def _flat_schema_to_tree_string(schema: list[SchemaField]) -> str:
    """Generates a string representing a flat schema in tree format"""

    def str_gen_schema_field(schema_field: SchemaField, prefix: str) -> list[str]:
        res = [
            f"{prefix}{schema_field.name}: {schema_field.field_type} "
            f"(nullable = {str(is_nullable(schema_field)).lower()})",
        ]
        return res

    def str_gen_schema(schema: list[SchemaField], prefix: str) -> list[str]:
        return [string for schema_field in schema for string in str_gen_schema_field(schema_field, prefix)]

    res = ["root", *str_gen_schema(schema, " |-- ")]

    return "\n".join(res) + "\n"


def schema_string(df: DataFrame) -> str:
    """Write the DataFrame's flattened schema to a string.

    - Structs are flattened with a `.` after their name.
    - Arrays are flattened with a `!` character after their name.

    Args:
        df: A DataFrame

    Returns:
        a string representing the flattened schema

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
        >>> print(nested.schema_string(df))
        root
         |-- id: INTEGER (nullable = true)
         |-- s1!.a: INTEGER (nullable = true)
         |-- s1!.b!.c: INTEGER (nullable = true)
         |-- s1!.b!.d: INTEGER (nullable = true)
         |-- s1!.e!: INTEGER (nullable = false)
         |-- s2.f: INTEGER (nullable = true)
        <BLANKLINE>
    """
    flat_schema = flatten_schema(df.schema, explode=True)
    return _flat_schema_to_tree_string(flat_schema)
