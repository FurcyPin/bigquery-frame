from typing import List

from google.cloud.bigquery import SchemaField

from bigquery_frame import DataFrame
from bigquery_frame.dataframe import is_nullable, is_repeated, is_struct


def flatten(df: DataFrame, struct_separator: str = "_") -> DataFrame:
    """Flattens all the struct columns of a DataFrame
    Nested fields names will be joined together using the specified separator

    Examples:
    >>> from bigquery_frame import BigQueryBuilder
    >>> from bigquery_frame.auth import get_bq_client
    >>> bq = BigQueryBuilder(get_bq_client())
    >>> df = bq.sql('''SELECT 1 as id, STRUCT(1 as a, STRUCT(1 as c, 1 as d) as b) as s''')
    >>> df.printSchema()
    root
     |-- id: INTEGER (NULLABLE)
     |-- s: RECORD (NULLABLE)
     |    |-- a: INTEGER (NULLABLE)
     |    |-- b: RECORD (NULLABLE)
     |    |    |-- c: INTEGER (NULLABLE)
     |    |    |-- d: INTEGER (NULLABLE)
    <BLANKLINE>
    >>> flatten(df).printSchema()
    root
     |-- id: INTEGER (NULLABLE)
     |-- s_a: INTEGER (NULLABLE)
     |-- s_b_c: INTEGER (NULLABLE)
     |-- s_b_d: INTEGER (NULLABLE)
    <BLANKLINE>
    >>> flatten(df, "__").printSchema()
    root
     |-- id: INTEGER (NULLABLE)
     |-- s__a: INTEGER (NULLABLE)
     |-- s__b__c: INTEGER (NULLABLE)
     |-- s__b__d: INTEGER (NULLABLE)
    <BLANKLINE>

    :param df: a DataFrame
    :param struct_separator: It might be useful to change the separator when some DataFrame's column names already
            contain dots
    :return: a flattened DataFrame
    """
    # The idea is to recursively write a "SELECT s.b.c as s_b_c" for each nested column.
    cols = []

    def expand_struct(struct: List[SchemaField], col_stack: List[str]):
        for field in struct:
            if is_struct(field) and not is_repeated(field):
                expand_struct(field.fields, col_stack + [field.name])
            else:
                column = ".".join(col_stack + [field.name]) + " as " + struct_separator.join(col_stack + [field.name])
                cols.append(column)

    expand_struct(df.schema, col_stack=[])
    return df.select(cols)


def flatten_schema(
    schema: List[SchemaField],
    explode: bool,
    struct_separator: str = ".",
    array_separator: str = "!",
) -> List[SchemaField]:
    """Transforms a BigQuery DataFrame schema into a new schema where all structs have been flattened.
    The field names are kept, with a '.' separator for struct fields.
    If `explode` option is set, arrays are exploded with a '!' separator.

    Example:
    >>> from bigquery_frame import BigQueryBuilder
    >>> from bigquery_frame.auth import get_bq_client
    >>> from bigquery_frame.dataframe import schema_to_simple_string
    >>> bq = BigQueryBuilder(get_bq_client())
    >>> df = bq.sql('SELECT 1 as id, STRUCT(1 as a, [STRUCT(2 as c, 3 as d)] as b, [4, 5] as e) as s')
    >>> schema_to_simple_string(df.schema)
    'id:INTEGER,s:STRUCT<a:INTEGER,b:ARRAY<STRUCT<c:INTEGER,d:INTEGER>>,e:ARRAY<INTEGER>>'
    >>> schema_to_simple_string(flatten_schema(df.schema, explode=True))
    'id:INTEGER,s.a:INTEGER,s.b!.c:INTEGER,s.b!.d:INTEGER,s.e!:INTEGER'
    >>> schema_to_simple_string(flatten_schema(df.schema, explode=False))
    'id:INTEGER,s.a:INTEGER,s.b:ARRAY<STRUCT<c:INTEGER,d:INTEGER>>,s.e:ARRAY<INTEGER>'

    :param schema: A BigQuery DataFrame's schema
    :param explode: If set, arrays are exploded and an `array_separator` is appended to their name.
    :param struct_separator: separator used to delimit structs
    :param array_separator: separator used to delimit arrays
    :return:
    """

    def flatten_schema_field(prefix: str, schema_field: SchemaField, nullable: bool) -> List[SchemaField]:
        if is_struct(schema_field) and is_repeated(schema_field) and explode:
            return flatten_struct_type(
                schema_field.fields,
                nullable or is_nullable(schema_field),
                prefix + array_separator + struct_separator,
            )
        elif is_struct(schema_field) and not is_repeated(schema_field):
            return flatten_struct_type(
                schema_field.fields,
                nullable or is_nullable(schema_field),
                prefix + struct_separator,
            )
        else:
            mode = "NULLABLE" if nullable or is_nullable(schema_field) else "REQUIRED"
            if is_repeated(schema_field):
                if explode:
                    prefix += array_separator
                else:
                    mode = "REPEATED"
            return [
                SchemaField(
                    prefix,
                    schema_field.field_type,
                    mode,
                    schema_field.description,
                    schema_field.fields,
                )
            ]

    def flatten_struct_type(schema: List[SchemaField], nullable: bool = False, prefix: str = "") -> List[SchemaField]:
        res = []
        for schema_field in schema:
            res += flatten_schema_field(prefix + schema_field.name, schema_field, nullable)
        return res

    return flatten_struct_type(schema)
