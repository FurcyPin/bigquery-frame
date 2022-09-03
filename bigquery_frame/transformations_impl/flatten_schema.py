from typing import List

from google.cloud.bigquery import SchemaField

from bigquery_frame.conf import REPETITION_MARKER, STRUCT_SEPARATOR


def flatten_schema(
    schema: List[SchemaField],
    explode: bool,
    struct_separator: str = STRUCT_SEPARATOR,
    repetition_marker: str = REPETITION_MARKER,
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
    :param repetition_marker: separator used to delimit arrays
    :return:
    """
    from bigquery_frame.dataframe import is_nullable, is_repeated, is_struct

    def flatten_schema_field(prefix: str, schema_field: SchemaField, nullable: bool) -> List[SchemaField]:
        if is_struct(schema_field) and is_repeated(schema_field) and explode:
            return flatten_struct_type(
                schema_field.fields,
                nullable or is_nullable(schema_field),
                prefix + repetition_marker + struct_separator,
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
                    prefix += repetition_marker
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
