from typing import Optional

from google.cloud.bigquery import SchemaField

from bigquery_frame import BigQueryBuilder, Column, nested
from bigquery_frame.transformations import transform_all_fields
from bigquery_frame.utils import strip_margin

WEIRD_CHARS = "_:<ù%µ> &é-+'è_çà=#|"


def test_transform_all_fields_with_weird_column_names(bq: BigQueryBuilder):
    df = bq.sql(
        f"""SELECT
        "John" as `name`,
        [STRUCT(1 as `a{WEIRD_CHARS}`), STRUCT(2 as `a{WEIRD_CHARS}`)] as s1,
        [STRUCT([1, 2] as `a{WEIRD_CHARS}`), STRUCT([3, 4] as `a{WEIRD_CHARS}`)] as s2,
        [
            STRUCT([
                STRUCT(STRUCT(1 as `c{WEIRD_CHARS}`) as `b{WEIRD_CHARS}`),
                STRUCT(STRUCT(2 as `c{WEIRD_CHARS}`) as `b{WEIRD_CHARS}`)
            ] as `a{WEIRD_CHARS}`),
            STRUCT([
                STRUCT(STRUCT(3 as `c{WEIRD_CHARS}`) as `b{WEIRD_CHARS}`),
                STRUCT(STRUCT(4 as `c{WEIRD_CHARS}`) as `b{WEIRD_CHARS}`)
            ] as `a{WEIRD_CHARS}`)
        ] as s3
    """,
    )
    assert nested.schema_string(df) == strip_margin(
        f"""
        |root
        | |-- name: STRING (nullable = true)
        | |-- s1!.a{WEIRD_CHARS}: INTEGER (nullable = true)
        | |-- s2!.a{WEIRD_CHARS}!: INTEGER (nullable = false)
        | |-- s3!.a{WEIRD_CHARS}!.b{WEIRD_CHARS}.c{WEIRD_CHARS}: INTEGER (nullable = true)
        |""",
    )
    assert df.show_string(simplify_structs=True) == strip_margin(
        """
        |+------+------------+----------------------+--------------------------------------+
        || name |         s1 |                   s2 |                                   s3 |
        |+------+------------+----------------------+--------------------------------------+
        || John | [{1}, {2}] | [{[1, 2]}, {[3, 4]}] | [{[{{1}}, {{2}}]}, {[{{3}}, {{4}}]}] |
        |+------+------------+----------------------+--------------------------------------+""",
    )

    def cast_int_as_double(col: Column, schema_field: SchemaField) -> Optional[Column]:
        if schema_field.field_type == "INTEGER" and schema_field.mode != "REPEATED":
            return col.cast("FLOAT64")

    actual = transform_all_fields(df, cast_int_as_double)
    assert nested.schema_string(actual) == strip_margin(
        f"""
        |root
        | |-- name: STRING (nullable = true)
        | |-- s1!.a{WEIRD_CHARS}: FLOAT (nullable = true)
        | |-- s2!.a{WEIRD_CHARS}!: FLOAT (nullable = false)
        | |-- s3!.a{WEIRD_CHARS}!.b{WEIRD_CHARS}.c{WEIRD_CHARS}: FLOAT (nullable = true)
        |""",
    )
    assert actual.show_string(simplify_structs=True) == strip_margin(
        """
        |+------+----------------+------------------------------+----------------------------------------------+
        || name |             s1 |                           s2 |                                           s3 |
        |+------+----------------+------------------------------+----------------------------------------------+
        || John | [{1.0}, {2.0}] | [{[1.0, 2.0]}, {[3.0, 4.0]}] | [{[{{1.0}}, {{2.0}}]}, {[{{3.0}}, {{4.0}}]}] |
        |+------+----------------+------------------------------+----------------------------------------------+""",
    )
