from bigquery_frame import BigQueryBuilder, nested
from bigquery_frame import functions as f
from bigquery_frame.nested_impl.schema_string import schema_string
from bigquery_frame.utils import strip_margin


def test_with_fields(bq: BigQueryBuilder):
    """GIVEN a DataFrame with nested fields
    WHEN we use with_fields to add a new field
    THEN the other fields should remain undisturbed
    """
    df = bq.sql(
        """SELECT
        1 as id,
        [STRUCT(2 as a, [STRUCT(3 as c, 4 as d)] as b, [5, 6] as e)] as s1,
        STRUCT(7 as f) as s2,
        [STRUCT([1, 2] as a), STRUCT([3, 4] as a)] as s3,
        [STRUCT([STRUCT(1 as e, 2 as f)] as a), STRUCT([STRUCT(3 as e, 4 as f)] as a)] as s4
    """,
    )
    assert schema_string(df) == strip_margin(
        """
        |root
        | |-- id: INTEGER (nullable = true)
        | |-- s1!.a: INTEGER (nullable = true)
        | |-- s1!.b!.c: INTEGER (nullable = true)
        | |-- s1!.b!.d: INTEGER (nullable = true)
        | |-- s1!.e!: INTEGER (nullable = false)
        | |-- s2.f: INTEGER (nullable = true)
        | |-- s3!.a!: INTEGER (nullable = false)
        | |-- s4!.a!.e: INTEGER (nullable = true)
        | |-- s4!.a!.f: INTEGER (nullable = true)
        |""",
    )
    new_df = df.transform(nested.with_fields, {"s5.g": f.col("s2.f").cast("FLOAT64")})
    assert schema_string(new_df) == strip_margin(
        """
        |root
        | |-- id: INTEGER (nullable = true)
        | |-- s1!.a: INTEGER (nullable = true)
        | |-- s1!.b!.c: INTEGER (nullable = true)
        | |-- s1!.b!.d: INTEGER (nullable = true)
        | |-- s1!.e!: INTEGER (nullable = false)
        | |-- s2.f: INTEGER (nullable = true)
        | |-- s3!.a!: INTEGER (nullable = false)
        | |-- s4!.a!.e: INTEGER (nullable = true)
        | |-- s4!.a!.f: INTEGER (nullable = true)
        | |-- s5.g: FLOAT (nullable = true)
        |""",
    )
