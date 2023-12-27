from typing import Callable, Optional

from google.cloud.bigquery import SchemaField

from bigquery_frame import DataFrame
from bigquery_frame.column import Column
from bigquery_frame.nested_impl.package import build_transformation_from_schema


def transform_all_fields(
    df: DataFrame,
    transformation: Callable[[Column, SchemaField], Optional[Column]],
) -> DataFrame:
    """Apply a transformation to all nested fields of a DataFrame.

    !!! info
        This method is compatible with any schema. It recursively applies on structs and arrays
        and is compatible with field names containing special characters.

    !!! warning "BigQuery specificity"
        When applying a transformation on all columns of a given type, make sure to check
        that `schema_field.mode != "REPEATED"` otherwise the transformation will be applied on arrays
        containing this type too.
        *Explanation:* in BigQuery, columns of type `ARRAY<T>` are represented with a
        [SchemaField](https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.schema.SchemaField)
        with attributes `mode="REPEATED"` and `field_type="T"`.

    Args:
        df: A DataFrame
        transformation: Transformation to apply to all fields of the DataFrame. The transformation must take as input
            a Column expression and the DataType of the corresponding expression.

    Returns:
        A new DataFrame

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame import nested
        >>> bq = BigQueryBuilder()

        >>> df = bq.sql('''SELECT
        ...     "John" as name,
        ...     [1, 2, 3] as s1,
        ...     [STRUCT(1 as a), STRUCT(2 as a)] as s2,
        ...     [STRUCT([1, 2] as a), STRUCT([3, 4] as a)] as s3,
        ...     [
        ...         STRUCT([STRUCT(STRUCT(1 as c) as b), STRUCT(STRUCT(2 as c) as b)] as a),
        ...         STRUCT([STRUCT(STRUCT(3 as c) as b), STRUCT(STRUCT(4 as c) as b)] as a)
        ...     ] as s4
        ... ''')
        >>> nested.print_schema(df)
        root
         |-- name: STRING (nullable = true)
         |-- s1!: INTEGER (nullable = false)
         |-- s2!.a: INTEGER (nullable = true)
         |-- s3!.a!: INTEGER (nullable = false)
         |-- s4!.a!.b.c: INTEGER (nullable = true)
        <BLANKLINE>
        >>> df.show(simplify_structs=True)
        +------+-----------+------------+----------------------+--------------------------------------+
        | name |        s1 |         s2 |                   s3 |                                   s4 |
        +------+-----------+------------+----------------------+--------------------------------------+
        | John | [1, 2, 3] | [{1}, {2}] | [{[1, 2]}, {[3, 4]}] | [{[{{1}}, {{2}}]}, {[{{3}}, {{4}}]}] |
        +------+-----------+------------+----------------------+--------------------------------------+
        >>> from bigquery_frame.dataframe import is_repeated
        >>> def cast_int_as_double(col: Column, schema_field: SchemaField):
        ...     if schema_field.field_type == "INTEGER" and schema_field.mode != "REPEATED":
        ...         return col.cast("FLOAT64")
        >>> new_df = df.transform(transform_all_fields, cast_int_as_double)
        >>> nested.print_schema(new_df)
        root
         |-- name: STRING (nullable = true)
         |-- s1!: FLOAT (nullable = false)
         |-- s2!.a: FLOAT (nullable = true)
         |-- s3!.a!: FLOAT (nullable = false)
         |-- s4!.a!.b.c: FLOAT (nullable = true)
        <BLANKLINE>
        >>> new_df.show(simplify_structs=True)
        +------+-----------------+----------------+------------------------------+----------------------------------------------+
        | name |              s1 |             s2 |                           s3 |                                           s4 |
        +------+-----------------+----------------+------------------------------+----------------------------------------------+
        | John | [1.0, 2.0, 3.0] | [{1.0}, {2.0}] | [{[1.0, 2.0]}, {[3.0, 4.0]}] | [{[{{1.0}}, {{2.0}}]}, {[{{3.0}}, {{4.0}}]}] |
        +------+-----------------+----------------+------------------------------+----------------------------------------------+
    """  # noqa: E501
    root_transformation = build_transformation_from_schema(
        df.schema,
        column_transformation=transformation,
    )
    return df.select(*root_transformation(df))
