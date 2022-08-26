from google.cloud.bigquery import SchemaField

from bigquery_frame import functions as f
from bigquery_frame.column import Column


def column_number(col: str, schema_field: SchemaField, col_num: int) -> Column:  # NOSONAR
    return f.lit(col_num).alias("column_number")


def column_name(col: str, schema_field: SchemaField, col_num: int) -> Column:  # NOSONAR
    return f.lit(schema_field.name).alias("column_name")


def column_type(col: str, schema_field: SchemaField, col_num: int) -> Column:  # NOSONAR
    return f.lit(schema_field.field_type).alias("column_type")


def count(col: str, schema_field: SchemaField, col_num: int) -> Column:  # NOSONAR
    return f.count(f.lit(1)).alias("count")


def count_distinct(col: str, schema_field: SchemaField, col_num: int) -> Column:  # NOSONAR
    return f.count_distinct(col).alias("count_distinct")


def count_null(col: str, schema_field: SchemaField, col_num: int) -> Column:  # NOSONAR
    return (f.count(f.lit(1)) - f.count(col)).alias("count_null")


def min(col: str, schema_field: SchemaField, col_num: int) -> Column:  # NOSONAR
    return f.min(col).cast("STRING").alias("min")


def max(col: str, schema_field: SchemaField, col_num: int) -> Column:  # NOSONAR
    return f.max(col).cast("STRING").alias("max")


def approx_top_100(col: str, schema_field: SchemaField, col_num: int) -> Column:  # NOSONAR
    col = f.coalesce(f.col(col).asType("string"), f.lit("NULL"))
    return f.expr(f"APPROX_TOP_COUNT({col.expr}, 100)").alias("approx_top_100")
