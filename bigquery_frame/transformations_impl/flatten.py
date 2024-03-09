from typing import List

from google.cloud.bigquery import SchemaField

from bigquery_frame import DataFrame
from bigquery_frame import functions as f
from bigquery_frame.dataframe import is_repeated, is_struct


def flatten(df: DataFrame, struct_separator: str = "_") -> DataFrame:
    """Flattens all the struct columns of a DataFrame
    Nested fields names will be joined together using the specified separator

    Args:
        df: a DataFrame.
        struct_separator: separator added between the name of the struct and the name of its fields.

    Returns:
        a Flattened DataFrame.

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> bq = BigQueryBuilder()
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
    """
    # The idea is to recursively write a "SELECT s.b.c as s_b_c" for each nested column.
    cols = []

    def expand_struct(struct: List[SchemaField], col_stack: List[str]):
        for field in struct:
            if is_struct(field) and not is_repeated(field):
                expand_struct(field.fields, col_stack + [field.name])
            else:
                col_expr = ".".join(col_stack + [field.name])
                col_alias = struct_separator.join(col_stack + [field.name])
                column = f.col(col_expr).alias(col_alias)
                cols.append(column)

    expand_struct(df.schema, col_stack=[])
    return df.select(cols)
