from bigquery_frame import DataFrame


def sort_columns(df: DataFrame) -> DataFrame:
    """Returns a new DataFrame where the order of columns has been sorted

    Examples:
    >>> from bigquery_frame import BigQueryBuilder
    >>> from bigquery_frame.auth import get_bq_client
    >>> bq = BigQueryBuilder(get_bq_client())
    >>> df = bq.sql('''SELECT 1 as b, 1 as a, 1 as c''')
    >>> df.printSchema()
    root
     |-- b: INTEGER (NULLABLE)
     |-- a: INTEGER (NULLABLE)
     |-- c: INTEGER (NULLABLE)
    <BLANKLINE>
    >>> sort_columns(df).printSchema()
    root
     |-- a: INTEGER (NULLABLE)
     |-- b: INTEGER (NULLABLE)
     |-- c: INTEGER (NULLABLE)
    <BLANKLINE>
    """
    return df.select(*sorted(df.columns))
