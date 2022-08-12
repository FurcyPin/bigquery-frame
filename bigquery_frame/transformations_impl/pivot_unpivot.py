from typing import List

from bigquery_frame import DataFrame
from bigquery_frame.auth import get_bq_client
from bigquery_frame.column import cols_to_str
from bigquery_frame.dataframe import BigQueryBuilder, strip_margin
from bigquery_frame.utils import quote


def pivot(
    df: DataFrame,
    pivot_column: str,
    agg_fun: str,
    agg_col: str,
    pivoted_columns: List[str] = None,
    implem_version: int = 1,
) -> DataFrame:
    """Pivots a column of the current :class:`DataFrame` and performs the specified aggregation.
    There are two versions of pivot function: one that requires the caller to specify the list
    of distinct values to pivot on, and one that does not. The latter is more concise but less
    efficient, because we need to first compute the list of distinct values internally.

    Example:
    >>> df = __get_test_unpivoted_df()
    >>> df.show()
    +------+---------+---------+--------+
    | year | product | country | amount |
    +------+---------+---------+--------+
    | 2018 |  Orange |  Canada |   null |
    | 2018 |  Orange |   China |   4000 |
    | 2018 |  Orange |  Mexico |   null |
    | 2018 |   Beans |  Canada |   null |
    | 2018 |   Beans |   China |   1500 |
    | 2018 |   Beans |  Mexico |   2000 |
    | 2018 |  Banana |  Canada |   2000 |
    | 2018 |  Banana |   China |    400 |
    | 2018 |  Banana |  Mexico |   null |
    | 2018 | Carrots |  Canada |   2000 |
    | 2018 | Carrots |   China |   1200 |
    | 2018 | Carrots |  Mexico |   null |
    | 2019 |  Orange |  Canada |   5000 |
    | 2019 |  Orange |   China |   null |
    | 2019 |  Orange |  Mexico |   5000 |
    | 2019 |   Beans |  Canada |   null |
    | 2019 |   Beans |   China |   1500 |
    | 2019 |   Beans |  Mexico |   2000 |
    | 2019 |  Banana |  Canada |   null |
    | 2019 |  Banana |   China |   1400 |
    +------+---------+---------+--------+
    only showing top 20 rows
    >>> pivot(df, pivot_column="country", agg_fun="sum", agg_col="amount").show()
    +------+---------+--------+-------+--------+
    | year | product | Canada | China | Mexico |
    +------+---------+--------+-------+--------+
    | 2018 |  Orange |   null |  4000 |   null |
    | 2018 |   Beans |   null |  1500 |   2000 |
    | 2018 |  Banana |   2000 |   400 |   null |
    | 2018 | Carrots |   2000 |  1200 |   null |
    | 2019 |  Orange |   5000 |  null |   5000 |
    | 2019 |   Beans |   null |  1500 |   2000 |
    | 2019 |  Banana |   null |  1400 |    400 |
    | 2019 | Carrots |   null |   200 |   null |
    +------+---------+--------+-------+--------+

    :param df: a DataFrame
    :param pivot_column: column to pivot
    :param agg_fun: aggregation function that will be applied
    :param agg_col: column that will be aggregated
    :param pivoted_columns: (Optional) list of distinct values in the pivot column.
        Execution will be faster if provided.
    :param implem_version: (Possible values [1, 2]) Version of the code to use.
        Version 2 uses the BigQuery's PIVOT statement, while version 1 doesn't.
    :return:
    """
    if implem_version == 1:
        return pivot_v1(df, pivot_column, agg_fun, agg_col, pivoted_columns)
    elif implem_version == 2:
        return pivot_v2(df, pivot_column, agg_fun, agg_col, pivoted_columns)
    else:
        raise Exception("Pivot, please use v1 or v2")


def pivot_v1(
    df: DataFrame,
    pivot_column: str,
    agg_fun: str,
    agg_col: str,
    pivoted_columns: List[str] = None,
) -> DataFrame:
    """This version uses a good old GROUP BY statement and should be compatible with most ANSI-SQL engines."""
    group_columns = [col for col in df.columns if col.lower() not in [agg_col.lower(), pivot_column.lower()]]
    distinct_query = f"""SELECT DISTINCT {pivot_column} FROM {quote(df._alias)}"""
    if pivoted_columns is None:
        pivoted_columns = [row.get(pivot_column) for row in df._apply_query(distinct_query).collect()]

    aggregates = [f"""{agg_fun}(IF({pivot_column} = '{col}', {agg_col}, null)) as {col}""" for col in pivoted_columns]

    group_query = strip_margin(
        f"""
        |SELECT
        |{cols_to_str(group_columns, 2)},
        |{cols_to_str(aggregates, 2)}
        |FROM {quote(df._alias)}
        |GROUP BY {cols_to_str(group_columns)}
        |"""
    )
    return df._apply_query(group_query)


def pivot_v2(
    df: DataFrame,
    pivot_column: str,
    agg_fun: str,
    agg_col: str,
    pivoted_columns: List[str] = None,
) -> DataFrame:
    """This version uses BigQuery's
    `PIVOT operator <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#pivot_operator>`_
    """
    distinct_query = f"""SELECT DISTINCT {pivot_column} FROM {quote(df._alias)}"""
    if pivoted_columns is None:
        pivoted_columns = [row.get(pivot_column) for row in df._apply_query(distinct_query).collect()]
    quoted_pivoted_columns = [f"'{col}'" for col in pivoted_columns]

    group_query = strip_margin(
        f"""
        |SELECT
        | *
        |FROM {quote(df._alias)}
        |PIVOT({agg_fun}({agg_col}) FOR {pivot_column} IN ({cols_to_str(quoted_pivoted_columns)}))
        |"""
    )
    return df._apply_query(group_query)


def unpivot(
    df: DataFrame,
    pivot_columns: List[str],
    key_alias: str = "key",
    value_alias: str = "value",
    exclude_nulls: bool = False,
    implem_version: int = 1,
) -> DataFrame:
    """Unpivot the given DataFrame along the specified pivot columns.
    All columns that are not pivot columns should have the same type.

    Example:
    >>> df = __get_test_pivoted_df()
    >>> df.show()
    +------+---------+--------+-------+--------+
    | year | product | Canada | China | Mexico |
    +------+---------+--------+-------+--------+
    | 2018 |  Orange |   null |  4000 |   null |
    | 2018 |   Beans |   null |  1500 |   2000 |
    | 2018 |  Banana |   2000 |   400 |   null |
    | 2018 | Carrots |   2000 |  1200 |   null |
    | 2019 |  Orange |   5000 |  null |   5000 |
    | 2019 |   Beans |   null |  1500 |   2000 |
    | 2019 |  Banana |   null |  1400 |    400 |
    | 2019 | Carrots |   null |   200 |   null |
    +------+---------+--------+-------+--------+
    >>> unpivot(df, ['year', 'product'], key_alias='country', value_alias='amount').show()
    +------+---------+---------+--------+
    | year | product | country | amount |
    +------+---------+---------+--------+
    | 2018 |  Orange |  Canada |   null |
    | 2018 |  Orange |   China |   4000 |
    | 2018 |  Orange |  Mexico |   null |
    | 2018 |   Beans |  Canada |   null |
    | 2018 |   Beans |   China |   1500 |
    | 2018 |   Beans |  Mexico |   2000 |
    | 2018 |  Banana |  Canada |   2000 |
    | 2018 |  Banana |   China |    400 |
    | 2018 |  Banana |  Mexico |   null |
    | 2018 | Carrots |  Canada |   2000 |
    | 2018 | Carrots |   China |   1200 |
    | 2018 | Carrots |  Mexico |   null |
    | 2019 |  Orange |  Canada |   5000 |
    | 2019 |  Orange |   China |   null |
    | 2019 |  Orange |  Mexico |   5000 |
    | 2019 |   Beans |  Canada |   null |
    | 2019 |   Beans |   China |   1500 |
    | 2019 |   Beans |  Mexico |   2000 |
    | 2019 |  Banana |  Canada |   null |
    | 2019 |  Banana |   China |   1400 |
    +------+---------+---------+--------+
    only showing top 20 rows

    :param df: a DataFrame
    :param pivot_columns: The list of columns names on which to perform the pivot
    :param key_alias: alias given to the 'key' column
    :param value_alias: alias given to the 'value' column
    :param exclude_nulls: Exclude rows with null values if true
    :param implem_version: (Possible values [1, 2]) Version of the code to use.
         Version 2 uses the BigQuery's UNPIVOT statement, while version 1 doesn't.
    :return:
    """
    if implem_version == 1:
        return unpivot_v1(df, pivot_columns, key_alias, value_alias, exclude_nulls)
    elif implem_version == 2:
        return unpivot_v2(df, pivot_columns, key_alias, value_alias, exclude_nulls)
    else:
        raise Exception("Pivot, please use v1 or v2")


def unpivot_v1(
    df: DataFrame,
    pivot_columns: List[str],
    key_alias: str = "key",
    value_alias: str = "value",
    exclude_nulls: bool = False,
) -> DataFrame:
    """This version uses a LEFT JOIN UNNEST statement."""
    pivoted_columns = [(field.name, field.field_type) for field in df.schema if field.name not in pivot_columns]

    cols, types = zip(*pivoted_columns)

    # Check that all columns have the same type.
    assert len(set(types)) == 1, (
        "All pivoted columns should be of the same type:\n Pivoted columns are: %s" % pivoted_columns
    )

    # Create and explode an array of (column_name, column_value) structs
    struct_cols = [f'STRUCT("{col}" as {key_alias}, {col} as {value_alias})' for col in cols]
    exclude_nulls_str = f"WHERE {value_alias} IS NOT NULL" if exclude_nulls else ""
    query = strip_margin(
        f"""
        |SELECT
        |{cols_to_str(pivot_columns, 2)},
        |  pivoted.*
        |FROM {quote(df._alias)}
        |LEFT JOIN UNNEST([
        |{cols_to_str(struct_cols, 2)}
        |]) as pivoted
        |{exclude_nulls_str}
        |"""
    )
    return df._apply_query(query)


def unpivot_v2(
    df: DataFrame,
    pivot_columns: List[str],
    key_alias: str = "key",
    value_alias: str = "value",
    exclude_nulls: bool = False,
) -> DataFrame:
    """This version uses BigQuery's
    `UNPIVOT operator <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unpivot_operator>`_
    """
    pivoted_columns = [(field.name, field.field_type) for field in df.schema if field.name not in pivot_columns]

    cols, types = zip(*pivoted_columns)

    exclude_nulls_str = "EXCLUDE NULLS" if exclude_nulls else "INCLUDE NULLS"
    query = strip_margin(
        f"""
        |SELECT
        |  *
        |FROM {quote(df._alias)}
        |UNPIVOT {exclude_nulls_str}({value_alias} FOR {key_alias} IN ({cols_to_str(cols)}))
        |"""
    )
    return df._apply_query(query)


def __get_test_pivoted_df() -> DataFrame:
    bq = BigQueryBuilder(get_bq_client())
    query = """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(2018 as year,  "Orange" as product, null as Canada, 4000 as China,  null as Mexico),
            STRUCT(2018 as year,   "Beans" as product, null as Canada, 1500 as China,  2000 as Mexico),
            STRUCT(2018 as year,  "Banana" as product, 2000 as Canada,  400 as China,  null as Mexico),
            STRUCT(2018 as year, "Carrots" as product, 2000 as Canada, 1200 as China,  null as Mexico),
            STRUCT(2019 as year,  "Orange" as product, 5000 as Canada, null as China,  5000 as Mexico),
            STRUCT(2019 as year,   "Beans" as product, null as Canada, 1500 as China,  2000 as Mexico),
            STRUCT(2019 as year,  "Banana" as product, null as Canada, 1400 as China,   400 as Mexico),
            STRUCT(2019 as year, "Carrots" as product, null as Canada,  200 as China,  null as Mexico)
        ])
    """
    return bq.sql(query)


def __get_test_unpivoted_df():
    bq = BigQueryBuilder(get_bq_client())
    query = """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(2018 as year, "Orange"  as product, "Canada" as country, null as amount),
            STRUCT(2018 as year, "Orange"  as product,  "China" as country, 4000 as amount),
            STRUCT(2018 as year, "Orange"  as product, "Mexico" as country, null as amount),
            STRUCT(2018 as year,  "Beans"  as product, "Canada" as country, null as amount),
            STRUCT(2018 as year,  "Beans"  as product,  "China" as country, 1500 as amount),
            STRUCT(2018 as year,  "Beans"  as product, "Mexico" as country, 2000 as amount),
            STRUCT(2018 as year, "Banana"  as product, "Canada" as country, 2000 as amount),
            STRUCT(2018 as year, "Banana"  as product,  "China" as country, 400 as amount),
            STRUCT(2018 as year, "Banana"  as product, "Mexico" as country, null as amount),
            STRUCT(2018 as year, "Carrots" as product, "Canada" as country, 2000 as amount),
            STRUCT(2018 as year, "Carrots" as product,  "China" as country, 1200 as amount),
            STRUCT(2018 as year, "Carrots" as product, "Mexico" as country, null as amount),
            STRUCT(2019 as year, "Orange"  as product, "Canada" as country, 5000 as amount),
            STRUCT(2019 as year, "Orange"  as product,  "China" as country, null as amount),
            STRUCT(2019 as year, "Orange"  as product, "Mexico" as country, 5000 as amount),
            STRUCT(2019 as year,  "Beans"  as product, "Canada" as country, null as amount),
            STRUCT(2019 as year,  "Beans"  as product,  "China" as country, 1500 as amount),
            STRUCT(2019 as year,  "Beans"  as product, "Mexico" as country, 2000 as amount),
            STRUCT(2019 as year, "Banana"  as product, "Canada" as country, null as amount),
            STRUCT(2019 as year, "Banana"  as product,  "China" as country, 1400 as amount),
            STRUCT(2019 as year, "Banana"  as product, "Mexico" as country, 400 as amount),
            STRUCT(2019 as year, "Carrots" as product, "Canada" as country, null as amount),
            STRUCT(2019 as year, "Carrots" as product,  "China" as country, 200 as amount),
            STRUCT(2019 as year, "Carrots" as product, "Mexico" as country, null as amount)
        ])
    """
    return bq.sql(query)
