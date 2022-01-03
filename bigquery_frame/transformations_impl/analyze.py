from typing import List

from google.cloud.bigquery import SchemaField

from bigquery_frame import DataFrame, BigQueryBuilder
from bigquery_frame.auth import get_bq_client
from bigquery_frame import functions as f
from bigquery_frame.dataframe import strip_margin, quote
from bigquery_frame.transformations_impl.flatten import flatten_schema
from bigquery_frame.transformations_impl.union_dataframes import union_dataframes


def _unnest_column(df: DataFrame, col: str):
    """Recursively unnest a :class:`DataFrame`'s column

    >>> bq = BigQueryBuilder(get_bq_client())
    >>> df = bq.sql('SELECT 1 as id, [STRUCT(2 as a, [STRUCT(3 as c, 4 as d)] as b, [5, 6] as e)] as s')
    >>> [col.name for col in flatten_schema(df.schema, explode=True)]
    ['id', 's!.a', 's!.b!.c', 's!.b!.d', 's!.e!']
    >>> _unnest_column(df, 'id').show()
    +----+
    | id |
    +----+
    | 1  |
    +----+
    >>> _unnest_column(df, 's!.a').show()
    +---+
    | a |
    +---+
    | 2 |
    +---+
    >>> _unnest_column(df, 's!.b!.c').show()
    +---+
    | c |
    +---+
    | 3 |
    +---+
    >>> _unnest_column(df, 's!.b!.d').show()
    +---+
    | d |
    +---+
    | 4 |
    +---+
    >>> _unnest_column(df, 's!.e!').show()
    +---+
    | e |
    +---+
    | 5 |
    | 6 |
    +---+

    :param df:
    :param col:
    :return:
    """
    def build_cross_join_statement(split: List[str]):
        previous = ""
        counter = 1
        for sub in split[:-1]:
            if sub[0] == ".":
                struct = previous + sub
            else:
                struct = sub
            alias = f"_unnest_{counter}"
            previous = alias
            counter += 1
            yield f"CROSS JOIN UNNEST({struct}) as {alias}"

    if "!" in col:
        split = col.split("!")
        cross_joins = list(build_cross_join_statement(split))
        cross_join_str = "\n".join(cross_joins)
        last = split[-1]
        col = f"_unnest_{len(cross_joins)}"
        if "." in last:
            col += last
        else:
            col += " as " + split[-2].replace(".", "")

        query = strip_margin(f"""
            |SELECT 
            |  {col}
            |FROM {quote(df._alias)}
            |{cross_join_str}""")
        return df._apply_query(query)
    else:
        return df


def _analyze_column(df: DataFrame, schema_field: SchemaField):
    col = schema_field.name
    if "!" in col:
        col = col.split("!")[-2]
    if "." in col:
        col = col.split(".")[-2]
    df = _unnest_column(df, schema_field.name)
    res = df.select(
        f.lit(schema_field.name).alias("column_name"),
        f.lit(schema_field.field_type).alias("column_type"),
        f.count(f.lit(1)).alias("count"),
        f.count_distinct(col).alias("count_distinct"),
        (f.count(f.lit(1)) - f.count(col)).alias("count_null"),
        f.min(col).asType("STRING").alias("min"),
        f.max(col).asType("STRING").alias("max"),
        f.expr(f"APPROX_TOP_COUNT(COALESCE(CAST({col} as STRING), 'NULL'), 100)").alias("approx_top_100")
    )
    return res


def analyze(df: DataFrame):
    """Analyze a DataFrame by computing various stats for each column.

    It returns a DataFrame with one row per column and the following columns:

    - column_name: Name of the column
    - column_type: Type of the column
    - count: Number of rows in the column, it is equal to the number of rows in the table, except for columns nested
      inside arrays for which it may be different
    - count_distinct: Number of distinct values
    - count_null: Number of null values
    - min: smallest value
    - max: largest value
    - approx_top_100: Top 100 most frequent values with their respective count

    Implementation details:
    -----------------------

    - Structs are flattened with a `.` in their name.
    - Arrays are unnested with a `!` character in their name, which is why they may have a different count.
    - Null values are not counted in the count_distinct column.

    >>> df = __get_test_df()
    >>> df.show()
    +----+------------+---------------------+------------+--------------+
    | id |    name    |        types        | can_evolve | evolves_from |
    +----+------------+---------------------+------------+--------------+
    | 1  | Bulbasaur  | ['Grass', 'Poison'] |    True    |     null     |
    | 2  |  Ivysaur   | ['Grass', 'Poison'] |    True    |      1       |
    | 3  |  Venusaur  | ['Grass', 'Poison'] |   False    |      2       |
    | 4  | Charmander |      ['Fire']       |    True    |     null     |
    | 5  | Charmeleon |      ['Fire']       |    True    |      4       |
    | 6  | Charizard  | ['Fire', 'Flying']  |   False    |      5       |
    | 7  |  Squirtle  |      ['Water']      |    True    |     null     |
    | 8  | Wartortle  |      ['Water']      |    True    |      7       |
    | 9  | Blastoise  |      ['Water']      |   False    |      8       |
    +----+------------+---------------------+------------+--------------+
    >>> df = analyze(df)
    >>> df.withColumn("approx_top_100", f.expr("approx_top_100[OFFSET(0)]"), replace=True).show()
    +--------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------+
    | column_name  | column_type | count | count_distinct | count_null |    min    |    max    |           approx_top_100           |
    +--------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------+
    |      id      |   INTEGER   |   9   |       9        |     0      |     1     |     9     |     {'value': '1', 'count': 1}     |
    |     name     |   STRING    |   9   |       9        |     0      | Blastoise | Wartortle | {'value': 'Bulbasaur', 'count': 1} |
    |    types!    |   STRING    |  13   |       5        |     0      |   Fire    |   Water   |   {'value': 'Grass', 'count': 3}   |
    |  can_evolve  |   BOOLEAN   |   9   |       2        |     0      |   false   |   true    |   {'value': 'true', 'count': 6}    |
    | evolves_from |   INTEGER   |   9   |       6        |     3      |     1     |     8     |   {'value': 'NULL', 'count': 3}    |
    +--------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------+

    :param df:
    :return:
    """
    flat_schema = flatten_schema(df.schema, explode=True)
    col_dfs = [_analyze_column(df, schema_field) for schema_field in flat_schema]
    return union_dataframes(col_dfs)


def __get_test_df() -> DataFrame:
    bq = BigQueryBuilder(get_bq_client())
    query = """
        SELECT 
            *
        FROM UNNEST ([
            STRUCT(1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, TRUE as can_evolve, NULL as evolves_from),
            STRUCT(2 as id, "Ivysaur" as name, ["Grass", "Poison"] as types, TRUE as can_evolve, 1 as evolves_from),
            STRUCT(3 as id, "Venusaur" as name, ["Grass", "Poison"] as types, FALSE as can_evolve, 2 as evolves_from),
            STRUCT(4 as id, "Charmander" as name, ["Fire"] as types, TRUE as can_evolve, NULL as evolves_from),
            STRUCT(5 as id, "Charmeleon" as name, ["Fire"] as types, TRUE as can_evolve, 4 as evolves_from),
            STRUCT(6 as id, "Charizard" as name, ["Fire", "Flying"] as types, FALSE as can_evolve, 5 as evolves_from),
            STRUCT(7 as id, "Squirtle" as name, ["Water"] as types, TRUE as can_evolve, NULL as evolves_from),
            STRUCT(8 as id, "Wartortle" as name, ["Water"] as types, TRUE as can_evolve, 7 as evolves_from),
            STRUCT(9 as id, "Blastoise" as name, ["Water"] as types, FALSE as can_evolve, 8 as evolves_from)
        ])
    """
    return bq.sql(query)

