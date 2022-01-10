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
    >>> df = bq.sql('SELECT 1 as id, [STRUCT(2 as a, [STRUCT(3 as c, 4 as d)] as b, [5, 6] as e)] as s1, STRUCT(7 as f) as s2')
    >>> [col.name for col in flatten_schema(df.schema, explode=True)]
    ['id', 's1!.a', 's1!.b!.c', 's1!.b!.d', 's1!.e!', 's2.f']
    >>> _unnest_column(df, 'id').show()
    +----+--------------------------------------------------+----------+
    | id |                                               s1 |       s2 |
    +----+--------------------------------------------------+----------+
    |  1 | [{'a': 2, 'b': [{'c': 3, 'd': 4}], 'e': [5, 6]}] | {'f': 7} |
    +----+--------------------------------------------------+----------+
    >>> _unnest_column(df, 's1!.a').show()
    +---+
    | a |
    +---+
    | 2 |
    +---+
    >>> _unnest_column(df, 's1!.b!.c').show()
    +---+
    | c |
    +---+
    | 3 |
    +---+
    >>> _unnest_column(df, 's1!.b!.d').show()
    +---+
    | d |
    +---+
    | 4 |
    +---+
    >>> _unnest_column(df, 's1!.e!').show()
    +---+
    | e |
    +---+
    | 5 |
    | 6 |
    +---+
    >>> _unnest_column(df, 's2.f').show()
    +----+--------------------------------------------------+----------+
    | id |                                               s1 |       s2 |
    +----+--------------------------------------------------+----------+
    |  1 | [{'a': 2, 'b': [{'c': 3, 'd': 4}], 'e': [5, 6]}] | {'f': 7} |
    +----+--------------------------------------------------+----------+

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


def _analyze_column(df: DataFrame, schema_field: SchemaField, col_num: int):
    col = schema_field.name
    if "!" in col:
        if col[-1] == "!":
            col = col.split("!")[-2]
        else:
            col = col.split(".")[-1]
    df = _unnest_column(df, schema_field.name)
    res = df.select(
        f.lit(col_num).alias("column_number"),
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


def __chunks(lst: List, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def analyze(df: DataFrame, _chunk_size: int = 50):
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

    Troubleshooting:
    -----------------------
    When working on entreprise-level data, BigQuery might return a 400 error with the following message.
    "Not enough resources for query planning - too many subqueries or query is too complex"

    This may happen in two cases:
    - If the input DataFrame is already the result of a complex calculation, it is recommended to save it to a
      permanent or temporary table first. You can save it to a temporary table by using `df = df.persist()`
    - When analyzing DataFrames with hundreds of columns, the query generated by analyze() might get too complex
      for BigQuery. To avoid this, we split the query into smaller parts that are persisted.
      The optional argument `_chunk_size` (default = 50) determines the number of columns analyzed per chunk.
      If you already persisted the input DataFrame and the same error persist, you can try lowering the value
      of `_chunk_size`, but beware that the lower this value, the slower the analyzis will be.

    >>> df = __get_test_df()
    >>> df.show()
    +----+------------+---------------------+--------------------------------------------+
    | id |       name |               types |                                  evolution |
    +----+------------+---------------------+--------------------------------------------+
    |  1 |  Bulbasaur | ['Grass', 'Poison'] | {'can_evolve': True, 'evolves_from': None} |
    |  2 |    Ivysaur | ['Grass', 'Poison'] |    {'can_evolve': True, 'evolves_from': 1} |
    |  3 |   Venusaur | ['Grass', 'Poison'] |   {'can_evolve': False, 'evolves_from': 2} |
    |  4 | Charmander |            ['Fire'] | {'can_evolve': True, 'evolves_from': None} |
    |  5 | Charmeleon |            ['Fire'] |    {'can_evolve': True, 'evolves_from': 4} |
    |  6 |  Charizard |  ['Fire', 'Flying'] |   {'can_evolve': False, 'evolves_from': 5} |
    |  7 |   Squirtle |           ['Water'] | {'can_evolve': True, 'evolves_from': None} |
    |  8 |  Wartortle |           ['Water'] |    {'can_evolve': True, 'evolves_from': 7} |
    |  9 |  Blastoise |           ['Water'] |   {'can_evolve': False, 'evolves_from': 8} |
    +----+------------+---------------------+--------------------------------------------+
    >>> df = analyze(df)
    Analyzing 5 columns ...
    >>> df.withColumn("approx_top_100", f.expr("approx_top_100[OFFSET(0)]"), replace=True).show()
    +------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------+
    |            column_name | column_type | count | count_distinct | count_null |       min |       max |                     approx_top_100 |
    +------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------+
    |                     id |     INTEGER |     9 |              9 |          0 |         1 |         9 |         {'value': '1', 'count': 1} |
    |                   name |      STRING |     9 |              9 |          0 | Blastoise | Wartortle | {'value': 'Bulbasaur', 'count': 1} |
    |                 types! |      STRING |    13 |              5 |          0 |      Fire |     Water |     {'value': 'Grass', 'count': 3} |
    |   evolution.can_evolve |     BOOLEAN |     9 |              2 |          0 |     false |      true |      {'value': 'true', 'count': 6} |
    | evolution.evolves_from |     INTEGER |     9 |              6 |          3 |         1 |         8 |      {'value': 'NULL', 'count': 3} |
    +------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------+

    :param df:
    :param _chunk_size: (default: 50) Try reducing this number if you get this error from BigQuery: "Not enough resources for query planning"
    :return:
    """
    flat_schema = flatten_schema(df.schema, explode=True)
    col_dfs = [_analyze_column(df, schema_field, num) for num, schema_field in enumerate(flat_schema)]

    nb_cols = len(col_dfs)
    print(f"Analyzing {nb_cols} columns ...")
    if nb_cols <= _chunk_size:
        union_df = union_dataframes(col_dfs)
    else:
        from tqdm import tqdm
        big_dfs = [
            union_dataframes(chunk).persist()
            for chunk in tqdm(list(__chunks(col_dfs, _chunk_size)))
        ]
        union_df = union_dataframes(big_dfs)
    # For some reason, `union_df.orderBy("column_number").drop("column_number")`
    # does not preserve the ordering, but this does:
    query = f"""SELECT * EXCEPT(column_number) FROM {quote(union_df._alias)} ORDER BY column_number"""
    return union_df._apply_query(query)


def __get_test_df() -> DataFrame:
    bq = BigQueryBuilder(get_bq_client())
    query = """
        SELECT 
            *
        FROM UNNEST ([
            STRUCT(
                1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, 
                STRUCT(TRUE as can_evolve, NULL as evolves_from) as evolution
            ),
            STRUCT(
                2 as id, "Ivysaur" as name, ["Grass", "Poison"] as types, 
                STRUCT(TRUE as can_evolve, 1 as evolves_from) as evolution
            ),
            STRUCT(
                3 as id, "Venusaur" as name, ["Grass", "Poison"] as types, 
                STRUCT(FALSE as can_evolve, 2 as evolves_from) as evolution
            ),
            STRUCT(
                4 as id, "Charmander" as name, ["Fire"] as types, 
                STRUCT(TRUE as can_evolve, NULL as evolves_from) as evolution
            ),
            STRUCT(
                5 as id, "Charmeleon" as name, ["Fire"] as types, 
                STRUCT(TRUE as can_evolve, 4 as evolves_from) as evolution
            ),
            STRUCT(
                6 as id, "Charizard" as name, ["Fire", "Flying"] as types, 
                STRUCT(FALSE as can_evolve, 5 as evolves_from) as evolution
            ),
            STRUCT(
                7 as id, "Squirtle" as name, ["Water"] as types, 
                STRUCT(TRUE as can_evolve, NULL as evolves_from) as evolution
            ),
            STRUCT(
                8 as id, "Wartortle" as name, ["Water"] as types, 
                STRUCT(TRUE as can_evolve, 7 as evolves_from) as evolution
            ),
            STRUCT(
                9 as id, "Blastoise" as name, ["Water"] as types, 
                STRUCT(FALSE as can_evolve, 8 as evolves_from) as evolution
            )
        ])
    """
    return bq.sql(query)
