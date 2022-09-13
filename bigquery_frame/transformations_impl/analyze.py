from typing import Callable, List, Optional, Union

from google.cloud.bigquery import SchemaField

from bigquery_frame import BigQueryBuilder, DataFrame
from bigquery_frame import functions as f
from bigquery_frame.auth import get_bq_client
from bigquery_frame.column import StringOrColumn, cols_to_str
from bigquery_frame.dataframe import strip_margin
from bigquery_frame.transformations_impl import analyze_aggs
from bigquery_frame.transformations_impl.flatten_schema import flatten_schema
from bigquery_frame.transformations_impl.union_dataframes import union_dataframes
from bigquery_frame.utils import quote


def _unnest_column(df: DataFrame, col: str, extra_cols: Optional[List[str]] = None):
    """Recursively unnest a :class:`DataFrame`'s column

    >>> bq = BigQueryBuilder(get_bq_client())
    >>> df = bq.sql('SELECT 1 as id, '
    ... '[STRUCT(2 as a, [STRUCT(3 as c, 4 as d)] as b, [5, 6] as e)] as s1, STRUCT(7 as f) as s2')
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
    if extra_cols is None:
        extra_cols = []

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

        query = strip_margin(
            f"""
            |SELECT
            |  {cols_to_str(extra_cols + [col], 2)}
            |FROM {quote(df._alias)}
            |{cross_join_str}"""
        )
        return df._apply_query(query)
    else:
        return df


def _select_group_by(
    df: DataFrame, *columns: Union[List[StringOrColumn], StringOrColumn], group_by: List[str]
) -> "DataFrame":
    """Projects a set of expressions and returns a new :class:`DataFrame`."""
    if isinstance(columns[0], list):
        if len(columns) == 1:
            columns = columns[0]
        else:
            raise TypeError(f"Wrong argument type: {type(columns)}")
    group_by_str = ""
    if len(group_by) > 0:
        group_by_str = f"\nGROUP BY {cols_to_str(group_by)}"
    query = strip_margin(
        f"""SELECT
        |{cols_to_str(columns, 2)}
        |FROM {quote(df._alias)}{group_by_str}"""
    )
    return df._apply_query(query)


def _analyze_column(df: DataFrame, schema_field: SchemaField, col_num: int, group_by: List[str], aggs: List[Callable]):
    col = schema_field.name
    is_repeated = "!" in col

    group_alias = df._alias
    if is_repeated:
        if col[-1] == "!":
            col = col.split("!")[-2].replace(".", "")
        else:
            col = col.split(".")[-1]
        group_alias = quote("__group1__")
        group_by_1 = [df[col].alias(col.replace(".", "_")) for col in group_by]
        group_by_2 = [f.expr(f"{group_alias}.{quote(col.replace('.', '_'))}") for col in group_by]
        group_select_1 = [f.struct(*group_by_1).alias(group_alias)] if len(group_by) > 0 else []
        df = _unnest_column(df, schema_field.name, extra_cols=group_select_1)
    else:
        group_by_2 = [f.expr(f"{group_alias}.{quote(col)}") for col in group_by]
    group_select_2 = [f.struct(*group_by_2).alias(quote("group"))] if len(group_by) > 0 else []

    res = _select_group_by(df, *group_select_2, *[agg(col, schema_field, col_num) for agg in aggs], group_by=group_by_2)
    return res


def __chunks(lst: List, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        # fmt: off
        yield lst[i: i + n]
        # fmt: on


default_aggs = [
    analyze_aggs.column_number,
    analyze_aggs.column_name,
    analyze_aggs.column_type,
    analyze_aggs.count,
    analyze_aggs.count_distinct,
    analyze_aggs.count_null,
    analyze_aggs.min,
    analyze_aggs.max,
    analyze_aggs.approx_top_100,
]


def analyze(
    df: DataFrame,
    group_by: Optional[Union[str, List[str]]] = None,
    _aggs: Optional[List[Callable]] = None,
    _chunk_size: int = 50,
):
    """Analyze a DataFrame by computing various stats for each column.

    By default, it returns a DataFrame with one row per column and the following columns
    (but the columns computed can be customized, see the Customization section below):

    - column_number: Number of the column (useful for sorting)
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

    Customization:
    --------------
    By default, this method will compute for each column the aggregations listed in
    `bigquery_frame.transformation_impl.analyze.default_aggs`, but users can change this and even add their
    own custom aggregation by passing the argument `_agg`, a list of aggregation functions with the following signature
    (col: str, schema_field: SchemaField, col_num: int) -> Column

    Examples of aggregation methods can be found in the module :module:`bigquery_frame.transformation_impl.analyze_aggs`

    Grouping:
    ---------
    With the `group_by` option, users can specify one or multiple columns for which the statistics will be grouped.
    If this option is used, an extra column "group" of type struct will be added to output DataFrame.
    See the examples below.

    Troubleshooting:
    ----------------
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

    Examples:
    ---------
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

    >>> analyzed_df = analyze(df)
    Analyzing 5 columns ...
    >>> analyzed_df.withColumn("approx_top_100", f.expr("approx_top_100[OFFSET(0)]"), replace=True).show()  # noqa: E501
    +---------------+------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------+
    | column_number |            column_name | column_type | count | count_distinct | count_null |       min |       max |                     approx_top_100 |
    +---------------+------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------+
    |             0 |                     id |     INTEGER |     9 |              9 |          0 |         1 |         9 |         {'value': '1', 'count': 1} |
    |             1 |                   name |      STRING |     9 |              9 |          0 | Blastoise | Wartortle | {'value': 'Bulbasaur', 'count': 1} |
    |             2 |                 types! |      STRING |    13 |              5 |          0 |      Fire |     Water |     {'value': 'Grass', 'count': 3} |
    |             3 |   evolution.can_evolve |     BOOLEAN |     9 |              2 |          0 |     false |      true |      {'value': 'true', 'count': 6} |
    |             4 | evolution.evolves_from |     INTEGER |     9 |              6 |          3 |         1 |         8 |      {'value': 'NULL', 'count': 3} |
    +---------------+------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------+

    >>> df = __get_test_df().withColumn("main_type", f.expr("types[OFFSET(0)]"))
    >>> df.show()
    +----+------------+---------------------+--------------------------------------------+-----------+
    | id |       name |               types |                                  evolution | main_type |
    +----+------------+---------------------+--------------------------------------------+-----------+
    |  1 |  Bulbasaur | ['Grass', 'Poison'] | {'can_evolve': True, 'evolves_from': None} |     Grass |
    |  2 |    Ivysaur | ['Grass', 'Poison'] |    {'can_evolve': True, 'evolves_from': 1} |     Grass |
    |  3 |   Venusaur | ['Grass', 'Poison'] |   {'can_evolve': False, 'evolves_from': 2} |     Grass |
    |  4 | Charmander |            ['Fire'] | {'can_evolve': True, 'evolves_from': None} |      Fire |
    |  5 | Charmeleon |            ['Fire'] |    {'can_evolve': True, 'evolves_from': 4} |      Fire |
    |  6 |  Charizard |  ['Fire', 'Flying'] |   {'can_evolve': False, 'evolves_from': 5} |      Fire |
    |  7 |   Squirtle |           ['Water'] | {'can_evolve': True, 'evolves_from': None} |     Water |
    |  8 |  Wartortle |           ['Water'] |    {'can_evolve': True, 'evolves_from': 7} |     Water |
    |  9 |  Blastoise |           ['Water'] |   {'can_evolve': False, 'evolves_from': 8} |     Water |
    +----+------------+---------------------+--------------------------------------------+-----------+

    >>> from bigquery_frame.transformations_impl import analyze_aggs
    >>> aggs = [
    ...     analyze_aggs.column_number,
    ...     analyze_aggs.column_name,
    ...     analyze_aggs.count,
    ...     analyze_aggs.count_distinct,
    ...     analyze_aggs.count_null,
    ... ]
    >>> analyzed_df = analyze(df, group_by="main_type", _aggs=aggs)
    Analyzing 5 columns ...
    >>> analyzed_df.orderBy("`group`.main_type", "column_number").show()
    +------------------------+---------------+------------------------+-------+----------------+------------+
    |                  group | column_number |            column_name | count | count_distinct | count_null |
    +------------------------+---------------+------------------------+-------+----------------+------------+
    |  {'main_type': 'Fire'} |             0 |                     id |     3 |              3 |          0 |
    |  {'main_type': 'Fire'} |             1 |                   name |     3 |              3 |          0 |
    |  {'main_type': 'Fire'} |             2 |                 types! |     4 |              2 |          0 |
    |  {'main_type': 'Fire'} |             3 |   evolution.can_evolve |     3 |              2 |          0 |
    |  {'main_type': 'Fire'} |             4 | evolution.evolves_from |     3 |              2 |          1 |
    | {'main_type': 'Grass'} |             0 |                     id |     3 |              3 |          0 |
    | {'main_type': 'Grass'} |             1 |                   name |     3 |              3 |          0 |
    | {'main_type': 'Grass'} |             2 |                 types! |     6 |              2 |          0 |
    | {'main_type': 'Grass'} |             3 |   evolution.can_evolve |     3 |              2 |          0 |
    | {'main_type': 'Grass'} |             4 | evolution.evolves_from |     3 |              2 |          1 |
    | {'main_type': 'Water'} |             0 |                     id |     3 |              3 |          0 |
    | {'main_type': 'Water'} |             1 |                   name |     3 |              3 |          0 |
    | {'main_type': 'Water'} |             2 |                 types! |     3 |              1 |          0 |
    | {'main_type': 'Water'} |             3 |   evolution.can_evolve |     3 |              2 |          0 |
    | {'main_type': 'Water'} |             4 | evolution.evolves_from |     3 |              2 |          1 |
    +------------------------+---------------+------------------------+-------+----------------+------------+

    :param df: a DataFrame
    :param group_by: (optional) a list of column names on which the aggregations will be grouped
    :param _aggs: (optional) a list of aggregation to override the default aggregation made by the function
    :param _chunk_size: (default: 50) Try reducing this number if you get this error from BigQuery:
        "Not enough resources for query planning"
    :return:
    """
    if group_by is None:
        group_by = []
    if _aggs is None:
        _aggs = default_aggs
    if isinstance(group_by, str):
        group_by = [group_by]
    flat_schema = flatten_schema(df.schema, explode=True)
    col_dfs = [
        _analyze_column(df, schema_field, num, group_by, _aggs)
        for num, schema_field in enumerate(flat_schema)
        if schema_field.name not in group_by
    ]

    nb_cols = len(col_dfs)
    print(f"Analyzing {nb_cols} columns ...")
    if nb_cols <= _chunk_size:
        union_df = union_dataframes(col_dfs)
    else:
        from tqdm import tqdm

        big_dfs = [union_dataframes(chunk).persist() for chunk in tqdm(list(__chunks(col_dfs, _chunk_size)))]
        union_df = union_dataframes(big_dfs)
    # For some reason, `union_df.orderBy("column_number").drop("column_number")`
    # does not preserve the ordering, but this does:
    query = f"""SELECT * FROM {quote(union_df._alias)} ORDER BY column_number"""
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
