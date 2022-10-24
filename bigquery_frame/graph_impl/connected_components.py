from google.cloud.bigquery import SchemaField

from bigquery_frame import DataFrame
from bigquery_frame import functions as f
from bigquery_frame.utils import assert_true

_generic_star_sql = """
WITH edges AS (
    SELECT l_node, r_node FROM {input}
    UNION ALL
    SELECT r_node, l_node FROM {input}
    UNION ALL
    SELECT l_node, l_node FROM {input}
    UNION ALL
    SELECT r_node, r_node FROM {input}
),
neighborhood AS (
    SELECT
        l_node,
        array_agg(distinct r_node) AS neighbors_and_self
    FROM edges
    WHERE l_node IS NOT NULL and r_node IS NOT NULL
    GROUP BY l_node
)
SELECT
    l_node,
    (SELECT MIN(neighbor) FROM unnest(neighbors_and_self) AS neighbor) AS min_neighbors_and_self,
    ARRAY(SELECT * FROM unnest(neighbors_and_self) AS neighbor WHERE neighbor > l_node) AS larger_neighbors,
    ARRAY(SELECT * FROM unnest(neighbors_and_self) AS neighbor WHERE neighbor <= l_node) AS smaller_neighbors_and_self
FROM neighborhood
"""

_large_star_sql = """
SELECT
    min_neighbors_and_self AS l_node,
    r_node
FROM large_star
JOIN unnest(larger_neighbors) AS r_node
/* This adds back self-loop (N <=> N) relations */
UNION ALL
SELECT
    l_node AS l_node,
    l_node AS r_node
FROM large_star
"""

_small_star_sql = """
SELECT
    min_neighbors_and_self AS l_node,
    r_node
FROM small_star
JOIN UNNEST(smaller_neighbors_and_self) AS r_node
/* This makes sure that self-loops (N <=> N) relations are only added once */
WHERE min_neighbors_and_self <> r_node OR l_node = min_neighbors_and_self
"""


def _large_star(df: DataFrame):
    df.createOrReplaceTempView("large_star_input")
    df.bigquery.sql(_generic_star_sql.format(input="large_star_input")).createOrReplaceTempView("large_star")
    return df.bigquery.sql(_large_star_sql)


def _small_star(df: DataFrame):
    df.createOrReplaceTempView("small_star_input")
    df.bigquery.sql(_generic_star_sql.format(input="small_star_input")).createOrReplaceTempView("small_star")
    return df.bigquery.sql(_small_star_sql)


def _star_loop(df: DataFrame):
    working_df = df
    changes = 1
    while changes > 0:
        new_df = _small_star(_large_star(working_df)).persist()
        working_df.createOrReplaceTempView("old_df")
        new_df.createOrReplaceTempView("new_df")
        changes = df.bigquery.sql(
            """
            SELECT 1
            FROM new_df
            WHERE NOT EXISTS (
                SELECT 1 FROM old_df WHERE new_df.l_node = old_df.l_node AND new_df.r_node = old_df.r_node
            )"""
        ).count()
        working_df = new_df
    return working_df.select("l_node", "r_node")


def connected_components(
    df: DataFrame, node_name: str = "node_id", connected_component_col_name: str = "connected_component_id"
):
    """Compute the connected components of a non-directed graph.

    Given a DataFrame with two columns of the same type STRING or INTEGER representing the edges of a graph,
    this computes a new DataFrame containing two columns of the same type named using `node_name` and
    `connected_component_col_name`.

    This is an implementation of the Alternating Algorithm (large-star, small-star) described in the 2014 paper
    "Connected Components in MapReduce and Beyond"
    written by {rkiveris, silviol, mirrokni, rvibhor, sergeiv} @google.com

    PERFORMANCE AND COST CONSIDERATIONS
    -----------------------------------
    This algorithm has been proved to converge in O(log(n)²) and is conjectured to converge in O(log(n)), where n
    is the number of nodes in the graph. It was the most performant known distributed connected component algorithm
    last time I checked (in 2017).

    This implementation persists temporary results at each iteration loop: for the BigQuery pricing, you should
    be expecting it to cost the equivalent of 15 to 30 scans on your input table. Since the input table has only
    two columns, this should be reasonable, and we recommend using INTEGER columns rather than STRING when possible.

    If your graph contains nodes with a very high number of neighbors, the algorithm may crash. It is recommended
    to apply a pre-filtering on your nodes and remove nodes with a pathologically high cardinality.
    You should also monitor actively the number of nodes filtered this way and their cardinality, as this could help
    you detect a data quality deterioration in your input graph.
    If the input graph contains duplicate edges, they will be automatically removed by the algorithm.

    If you want to have isolated nodes (nodes that have no neighbors) in the resulting graph, there is two possible
    ways to achieve this:
    A. Add self-loops edges to all your nodes in your input graph (it also works if you add edges between all the graph
       nodes and a fictitious node with id NULL)
    B. Only add edges between distinct nodes to your input, and perform a join between your input graph and the
       algorithm's output to find all the nodes that have disappeared. These will be the isolated nodes.
    Method B. requires a little more work but it should also be cheaper.

    Example:
    >>> df = __get_test_df()
    >>> df.show()
    +--------+--------+
    | l_node | r_node |
    +--------+--------+
    |      1 |      8 |
    |      8 |      9 |
    |      5 |      8 |
    |      7 |      8 |
    |      3 |      7 |
    |      2 |      3 |
    |      4 |      6 |
    +--------+--------+
    >>> connected_components(df, connected_component_col_name="cc_id").sort("node_id", "cc_id").show()
    +---------+-------+
    | node_id | cc_id |
    +---------+-------+
    |       1 |     1 |
    |       2 |     1 |
    |       3 |     1 |
    |       4 |     4 |
    |       5 |     1 |
    |       6 |     4 |
    |       7 |     1 |
    |       8 |     1 |
    |       9 |     1 |
    +---------+-------+

    :param df:
    :param node_name: Name of the column representing the node in the output DataFrame (default: "node_id")
    :param connected_component_col_name: Name of the column representing the connected component to which each node
        belongs in the output DataFrame (default: "cc_id")
    :return:
    """
    assert_true(len(df.columns) == 2, "Input DataFrame must have two columns")
    l_field: SchemaField
    r_field: SchemaField
    [l_field, r_field] = df.schema
    assert_true(
        l_field.field_type == r_field.field_type, "The two columns of the input DataFrame must have the same type"
    )
    assert_true(
        l_field.field_type in ["STRING", "INTEGER"],
        "The two columns of the input DataFrame must be of type STRING or INTEGER",
    )
    [l_col, r_col] = df.columns
    df = df.select(f.col(l_col).alias("l_node"), f.col(r_col).alias("r_node"))
    res = _star_loop(df)
    return res.select(f.col("r_node").alias(node_name), f.col("l_node").alias(connected_component_col_name))


def __get_test_df() -> DataFrame:
    from bigquery_frame import BigQueryBuilder

    bq = BigQueryBuilder()
    df = bq.sql(
        """
        SELECT *
        FROM UNNEST([
            STRUCT(1 as l_node, 8 as r_node),
            STRUCT(8 as l_node, 9 as r_node),
            STRUCT(5 as l_node, 8 as r_node),
            STRUCT(7 as l_node, 8 as r_node),
            STRUCT(3 as l_node, 7 as r_node),
            STRUCT(2 as l_node, 3 as r_node),
            STRUCT(4 as l_node, 6 as r_node)
        ])
    """
    )
    return df
