from bigquery_frame import BigQueryBuilder
from bigquery_frame import functions as f
from bigquery_frame.graph import connected_components


def test_connected_components(bq: BigQueryBuilder):
    df = bq.sql(
        """
        SELECT *
        FROM UNNEST([
            STRUCT(1 as L, 8 as R),
            STRUCT(8 as L, 9 as R),
            STRUCT(5 as L, 8 as R),
            STRUCT(7 as L, 8 as R),
            STRUCT(3 as L, 7 as R),
            STRUCT(2 as L, 3 as R),
            STRUCT(1 as L, 3 as R),
            STRUCT(1 as L, 2 as R),
            STRUCT(1 as L, 8 as R),
            STRUCT(4 as L, 6 as R),
            STRUCT(11 as L, 18 as R),
            STRUCT(18 as L, 19 as R),
            STRUCT(15 as L, 18 as R),
            STRUCT(17 as L, 18 as R),
            STRUCT(13 as L, 17 as R),
            STRUCT(12 as L, 13 as R),
            STRUCT(14 as L, 16 as R),
            STRUCT(11 as L, 18 as R),
            STRUCT(18 as L, 19 as R),
            STRUCT(15 as L, 18 as R),
            STRUCT(17 as L, 18 as R),
            STRUCT(13 as L, 17 as R),
            STRUCT(12 as L, 13 as R),
            STRUCT(14 as L, 16 as R)
        ])
    """
    )
    expected_df = bq.sql(
        """
        SELECT *
        FROM UNNEST([
            STRUCT(1 as node_id, 1 as connected_component_id),
            STRUCT(2 as node_id, 1 as connected_component_id),
            STRUCT(3 as node_id, 1 as connected_component_id),
            STRUCT(4 as node_id, 4 as connected_component_id),
            STRUCT(5 as node_id, 1 as connected_component_id),
            STRUCT(6 as node_id, 4 as connected_component_id),
            STRUCT(7 as node_id, 1 as connected_component_id),
            STRUCT(8 as node_id, 1 as connected_component_id),
            STRUCT(9 as node_id, 1 as connected_component_id),
            STRUCT(11 as node_id, 11 as connected_component_id),
            STRUCT(12 as node_id, 11 as connected_component_id),
            STRUCT(13 as node_id, 11 as connected_component_id),
            STRUCT(14 as node_id, 14 as connected_component_id),
            STRUCT(15 as node_id, 11 as connected_component_id),
            STRUCT(16 as node_id, 14 as connected_component_id),
            STRUCT(17 as node_id, 11 as connected_component_id),
            STRUCT(18 as node_id, 11 as connected_component_id),
            STRUCT(19 as node_id, 11 as connected_component_id)
        ])
    """
    )
    df = df.select(f.col("L").alias("l_node"), f.col("R").alias("r_node")).persist()
    actual_df = connected_components(df).sort("node_id", "connected_component_id")
    assert actual_df.collect() == expected_df.collect()
