from unittest import mock

import pytest
from google.api_core.exceptions import BadRequest, InternalServerError
from google.cloud.bigquery import Client

from bigquery_frame.has_bigquery_client import HasBigQueryClient
from bigquery_frame.utils import strip_margin


def test_error_handling(client: Client):
    """
    GIVEN a HasBigQueryClient
    WHEN we execute a query with an incorrect syntax
    THEN a BadRequest exception should be raised
     AND it should contain the numbered text of the query
    """
    bq_client = HasBigQueryClient(client)
    bad_query = """bad query"""
    with pytest.raises(BadRequest) as e:
        bq_client._execute_query(bad_query)
    assert f"Query:\n1: {bad_query}" in e.value.message


def test_runtime_error_handling(client: Client):
    """
    GIVEN a HasBigQueryClient
    WHEN we execute a query that compiles but fails at runtime
    THEN a BadRequest exception should be raised
     AND it should contain the numbered text of the query
    """
    bq_client = HasBigQueryClient(client)
    bad_query = """SELECT (SELECT * FROM UNNEST ([1, 2]))"""
    with pytest.raises(BadRequest) as e:
        bq_client._execute_query(bad_query)
    assert f"Query:\n1: {bad_query}" in e.value.message


def test_retry(client: Client):
    """
    GIVEN a HasBigQueryClient
    WHEN we execute a query and an InternalServerError happens
    THEN we retry the query 3 times
    """

    def result_mock(*args, **kwargs):
        raise InternalServerError("This is a test error")

    bq_client = HasBigQueryClient(client)
    bad_query = """bad query"""
    with mock.patch("google.cloud.bigquery.job.query.QueryJob.result", side_effect=result_mock) as mocked_result:
        with pytest.raises(InternalServerError) as e:
            bq_client._execute_query(bad_query)
    assert mocked_result.call_count == 3
    assert "This is a test error" in e.value.message


def test_stats_human_readable(client: Client, random_test_dataset: str):
    """
    GIVEN a HasBigQueryClient
    WHEN we execute a query that reads from a table
     AND we display the query stats in human_readable mode
    THEN they should be correctly displayed
    """
    bq_client = HasBigQueryClient(client)
    bq_client._execute_query(f"CREATE OR REPLACE TABLE {random_test_dataset}.my_table AS SELECT 1 as a")
    bq_client._execute_query(f"SELECT * FROM {random_test_dataset}.my_table")
    bq_client._execute_query(f"SELECT * FROM {random_test_dataset}.my_table LIMIT 1")
    expected = strip_margin(
        """
    |Estimated bytes processed : 16.00 B
    |Total bytes processed : 16.00 B
    |Total bytes billed : 20.00 MiB
    |"""
    )
    assert bq_client.stats.human_readable() == expected


def test_cache_enabled(client: Client, random_test_dataset: str):
    """
    GIVEN a HasBigQueryClient
    WHEN we execute a query that reads from a table with the query cache ENABLED
    THEN the number of bytes processed/billed SHOULD NOT increase the second time
    """
    bq_client = HasBigQueryClient(client)
    bq_client._execute_query(f"CREATE OR REPLACE TABLE {random_test_dataset}.my_table AS SELECT 1 as a")
    bq_client._execute_query(f"SELECT * FROM {random_test_dataset}.my_table", use_query_cache=True)
    assert bq_client.stats.estimated_bytes_processed == 8
    assert bq_client.stats.total_bytes_processed == 8
    assert bq_client.stats.total_bytes_billed == 10 * 1024 * 1024
    bq_client._execute_query(f"SELECT * FROM {random_test_dataset}.my_table", use_query_cache=True)
    assert bq_client.stats.estimated_bytes_processed == 8
    assert bq_client.stats.total_bytes_processed == 8
    assert bq_client.stats.total_bytes_billed == 10 * 1024 * 1024


def test_cache_disabled(client: Client, random_test_dataset: str):
    """
    GIVEN a HasBigQueryClient
    WHEN we execute a query that reads from a table with the query cache DISABLED
    THEN the number of bytes processed/billed SHOULD increase the second time
    """
    bq_client = HasBigQueryClient(client)
    bq_client._execute_query(f"CREATE OR REPLACE TABLE {random_test_dataset}.my_table AS SELECT 1 as a")
    bq_client._execute_query(f"SELECT * FROM {random_test_dataset}.my_table", use_query_cache=True)
    assert bq_client.stats.estimated_bytes_processed == 8
    assert bq_client.stats.total_bytes_processed == 8
    assert bq_client.stats.total_bytes_billed == 10 * 1024 * 1024
    bq_client._execute_query(f"SELECT * FROM {random_test_dataset}.my_table", use_query_cache=False)
    assert bq_client.stats.estimated_bytes_processed == 8 * 2
    assert bq_client.stats.total_bytes_processed == 8 * 2
    assert bq_client.stats.total_bytes_billed == 10 * 1024 * 1024 * 2
