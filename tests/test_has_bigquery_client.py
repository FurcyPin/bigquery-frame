from unittest import mock

import pytest
from google.api_core.exceptions import BadRequest, InternalServerError
from google.cloud.bigquery import Client

from bigquery_frame.has_bigquery_client import HasBigQueryClient


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
