import unittest
from unittest import mock

from google.api_core.exceptions import BadRequest, InternalServerError

from bigquery_frame.auth import get_bq_client
from bigquery_frame.has_bigquery_client import HasBigQueryClient


class TestHasBigQueryClient(unittest.TestCase):
    def test_error_handling(self):
        """
        GIVEN a HasBigQueryClient
        WHEN we execute a query with an incorrect syntax
        THEN a BadRequest exception should be raised
         AND it should contain the numbered text of the query
        """
        client = HasBigQueryClient(get_bq_client())
        bad_query = """bad query"""
        with self.assertRaises(BadRequest) as e:
            client._execute_query(bad_query)
        self.assertIn(f"Query:\n1: {bad_query}", e.exception.message)

    def test_runtime_error_handling(self):
        """
        GIVEN a HasBigQueryClient
        WHEN we execute a query that compiles but fails at runtime
        THEN a BadRequest exception should be raised
         AND it should contain the numbered text of the query
        """
        client = HasBigQueryClient(get_bq_client())
        bad_query = """SELECT (SELECT * FROM UNNEST ([1, 2]))"""
        with self.assertRaises(BadRequest) as e:
            client._execute_query(bad_query)
        self.assertIn(f"Query:\n1: {bad_query}", e.exception.message)

    def test_retry(self):
        """
        GIVEN a HasBigQueryClient
        WHEN we execute a query and an InternalServerError happens
        THEN we retry the query 3 times
        """

        def result_mock(*args, **kwargs):
            raise InternalServerError("This is a test error")

        client = HasBigQueryClient(get_bq_client())
        bad_query = """bad query"""
        with mock.patch("google.cloud.bigquery.job.query.QueryJob.result", side_effect=result_mock) as mocked_result:
            with self.assertRaises(InternalServerError) as e:
                client._execute_query(bad_query)
        self.assertEqual(3, mocked_result.call_count)
        self.assertIn("This is a test error", e.exception.message)
