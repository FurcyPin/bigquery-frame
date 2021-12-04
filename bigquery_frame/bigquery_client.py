from typing import Union

from google.cloud.bigquery.client import Client
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator


class HasBigQueryClient:
    """Wrapper class for the BigQuery client

    Limitations
    -----------
    This project is a simple POC, and this class uses the user's credentials.
    It is recommended to either:
    - Use it with a test account that does not have access to sensitive information.
    - Or make sure the code you are using doesn't do anything malicious.

    In that regard, the original author declines all responsibility, especially if you use a forked version of the code.
    """

    def __init__(self, client: Client):
        self.client = client

    def _execute_query(self, query: str) -> Union[RowIterator, _EmptyRowIterator]:
        return self.client.query(query).result()
