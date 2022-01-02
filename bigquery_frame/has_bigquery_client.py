from google.cloud.bigquery.client import Client
from google.cloud.bigquery.table import RowIterator


class HasBigQueryClient:
    """Wrapper class for the BigQuery client

    This isolates all the logic of direct interaction with the BigQuery client,
    which makes the code's security easier to audit (although nothing can be really private in Python).
    """

    def __init__(self, client: Client):
        self.__client = client

    def _execute_query(self, query: str) -> RowIterator:
        return self.__client.query(query).result()

    def close(self):
        self.__client.close()
