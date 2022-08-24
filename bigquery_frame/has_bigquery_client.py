from typing import Optional

from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import ConnectionProperty, QueryJobConfig
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.table import RowIterator

from bigquery_frame.utils import number_lines


class HasBigQueryClient:
    """Wrapper class for the BigQuery client

    This isolates all the logic of direct interaction with the BigQuery client,
    which makes the code's security easier to audit (although nothing can be really private in Python).
    """

    def __init__(self, client: Client, use_session: bool = True):
        """Wrapper class for the BigQuery client

        :param client: A :class:`google.cloud.bigquery.client.Client`
        :param use_session: If set to true, all queries will be executed in the same session.
                            This is necessary for reusing temporary tables across multiple queries
        """
        self.__use_session = use_session
        self.__client = client
        self.__session_id: Optional[str] = None

    def _execute_query(self, query: str) -> RowIterator:
        job_config = QueryJobConfig()

        if self.__use_session and self.__session_id is None:
            job_config.create_session = True

        if self.__use_session and self.__session_id is not None:
            job_config.connection_properties = [
                ConnectionProperty("session_id", self.__session_id),
            ]

        job = self.__client.query(query=query, job_config=job_config)

        if self.__use_session and self.__session_id is None and job.session_info is not None:
            self.__session_id = job.session_info.session_id

        try:
            return job.result()
        except BadRequest as e:
            e.message += "\nQuery:\n" + number_lines(query, 1)
            raise e

    def close(self):
        self.__client.close()
