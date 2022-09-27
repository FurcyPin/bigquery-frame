import copy
import sys
import traceback
from dataclasses import dataclass
from typing import Callable, List, Optional, TypeVar, cast

from google.api_core.exceptions import BadRequest, InternalServerError
from google.cloud.bigquery import (
    ConnectionProperty,
    QueryJob,
    QueryJobConfig,
    SchemaField,
)
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.table import RowIterator

from bigquery_frame.units import bytes_to_human_readable
from bigquery_frame.utils import number_lines, strip_margin

DEFAULT_MAX_TRY_COUNT = 3

ReturnType = TypeVar("ReturnType")


@dataclass()
class BigQueryStats:

    estimated_bytes_processed: int = 0
    """Estimation of the number of bytes computed before the query is run to dimension the query's provisioning."""

    total_bytes_processed: int = 0
    """Actual number of bytes processed by the query."""

    total_bytes_billed: int = 0
    """Actual number of bytes billed for the query.
    This may exceed the number of bytes processed for small queries because BigQuery charges a minimum of 10 MiB
    per input table to account for the query overhead. However, queries with a "LIMIT 0" are completely free.
    For more details, please see the official
    `BigQuery Documentation <https://cloud.google.com/bigquery/pricing#on_demand_pricing>`_.
    """

    def add_job_stats(self, job: QueryJob):
        if job.estimated_bytes_processed is not None:
            self.estimated_bytes_processed += job.estimated_bytes_processed
        if job.total_bytes_processed is not None:
            self.total_bytes_processed += job.total_bytes_processed
        if job.total_bytes_billed is not None:
            self.total_bytes_billed += job.total_bytes_billed

    def human_readable_estimated_bytes_processed(self):
        return f"Estimated bytes processed : {bytes_to_human_readable(self.estimated_bytes_processed)}"

    def human_readable_total_bytes_billed(self):
        return f"Total bytes billed : {bytes_to_human_readable(self.total_bytes_billed)}"

    def human_readable_total_bytes_processed(self):
        return f"Total bytes processed : {bytes_to_human_readable(self.total_bytes_processed)}"

    def human_readable(self):
        return strip_margin(
            f"""
            |{self.human_readable_estimated_bytes_processed()}
            |{self.human_readable_total_bytes_processed()}
            |{self.human_readable_total_bytes_billed()}
            |"""
        )


class HasBigQueryClient:
    """Wrapper class for the BigQuery client

    This isolates all the logic of direct interaction with the BigQuery client,
    which makes the code's security easier to audit (although nothing can be really private in Python).
    """

    def __init__(self, client: Client, use_session: bool = True, max_try_count: int = DEFAULT_MAX_TRY_COUNT):
        """Wrapper class for the BigQuery client

        :param client: A :class:`google.cloud.bigquery.client.Client`
        :param use_session: If set to true, all queries will be executed in the same session.
                            This is necessary for reusing temporary tables across multiple queries
        """
        self.max_try_count = max_try_count
        self.__use_session = use_session
        self.__client = client
        self.__session_id: Optional[str] = None
        self.__stats: BigQueryStats = BigQueryStats()

    def _get_session_id_after_query(self, job):
        if self.__use_session and self.__session_id is None and job.session_info is not None:
            self.__session_id = job.session_info.session_id

    def _set_session_id_before_query(self, job_config):
        if self.__use_session:
            if self.__session_id is None:
                job_config.create_session = True
            else:
                job_config.connection_properties = [ConnectionProperty("session_id", self.__session_id)]

    def _execute_job(
        self,
        query: str,
        action: Callable[[QueryJob], ReturnType],
        dry_run: bool,
        use_query_cache: bool,
        try_count: int = 1,
    ) -> ReturnType:
        job_config = QueryJobConfig(use_query_cache=use_query_cache, dry_run=dry_run)

        self._set_session_id_before_query(job_config)
        job = self.__client.query(query=query, job_config=job_config)
        self._get_session_id_after_query(job)

        try:
            res = action(job)
        except BadRequest as e:
            e.message += "\nQuery:\n" + number_lines(query, 1)
            raise e
        except InternalServerError as e:
            try_count += 1
            if try_count <= self.max_try_count:
                traceback.print_exc(file=sys.stderr)
            else:
                raise e
        else:
            self.__stats.add_job_stats(job)
            return res
        print(f"Retrying query (Try n°{try_count}/{self.max_try_count})", file=sys.stderr)
        return self._execute_job(query, action, dry_run=dry_run, use_query_cache=use_query_cache, try_count=try_count)

    def _get_query_schema(self, query: str) -> List[SchemaField]:
        def action(job: QueryJob) -> List[SchemaField]:
            return cast(List[SchemaField], job.schema)

        return self._execute_job(query, action, dry_run=True, use_query_cache=False)

    def _execute_query(self, query: str, use_query_cache=True) -> RowIterator:
        def action(job: QueryJob) -> RowIterator:
            return job.result()

        return self._execute_job(query, action, dry_run=False, use_query_cache=use_query_cache)

    def close(self):
        self.__client.close()

    @property
    def stats(self):
        return copy.copy(self.__stats)
