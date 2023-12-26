from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

from google.cloud.bigquery import Client, SchemaField
from google.cloud.bigquery.table import RowIterator

import bigquery_frame
from bigquery_frame.auth import get_bq_client
from bigquery_frame.has_bigquery_client import HasBigQueryClient
from bigquery_frame.temp_names import _get_temp_table_name
from bigquery_frame.utils import indent, quote, strip_margin

if TYPE_CHECKING:
    from bigquery_frame import DataFrame


class BigQueryBuilder(HasBigQueryClient):
    def __init__(self, client: Optional[Client] = None, use_session: bool = True, debug: bool = False):
        if client is None:
            client = get_bq_client()
        super().__init__(client, use_session)
        self._views: Dict[str, "DataFrame"] = {}
        self._temp_tables: Set[str] = set()
        self.debug = debug

    def table(self, full_table_name: str) -> "DataFrame":
        """Returns the specified table as a :class:`DataFrame`."""
        from bigquery_frame import DataFrame

        query = f"""SELECT * FROM {quote(full_table_name)}"""
        return DataFrame(query, alias=None, bigquery=self)

    def sql(self, sql_query) -> "DataFrame":
        from bigquery_frame import DataFrame

        """Returns a :class:`DataFrame` representing the result of the given query."""
        return DataFrame(sql_query, None, self)

    def _generate_header(self) -> str:
        return f"/* This query was generated using bigquery-frame v{bigquery_frame.__version__} */\n"

    def _get_query_schema(self, query: str) -> List[SchemaField]:
        query = self._generate_header() + query
        return super()._get_query_schema(query)

    def _execute_query(self, query: str, use_query_cache=True) -> RowIterator:
        query = self._generate_header() + query
        return super()._execute_query(query, use_query_cache=use_query_cache)

    def _registerDataFrameAsTempView(self, df: "DataFrame", alias: str) -> None:
        self._views[alias] = df

    def _registerDataFrameAsTempTable(self, df: "DataFrame", alias: Optional[str] = None) -> "DataFrame":
        if alias is None:
            alias = _get_temp_table_name()
        query = f"CREATE OR REPLACE TEMP TABLE {quote(alias)} AS \n" + df.compile()
        self._execute_query(query)
        return self.table(alias)

    def _compile_views(self) -> Dict[str, str]:
        return {
            alias: strip_margin(
                f"""{quote(alias)} AS (
                |{indent(df._compile_with_deps(), 2)}
                |)"""
            )
            for alias, df in self._views.items()
        }

    def _check_alias(self, new_alias, deps: List[Tuple[str, "DataFrame"]]) -> None:
        """Checks that the alias follows BigQuery constraints, such as:

        - BigQuery does not allow having two CTEs with the same name in a query.

        :param new_alias:
        :param deps:
        :return: None
        :raises: an Exception if something that does not comply with BigQuery's rules is found.
        """
        collisions = [alias for alias, df in list(self._views.items()) + deps if alias == new_alias]
        if len(collisions) > 0:
            raise ValueError(f"Duplicate alias {new_alias}")
