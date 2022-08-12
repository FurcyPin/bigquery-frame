from typing import List

from bigquery_frame import DataFrame
from bigquery_frame.utils import quote


def union_dataframes(dfs: List[DataFrame]) -> DataFrame:
    """Returns the union between multiple DataFrames"""
    if len(dfs) == 0:
        raise ValueError("input list is empty")
    query = "\nUNION ALL\n".join([f"  SELECT * FROM {quote(df._alias)}" for df in dfs])
    return DataFrame(query, alias=None, bigquery=dfs[0].bigquery, deps=dfs)
