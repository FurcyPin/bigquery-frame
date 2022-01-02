from functools import reduce
from typing import List

from bigquery_frame import DataFrame


def union_dataframes(dfs: List[DataFrame]) -> DataFrame:
    """Returns the union between multiple DataFrames"""
    if len(dfs) == 0:
        raise ValueError("input list is empty")
    return reduce(lambda a, b: a.union(b), dfs)

