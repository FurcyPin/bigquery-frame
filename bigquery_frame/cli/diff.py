import sys
from argparse import ArgumentParser
from typing import List

from bigquery_frame import BigQueryBuilder
from bigquery_frame.data_diff import DataframeComparator


def main(argv: List[str] = None):
    if argv is None:
        argv = sys.argv[1:]

    parser = ArgumentParser(description="", prog="bq-diff")
    parser.add_argument("--tables", nargs=2, type=str)
    parser.add_argument("--join-cols", nargs="*", default=None, type=str)
    args = parser.parse_args(argv)
    bq = BigQueryBuilder()
    df_comparator = DataframeComparator()

    left_table, right_table = args.tables
    left_df = bq.table(left_table)
    right_df = bq.table(right_table)
    diff_result = df_comparator.compare_df(left_df, right_df, args.join_cols)
    diff_result.display()
