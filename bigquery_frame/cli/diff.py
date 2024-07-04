import sys
from argparse import ArgumentParser

from bigquery_frame import BigQueryBuilder
from bigquery_frame.data_diff import compare_dataframes
from bigquery_frame.data_diff.diff_format_options import DiffFormatOptions, DEFAULT_NB_DIFFED_ROWS
from bigquery_frame.data_diff.export import DEFAULT_HTML_REPORT_OUTPUT_FILE_PATH


def main(argv: list[str] = None):
    if argv is None:
        argv = sys.argv[1:]
    if len(argv) == 0:
        argv = ["--help"]
    parser = ArgumentParser(description="Compare two BigQuery Tables and generate a HTML report", prog="bq-diff")
    parser.add_argument(
        "--tables",
        nargs=2,
        metavar=("LEFT_TABLE", "RIGHT_TABLE"),
        type=str,
        help="Fully qualified names of the two tables to compare",
    )
    parser.add_argument(
        "--join-cols",
        nargs="*",
        default=None,
        type=str,
        help="Name of the fields used to join the DataFrames together. "
        "Each row should be uniquely identifiable using these fields. "
        "Fields inside repeated structs are also supported.",
    )
    parser.add_argument(
        "--output",
        default=None,
        type=str,
        help="Path of the HTML report to generate.",
    )
    parser.add_argument(
        "--nb-top-values",
        default=DEFAULT_NB_DIFFED_ROWS,
        type=int,
        help="Number of most frequent change/values to display in the diff for each column "
        f"(Default: {DEFAULT_NB_DIFFED_ROWS}).",
    )
    args = parser.parse_args(argv)
    left_table, right_table = args.tables
    bq = BigQueryBuilder()
    left_df = bq.table(left_table)
    right_df = bq.table(right_table)
    diff_result = compare_dataframes(left_df, right_df, args.join_cols)
    if args.output is not None:
        output_path = args.output
    else:
        output_path = DEFAULT_HTML_REPORT_OUTPUT_FILE_PATH
    diff_format_options = DiffFormatOptions(nb_diffed_rows=args.nb_top_values)

    diff_result.export_to_html(output_file_path=output_path, diff_format_options=diff_format_options)
