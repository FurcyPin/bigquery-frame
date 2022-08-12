from google.cloud.bigquery.table import RowIterator
from tabulate import tabulate


def print_results(it: RowIterator, format_args: dict = None, limit=None):
    if format_args is None:
        format_args = {
            "tablefmt": "pretty",
            "missingval": "null",
            "stralign": "right",
        }
    headers = {field.name: field.name for field in it.schema}
    rows = list(it)
    print(tabulate(rows[0:limit], headers=headers, **format_args))
    if len(rows) > limit:
        plural = "s" if limit > 1 else ""
        print(f"only showing top {limit} row{plural}")
