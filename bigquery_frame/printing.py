from google.cloud.bigquery.table import RowIterator
from tabulate import tabulate


def print_results(it: RowIterator, format_args: dict = None):
    if format_args is None:
        format_args = {
            "tablefmt": "pretty",
            "missingval": "null"
        }
    headers = {field.name: field.name for field in it.schema}
    rows = list(it)
    print(tabulate(rows, headers=headers, **format_args))

