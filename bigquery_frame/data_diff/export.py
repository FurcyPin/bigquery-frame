import tempfile
from pathlib import Path
from typing import Optional

import bigquery_frame
from bigquery_frame.data_diff.diff_result_summary import DiffResultSummary

DEFAULT_HTML_REPORT_OUTPUT_FILE_PATH = "diff_report.html"
DEFAULT_HTML_REPORT_ENCODING = "utf-8"


def export_html_diff_report(
    diff_result_summary: DiffResultSummary,
    title: Optional[str] = None,
    output_file_path: str = DEFAULT_HTML_REPORT_OUTPUT_FILE_PATH,
    encoding: str = DEFAULT_HTML_REPORT_ENCODING,
) -> None:
    from data_diff_viewer import DiffSummary, generate_report_string

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)
        diff_per_col_parquet_path = temp_dir_path / "diff_per_col.parquet"
        diff_result_summary.diff_per_col_df.toPandas().to_parquet(diff_per_col_parquet_path)
        if title is None:
            report_title = f"{diff_result_summary.left_df_alias} vs {diff_result_summary.right_df_alias}"
        else:
            report_title = title
        column_names_diff = {k: v.value for k, v in diff_result_summary.schema_diff_result.column_names_diff.items()}
        diff_summary = DiffSummary(
            generated_with=f"{bigquery_frame.__name__}:{bigquery_frame.__version__}",
            left_df_alias=diff_result_summary.left_df_alias,
            right_df_alias=diff_result_summary.right_df_alias,
            join_cols=diff_result_summary.join_cols,
            same_schema=diff_result_summary.same_schema,
            schema_diff_str=diff_result_summary.schema_diff_result.diff_str,
            column_names_diff=column_names_diff,
            same_data=diff_result_summary.same_data,
            total_nb_rows=diff_result_summary.total_nb_rows,
        )
        report = generate_report_string(report_title, diff_summary, temp_dir_path, diff_per_col_parquet_path)
        output_path = Path(output_file_path)
        with output_path.open("w", encoding=encoding) as output:
            output.write(report)
        print(f"Report exported as {output_file_path}")
