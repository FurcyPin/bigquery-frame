from typing import Dict, Generator, List, Optional, Tuple, TypeVar, Union

from bigquery_frame import Column, DataFrame
from bigquery_frame import functions as f
from bigquery_frame import nested
from bigquery_frame.column import cols_to_str
from bigquery_frame.conf import STRUCT_SEPARATOR_REPLACEMENT
from bigquery_frame.data_diff.diff_result import DiffResult
from bigquery_frame.data_diff.package import EXISTS_COL_NAME, IS_EQUAL_COL_NAME, canonize_col
from bigquery_frame.data_diff.schema_diff import DiffPrefix, SchemaDiffResult, diff_dataframe_schemas
from bigquery_frame.data_type_utils import flatten_schema, get_common_columns
from bigquery_frame.dataframe import is_repeated
from bigquery_frame.exceptions import AnalysisException, CombinatorialExplosionError, DataframeComparatorException
from bigquery_frame.nested_impl.package import unnest_fields, validate_fields_exist
from bigquery_frame.special_characters import _replace_special_characters, _restore_special_characters
from bigquery_frame.transformations import flatten, harmonize_dataframes, sort_all_arrays
from bigquery_frame.utils import quote, quote_columns, str_to_cols, strip_margin

A = TypeVar("A")


def _shard_column_dict_but_keep_arrays_grouped(
    columns: Dict[str, A], max_number_of_col_per_shard: int
) -> List[Dict[str, A]]:
    """Separate the specified columns into shards, but make sure that arrays are kept together.

    >>> cols = {"a": None, "b": None, "s!.a": None, "s!.b": None, "s!.c": None, "c": None, "d": None}
    >>> list(_shard_column_dict_but_keep_arrays_grouped(cols, 3))
    [{'a': None, 'b': None}, {'s!.a': None, 's!.b': None, 's!.c': None, 'c': None}, {'d': None}]
    >>> list(_shard_column_dict_but_keep_arrays_grouped({},3))
    [{}]

    """
    if len(columns) == 0:
        yield {}
    res: Dict[str, A] = {}
    group: Dict[str, A] = {}
    last_col_group = None

    for col, tpe in columns.items():
        col_group = col.split("!.")[0]
        if col_group != last_col_group:
            if len(res) > 0 and len(res) + len(group) >= max_number_of_col_per_shard:
                yield res
                res = {}
            else:
                res = {**res, **group}
                group = {}
        last_col_group = col_group
        group[col] = tpe
        if len(res) >= max_number_of_col_per_shard:
            yield res
            res = {}
    if len(res) + len(group) < max_number_of_col_per_shard:
        res = {**res, **group}
        group = {}
    if len(res) > 0:
        yield res
    if len(group) > 0:
        yield group


def _deduplicate_list_while_conserving_ordering(a_list: List[A]) -> List[A]:
    """Deduplicate a list while conserving its ordering.
    The implementation uses the fact that unlike sets, dict keys preserve ordering.

    Args:
        a_list: A list

    Returns: A deduplicated list

    Examples:
        >>> _deduplicate_list_while_conserving_ordering([1, 3, 2, 1, 2, 3, 1, 4])
        [1, 3, 2, 4]

    """
    return list({elem: 0 for elem in a_list}.keys())


def _get_common_root_column_names(common_fields: Dict[str, Optional[str]]) -> List[str]:
    """Given common_columns, compute the ordered list of names of common root columns.

    Args:
        common_fields: the list of common fields

    Returns:

    Examples:
        >>> _get_common_root_column_names({"id": None, "array!.c1": None, "array!.c2": None})
        ['id', 'array']

    """
    root_columns = [col.split("!")[0] for col in common_fields]
    return _deduplicate_list_while_conserving_ordering(root_columns)


def _get_self_join_growth_estimate(df: DataFrame, cols: Union[str, List[str]]) -> float:
    """Computes how many times bigger a DataFrame will be if we self-join it using the provided columns, rounded
    to 2 decimals

    Args:
        df: A DataFrame
        cols: A list of column names

    Returns:
        The estimated ratio of duplicates

    Examples:
        If a DataFrame with 6 rows has one value present on 2 rows and another value present on 3 rows,
        the growth factor will be (1*1 + 2*2 + 3*3) / 6 ~= 2.33.
        If a column unique on each row, it's number of duplicates will be 0.

        >>> from bigquery_frame import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as name),
        ...     STRUCT(2 as id, "b" as name),
        ...     STRUCT(3 as id, "b" as name),
        ...     STRUCT(4 as id, "c" as name),
        ...     STRUCT(5 as id, "c" as name),
        ...     STRUCT(6 as id, "c" as name)
        ... ])''')
        >>> _get_self_join_growth_estimate(df, "id")
        1.0
        >>> _get_self_join_growth_estimate(df, "name")
        2.33

    Tests:
        It should work with NULL values too.

        >>> df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as name),
        ...     STRUCT(2 as id, "b" as name),
        ...     STRUCT(3 as id, "b" as name),
        ...     STRUCT(4 as id, NULL as name),
        ...     STRUCT(5 as id, NULL as name),
        ...     STRUCT(NULL as id, NULL as name)
        ... ])''')
        >>> _get_self_join_growth_estimate(df, "id")
        1.0
        >>> _get_self_join_growth_estimate(df, "name")
        2.33

    """
    if isinstance(cols, str):
        cols = [cols]

    df1 = df.groupBy(*quote_columns(cols)).agg(f.count(f.lit(1)).alias("nb"))
    df2 = df1.agg(
        f.sum(f.col("nb")).alias("nb_rows"),
        f.sum(f.col("nb") * f.col("nb")).alias("nb_rows_after_self_join"),
    )
    res = df2.take(1)[0]
    nb_rows = res.get("nb_rows")
    nb_rows_after_self_join = res.get("nb_rows_after_self_join")
    if nb_rows_after_self_join is None:
        nb_rows_after_self_join = 0
    if nb_rows is None or nb_rows == 0:
        return 1.0
    else:
        return round(nb_rows_after_self_join * 1.0 / nb_rows, 2)


def _get_eligible_columns_for_join(df: DataFrame) -> Dict[str, float]:
    """Identifies the column with the least duplicates, in order to use it as the id for the comparison join.

    Eligible columns are all columns of type String, Int or Bigint that have an approximate distinct count of 90%
    of the number of rows in the DataFrame. Returns None if no such column is found.

    Args:
        df: a DataFrame

    Returns:
        The name of the columns with less than 10% duplicates, and their
        corresponding self-join-growth-estimate

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as name),
        ...     STRUCT(2 as id, "b" as name),
        ...     STRUCT(3 as id, "b" as name)
        ... ])''')
        >>> _get_eligible_columns_for_join(df)
        {'id': 1.0}
        >>> df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as name),
        ...     STRUCT(1 as id, "a" as name)
        ... ])''')
        >>> _get_eligible_columns_for_join(df)
        {}
    """
    eligible_cols = [
        col.name for col in df.schema if col.field_type in ["STRING", "INTEGER", "FLOAT"] and not is_repeated(col)
    ]
    if len(eligible_cols) == 0:
        return dict()
    distinct_count_threshold = f.lit(90.0)
    eligibility_df = df.select(
        [
            (
                f.when(f.count(f.lit(1)) == f.lit(0), f.lit(False)).otherwise(
                    f.approx_count_distinct(quote(col)) * f.lit(100.0) / f.count(f.lit(1)) > distinct_count_threshold
                )
            ).alias(col)
            for col in eligible_cols
        ]
    )
    columns_with_high_distinct_count = [key for key, value in eligibility_df.collect()[0].items() if value]
    cols_with_duplicates = {col: _get_self_join_growth_estimate(df, col) for col in columns_with_high_distinct_count}
    return cols_with_duplicates


def _merge_growth_estimate_dicts(left_dict: Dict[str, float], right_dict: Dict[str, float]):
    """Merge together two dicts giving for each column name the corresponding growth_estimate

    >>> _merge_growth_estimate_dicts({"a": 10.0, "b": 1.0}, {"a": 1.0, "c": 1.0})
    {'a': 5.5, 'b': 1.0, 'c': 1.0}
    """
    res = left_dict.copy()
    for x in right_dict:
        if x in left_dict:
            res[x] = (res[x] + right_dict[x]) / 2
        else:
            res[x] = right_dict[x]
    return res


def _automatically_infer_join_col(left_df: DataFrame, right_df: DataFrame) -> Tuple[Optional[str], Optional[float]]:
    """Identify the column with the least duplicates, in order to use it as the id for the comparison join.

    Eligible columns are all columns of type String, Int or Bigint that have an approximate distinct count of 90%
    of the number of rows in the DataFrame. Returns None if no suche column is found.

    >>> from bigquery_frame import BigQueryBuilder
    >>> bq = BigQueryBuilder()
    >>> left_df = bq.sql('''SELECT * FROM UNNEST([
    ...     STRUCT(1 as id, "a" as name),
    ...     STRUCT(2 as id, "b" as name),
    ...     STRUCT(3 as id, "c" as name),
    ...     STRUCT(4 as id, "d" as name),
    ...     STRUCT(5 as id, "e" as name),
    ...     STRUCT(6 as id, "f" as name)
    ... ])''')
    >>> right_df = bq.sql('''SELECT * FROM UNNEST([
    ...     STRUCT(1 as id, "a" as name),
    ...     STRUCT(2 as id, "a" as name),
    ...     STRUCT(3 as id, "b" as name),
    ...     STRUCT(4 as id, "c" as name),
    ...     STRUCT(5 as id, "d" as name),
    ...     STRUCT(6 as id, "e" as name)
    ... ])''')
    >>> _automatically_infer_join_col(left_df, right_df)
    ('id', 1.0)
    >>> left_df = bq.sql('''SELECT * FROM UNNEST([
    ...     STRUCT(1 as id, "a" as name),
    ...     STRUCT(1 as id, "a" as name)
    ... ])''')
    >>> right_df = bq.sql('''SELECT * FROM UNNEST([
    ...     STRUCT(1 as id, "a" as name),
    ...     STRUCT(1 as id, "a" as name)
    ... ])''')
    >>> _automatically_infer_join_col(left_df, right_df)
    (None, None)

    :param left_df: a DataFrame
    :param right_df: a DataFrame
    :return: The name of the column with the least duplicates in both DataFrames if it has less than 10% duplicates.
    """
    left_col_dict = _get_eligible_columns_for_join(left_df)
    right_col_dict = _get_eligible_columns_for_join(right_df)
    merged_col_dict = _merge_growth_estimate_dicts(left_col_dict, right_col_dict)

    if len(merged_col_dict) > 0:
        col, self_join_growth_estimate = sorted(
            merged_col_dict.items(),
            key=lambda x: x[1],
        )[0]
        return col, self_join_growth_estimate
    else:
        return None, None


def _get_join_cols(left_df: DataFrame, right_df: DataFrame, join_cols: Optional[List[str]]) -> Tuple[List[str], float]:
    """Performs an in-depth analysis between two DataFrames with the same columns and prints the differences found.
    We first attempt to identify columns that look like ids.
    For that we choose all the columns with an approximate_count_distinct greater than 90% of the row count.
    For each column selected this way, we then perform a join and compare the DataFrames column by column.

    :param left_df: a DataFrame
    :param right_df: another DataFrame with the same columns
    :param join_cols: the list of columns on which to perform the join
    :return: a Dict that gives for each eligible join column the corresponding diff DataFrame
    """
    if join_cols is None:
        print(
            "No join_cols provided: "
            "trying to automatically infer a column that can be used for joining the two DataFrames",
        )
        inferred_join_col, self_join_growth_estimate = _automatically_infer_join_col(left_df, right_df)
        if inferred_join_col is None or self_join_growth_estimate is None:
            error_message = (
                "Could not automatically infer a column sufficiently "
                "unique to join the two DataFrames and perform a comparison. "
                "Please specify manually the columns to use with the join_cols parameter"
            )
            raise DataframeComparatorException(error_message)
        else:
            print(f"Found the following column: {inferred_join_col}")
            join_cols = [inferred_join_col]
    else:
        self_join_growth_estimate = (
            _get_self_join_growth_estimate(left_df, join_cols) + _get_self_join_growth_estimate(right_df, join_cols)
        ) / 2
    return join_cols, self_join_growth_estimate


def _check_join_cols(
    specified_join_cols: Optional[List[str]], join_cols: List[str], self_join_growth_estimate: float
) -> None:
    """Check the self_join_growth_estimate and raise an Exception if it is bigger than 2.

    This security helps to prevent users from accidentally spending huge query costs.
    Example: if a table has 10^9 rows and the join_col has a value with 10^6 duplicates, then the resulting
    self join will have (10^6)^2=10^12 which is 1000 times bigger than the original table.

    """
    inferred_provided_str = "provided"
    if specified_join_cols is None:
        inferred_provided_str = "inferred"
    if len(join_cols) == 1:
        plural_str = ""
        join_cols_str = str(_restore_special_characters(join_cols[0]))
    else:
        plural_str = "s"
        join_cols_str = str([_restore_special_characters(join_col) for join_col in join_cols])

    if self_join_growth_estimate >= 2.0:
        raise CombinatorialExplosionError(
            f"Performing a join with the {inferred_provided_str} column{plural_str} {join_cols_str} "
            f"would increase the size of the table by a factor of {self_join_growth_estimate}. "
            f"Please provide join_cols that are truly unique for both DataFrames."
        )
    print(
        f"Generating the diff by joining the DataFrames together "
        f"using the {inferred_provided_str} column{plural_str}: {join_cols_str}"
    )
    if self_join_growth_estimate > 1.0:
        print(
            f"WARNING: duplicates have been detected in the joining key, the resulting DataFrame "
            f"will be {self_join_growth_estimate} bigger which might affect the diff results. "
            f"Please consider providing join_cols that are truly unique for both DataFrames."
        )


default_values_per_type = {
    "STRING": '""',
    "BOOL": "FALSE",
    "INTEGER": "0",
    "NUMERIC": "0",
    "BIGNUMERIC": "0",
    "FLOAT": "0",
    "DATE": '"0001-01-01"',
    "TIME": '"00:00:00"',
    "DATETIME": '"0001-01-01"',
    "TIMESTAMP": '"0001-01-01"',
    "BYTES": 'b""',
    "GEOGRAPHY": 'ST_GEOGFROMTEXT("POINT(0 0)")',
}


def _build_null_safe_join_clause(
    left_df: DataFrame,
    right_df: DataFrame,
    join_cols: List[str],
) -> Column:
    """Generates a join clause that matches NULL values for the given join_cols

    BigQuery does not have a null-safe equality operator, and a join clause
    "T1.x=T2.x OR (T1.x IS NULL AND T2.x IS NULL)" does not work for all data types.

    A safe way is to build a "STRUCT(COALESCE(x, default_value_for_type(x)), x IS NULL)"
    """
    join_cols_with_type = {
        col: next(field.field_type for field in left_df.schema if field.name == col) for col in join_cols
    }

    def safe_struct_for_single_col(col: Column, col_type: str):
        if col_type not in default_values_per_type:
            raise AnalysisException(f"Unsupported type for join_col {col}: {col_type}")
        return f.struct(f.coalesce(col, f.expr(default_values_per_type[col_type])), col.isNull())

    def join_clause_for_single_column(column: str) -> Column:
        col_type = join_cols_with_type[column]
        left_struct = safe_struct_for_single_col(left_df[column], col_type)
        right_struct = safe_struct_for_single_col(right_df[column], col_type)
        return left_struct == right_struct

    first_column: str = join_cols[0]
    join_clause: Column = join_clause_for_single_column(first_column)
    for col in join_cols[1:]:
        join_clause &= join_clause_for_single_column(col)
    return join_clause


def _build_diff_dataframe(
    left_df: DataFrame,
    right_df: DataFrame,
    column_names_diff: Dict[str, DiffPrefix],
    join_cols: List[str],
) -> DataFrame:
    """Perform a column-by-column comparison between two DataFrames.
    The two DataFrames must have the same columns with the same ordering.
    The column `join_col` will be used to join the two DataFrames together.
    Then we build a new DataFrame with the `join_col` and for each column, a struct with three elements:
    - `left_value`: the value coming from the `left_df`
    - `right_value`: the value coming from the `right_df`
    - `is_equal`: True if both values have the same hash, False otherwise.

    Args:
        left_df: A DataFrame
        right_df: Another DataFrame
        join_cols: The names of the columns to use to perform the join.

    Returns:
        A DataFrame containing all the columns that differ, and a dictionary that gives the number of
        differing rows for each column

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> left_df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as c1, 1 as c2, 2 as c3),
        ...     STRUCT(2 as id, "b" as c1, 2 as c2, 3 as c3),
        ...     STRUCT(3 as id, "c" as c1, 3 as c2, 4 as c3)
        ... ])''')
        >>> right_df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as c1, 1 as c2, 3 as c4),
        ...     STRUCT(2 as id, "b" as c1, 4 as c2, 4 as c4),
        ...     STRUCT(4 as id, "f" as c1, 3 as c2, 5 as c4)
        ... ])''')
        >>> left_df.show()
        +----+----+----+----+
        | id | c1 | c2 | c3 |
        +----+----+----+----+
        |  1 |  a |  1 |  2 |
        |  2 |  b |  2 |  3 |
        |  3 |  c |  3 |  4 |
        +----+----+----+----+
        >>> right_df.show()
        +----+----+----+----+
        | id | c1 | c2 | c4 |
        +----+----+----+----+
        |  1 |  a |  1 |  3 |
        |  2 |  b |  4 |  4 |
        |  4 |  f |  3 |  5 |
        +----+----+----+----+
        >>> from bigquery_frame.data_diff.schema_diff import _diff_dataframe_column_names
        >>> column_names_diff = _diff_dataframe_column_names(left_df.columns, right_df.columns)
        >>> column_names_diff
        {'id': ' ', 'c1': ' ', 'c2': ' ', 'c3': '-', 'c4': '+'}
        >>> (_build_diff_dataframe(left_df, right_df, column_names_diff, ['id'])
        ...     .withColumn('coalesced_id', f.expr('coalesce(id.left_value, id.right_value)'))
        ...     .orderBy('coalesced_id').drop('coalesced_id').show()
        ... ) # noqa: E501
        +-------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+--------------------------------------------+--------------+
        |                                                                                                    id |                                                                                                      c1 |                                                                                                    c2 |                                                                                                        c3 |                                                                                                        c4 |                                 __EXISTS__ | __IS_EQUAL__ |
        +-------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+--------------------------------------------+--------------+
        |      {'left_value': 1, 'right_value': 1, 'is_equal': True, 'exists_left': True, 'exists_right': True} |    {'left_value': 'a', 'right_value': 'a', 'is_equal': True, 'exists_left': True, 'exists_right': True} |      {'left_value': 1, 'right_value': 1, 'is_equal': True, 'exists_left': True, 'exists_right': True} |     {'left_value': 2, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} |     {'left_value': None, 'right_value': 3, 'is_equal': False, 'exists_left': False, 'exists_right': True} |  {'left_value': True, 'right_value': True} |         True |
        |      {'left_value': 2, 'right_value': 2, 'is_equal': True, 'exists_left': True, 'exists_right': True} |    {'left_value': 'b', 'right_value': 'b', 'is_equal': True, 'exists_left': True, 'exists_right': True} |     {'left_value': 2, 'right_value': 4, 'is_equal': False, 'exists_left': True, 'exists_right': True} |     {'left_value': 3, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} |     {'left_value': None, 'right_value': 4, 'is_equal': False, 'exists_left': False, 'exists_right': True} |  {'left_value': True, 'right_value': True} |        False |
        | {'left_value': 3, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} | {'left_value': 'c', 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} | {'left_value': 3, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} |     {'left_value': 4, 'right_value': None, 'is_equal': False, 'exists_left': True, 'exists_right': False} | {'left_value': None, 'right_value': None, 'is_equal': False, 'exists_left': False, 'exists_right': False} | {'left_value': True, 'right_value': False} |        False |
        | {'left_value': None, 'right_value': 4, 'is_equal': False, 'exists_left': False, 'exists_right': True} | {'left_value': None, 'right_value': 'f', 'is_equal': False, 'exists_left': False, 'exists_right': True} | {'left_value': None, 'right_value': 3, 'is_equal': False, 'exists_left': False, 'exists_right': True} | {'left_value': None, 'right_value': None, 'is_equal': False, 'exists_left': False, 'exists_right': False} |     {'left_value': None, 'right_value': 5, 'is_equal': False, 'exists_left': False, 'exists_right': True} | {'left_value': False, 'right_value': True} |        False |
        +-------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------+--------------------------------------------+--------------+
    """  # noqa: E501
    column_names_diff = {col_name: diff_str for col_name, diff_str in column_names_diff.items()}

    left_df = left_df.withColumn(EXISTS_COL_NAME, f.lit(col=True))
    right_df = right_df.withColumn(EXISTS_COL_NAME, f.lit(col=True))

    null_safe_join_clause = _build_null_safe_join_clause(left_df, right_df, join_cols)
    diff = left_df.join(right_df, null_safe_join_clause, "full")

    left_df_fields = {field.name: field for field in left_df.schema}
    right_df_fields = {field.name: field for field in right_df.schema}

    def comparison_struct(col_name: str, diff_prefix: str) -> Column:
        if diff_prefix == DiffPrefix.ADDED:
            left_col = f.lit(None)
            left_col_str = left_col
            exists_left = f.lit(col=False)
        else:
            left_col = left_df[col_name]
            left_col_str = canonize_col(left_col, left_df_fields[col_name])
            exists_left = f.coalesce(left_df[EXISTS_COL_NAME], f.lit(col=False))

        if diff_prefix == DiffPrefix.REMOVED:
            right_col = f.lit(None)
            right_col_str = right_col
            exists_right = f.lit(col=False)
        else:
            right_col = right_df[col_name]
            right_col_str = canonize_col(right_col, right_df_fields[col_name])
            exists_right = f.coalesce(right_df[EXISTS_COL_NAME], f.lit(col=False))

        if diff_prefix == DiffPrefix.UNCHANGED:
            is_equal_col = (left_col_str.isNull() & right_col_str.isNull()) | (
                left_col_str.isNotNull() & right_col_str.isNotNull() & (left_col_str == right_col_str)
            )
        else:
            is_equal_col = f.lit(col=False)

        return f.struct(
            left_col.alias("left_value"),
            right_col.alias("right_value"),
            is_equal_col.alias("is_equal"),
            exists_left.alias("exists_left"),
            exists_right.alias("exists_right"),
        ).alias(col_name)

    diff_columns = [
        sub_field.name for field in diff.schema for sub_field in field.fields if sub_field.name != EXISTS_COL_NAME
    ]

    diff_df = diff.select(
        *[
            comparison_struct(col_name, diff_prefix)
            for col_name, diff_prefix in column_names_diff.items()
            if col_name in diff_columns
        ],
        f.struct(
            f.coalesce(left_df[EXISTS_COL_NAME], f.lit(col=False)).alias("left_value"),
            f.coalesce(right_df[EXISTS_COL_NAME], f.lit(col=False)).alias("right_value"),
        ).alias(EXISTS_COL_NAME),
    )

    row_is_equal = f.lit(col=True)
    for col_name, diff_prefix in column_names_diff.items():
        if diff_prefix == DiffPrefix.UNCHANGED and col_name in diff_df.columns:
            row_is_equal = row_is_equal & f.col(f"{col_name}.is_equal")
    return diff_df.withColumn(IS_EQUAL_COL_NAME, row_is_equal)


def _harmonize_and_normalize_dataframes(
    left_flat: DataFrame,
    right_flat: DataFrame,
    common_columns: Dict[str, Optional[str]],
    skip_make_dataframes_comparable: bool,
) -> Tuple[DataFrame, DataFrame]:
    if not skip_make_dataframes_comparable:
        left_flat, right_flat = harmonize_dataframes(
            left_flat,
            right_flat,
            common_columns=common_columns,
            keep_missing_columns=True,
        )
    left_flat = sort_all_arrays(left_flat)
    right_flat = sort_all_arrays(right_flat)
    return left_flat, right_flat


def _build_diff_dataframe_shards(
    left_df: DataFrame,
    right_df: DataFrame,
    schema_diff_result: SchemaDiffResult,
    join_cols: List[str],
    specified_join_cols: Optional[List[str]],
    max_number_of_col_per_shard: int,
) -> Dict[str, DataFrame]:
    left_fields_to_unnest = [
        col_name
        for col_name, diff_prefix in schema_diff_result.column_names_diff.items()
        if diff_prefix in [DiffPrefix.UNCHANGED, DiffPrefix.REMOVED]
    ]
    right_fields_to_unnest = [
        col_name
        for col_name, diff_prefix in schema_diff_result.column_names_diff.items()
        if diff_prefix in [DiffPrefix.UNCHANGED, DiffPrefix.ADDED]
    ]

    unnested_left_dfs = unnest_fields(
        left_df,
        left_fields_to_unnest,
        keep_fields=join_cols,
    )
    unnested_right_dfs = unnest_fields(
        right_df,
        right_fields_to_unnest,
        keep_fields=join_cols,
    )

    common_keys = sorted(
        set(unnested_left_dfs.keys()).intersection(set(unnested_right_dfs.keys())),
    )

    def build_shard(key: str) -> DataFrame:
        l_df = unnested_left_dfs[key]
        r_df = unnested_right_dfs[key]
        schema_diff_result = diff_dataframe_schemas(l_df, r_df, join_cols)
        new_join_cols = [_replace_special_characters(col) for col in join_cols]
        new_join_cols = [col for col in new_join_cols if col in l_df.columns]
        new_join_cols, self_join_growth_estimate = _get_join_cols(
            l_df,
            r_df,
            new_join_cols,
        )
        _check_join_cols(specified_join_cols, new_join_cols, self_join_growth_estimate)
        return _build_diff_dataframe_with_split(
            l_df, r_df, schema_diff_result, new_join_cols, max_number_of_col_per_shard
        )

    return {key: build_shard(key) for key in common_keys}


def _build_diff_dataframe_with_split(
    left_df: DataFrame,
    right_df: DataFrame,
    schema_diff_result: SchemaDiffResult,
    join_cols: List[str],
    max_number_of_col_per_shard: int,
) -> DataFrame:
    """This variant of _build_diff_dataframe splits the DataFrames into smaller shards that contains
    around `max_number_of_col_per_shard` columns, persists the results, then performs a union of the results.
    """
    left_schema_flat = flatten_schema(left_df.schema, explode=True)
    right_schema_flat = flatten_schema(right_df.schema, explode=True)
    common_columns = get_common_columns(left_schema_flat, right_schema_flat)
    join_columns = {col: tpe for col, tpe in common_columns.items() if col in join_cols}
    non_join_columns = {col: tpe for col, tpe in common_columns.items() if col not in join_cols}
    columns_shards = [
        {**join_columns, **shard}
        for shard in _shard_column_dict_but_keep_arrays_grouped(non_join_columns, max_number_of_col_per_shard)
    ]

    def build_shards(_left_df: DataFrame, _right_df: DataFrame) -> Generator[DataFrame, None, None]:
        from tqdm import tqdm

        for index, columns_shard in enumerate(tqdm(columns_shards)):
            l_df, r_df = _harmonize_and_normalize_dataframes(
                _left_df, _right_df, columns_shard, skip_make_dataframes_comparable=schema_diff_result.same_schema
            )
            diff_df = _build_diff_dataframe(
                l_df,
                r_df,
                schema_diff_result.column_names_diff,
                join_cols,
            )
            yield diff_df.persist()

    diff_df_shards = list(build_shards(left_df, right_df))
    return _join_dataframes(*diff_df_shards, join_cols=join_cols)


def _join_dataframes(*dfs: DataFrame, join_cols: List[str]) -> DataFrame:
    """Optimized method that joins multiple DataFrames in on single select statement.
    Ideally, the default :func:`DataFrame.join` should be optimized to do this directly.

    >>> from bigquery_frame import BigQueryBuilder
    >>> bq = BigQueryBuilder()  # noqa: E501
    >>> df_a = bq.sql('SELECT 1 as id, STRUCT(1 as left_value, 1 as right_value, TRUE as is_equal) as a, True as __EXISTS__, TRUE as __IS_EQUAL__')
    >>> df_b = bq.sql('SELECT 1 as id, STRUCT(1 as left_value, 1 as right_value, TRUE as is_equal) as b, True as __EXISTS__, TRUE as __IS_EQUAL__')
    >>> df_c = bq.sql('SELECT 1 as id, STRUCT(1 as left_value, 1 as right_value, TRUE as is_equal) as c, True as __EXISTS__, TRUE as __IS_EQUAL__')
    >>> _join_dataframes(df_a, df_b, df_c, join_cols=['id']).show()
    +----+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+------------+--------------+
    | id |                                                     a |                                                     b |                                                     c | __EXISTS__ | __IS_EQUAL__ |
    +----+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+------------+--------------+
    |  1 | {'left_value': 1, 'right_value': 1, 'is_equal': True} | {'left_value': 1, 'right_value': 1, 'is_equal': True} | {'left_value': 1, 'right_value': 1, 'is_equal': True} |       True |         True |
    +----+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+------------+--------------+

    """
    if len(dfs) == 1:
        return dfs[0]
    first_df = dfs[0]
    other_dfs = dfs[1:]
    excluded_common_cols = join_cols + [EXISTS_COL_NAME, IS_EQUAL_COL_NAME]
    is_equal = f.lit(True)
    for df in dfs:
        is_equal = is_equal & df[IS_EQUAL_COL_NAME]
    selected_columns = (
        str_to_cols(join_cols)
        + [f.expr(f"{df._alias}.* EXCEPT ({cols_to_str(excluded_common_cols)})") for df in dfs]
        + [first_df[EXISTS_COL_NAME], is_equal.alias(IS_EQUAL_COL_NAME)]
    )
    on_str = ", ".join(join_cols)
    join_str = "\nJOIN ".join([f"{quote(df._alias)} USING ({on_str})" for df in other_dfs])

    query = strip_margin(
        f"""
        |SELECT
        |{cols_to_str(selected_columns, 2)}
        |FROM {quote(first_df._alias)}
        |JOIN {join_str}"""
    )
    return first_df._apply_query(query, deps=[first_df, *other_dfs])


def compare_dataframes(
    left_df: DataFrame,
    right_df: DataFrame,
    join_cols: Optional[List[str]] = None,
    _max_number_of_col_per_shard: int = 200,
) -> DiffResult:
    """Compares two DataFrames and print out the differences.

    We first compare the DataFrame schemas. If the schemas are different, we adapt the DataFrames to make them
    as much comparable as possible:
    - If the column ordering changed, we re-order them
    - If a column type changed, we cast the column to the smallest common type
    - If a column was added, removed or renamed, it will be ignored.

    If `join_cols` is specified, we will use the specified columns to perform the comparison join between the
    two DataFrames. Ideally, the `join_cols` should respect an unicity constraint.
    If they contain duplicates, a safety check is performed to prevent a potential combinatorial explosion:
    if the number of rows in the joined DataFrame would be more than twice the size of the original DataFrames,
    then an Exception is raised and the user will be asked to provide another set of `join_cols`.

    If no `join_cols` is specified, the algorithm will try to automatically find a single column suitable for
    the join. However, the automatic inference can only find join keys based on a single column.
    If the DataFrame's unique keys are composite (multiple columns) they must be given explicitly via `join_cols`
    to perform the diff analysis.

    !!! Tips:

        - If you want to test a column renaming, you can temporarily add renaming step to the DataFrame
          you want to test.
        - When comparing arrays, this algorithm ignores their ordering (e.g. `[1, 2, 3] == [3, 2, 1]`).
        - When dealing with nested structure, if the struct
          contains a unique identifier, it can be specified in the join_cols and the structure will be automatically
          unnested in the diff results. For instance, if we have a structure `my_array: ARRAY<STRUCT<a, b, ...>>`
          and if `a` is a unique identifier, then you can add `"my_array!.a"` in the join_cols argument.
          (C.F. Example 2)


    Args:
        left_df: A DataFrame
        right_df: Another DataFrame
        join_cols: Specifies the columns on which the two DataFrames should be joined to compare them
        _max_number_of_col_per_shard: When comparing tables with a large number of columns, DataFrames are split
          into smaller shards containing at most that number of columns, which are persisted individually.
          This optimization can be used to work around errors returned by BigQuery complaining about queries
          being too large. If you encounter such errors try decreasing the value of this parameter.

    Returns:
        A DiffResult object

    Examples:
        **Example 1: simple diff**

        >>> from bigquery_frame.data_diff.compare_dataframes_impl import __get_test_dfs
        >>> from bigquery_frame.data_diff import compare_dataframes
        >>> df1, df2 = __get_test_dfs()
        >>> df1.show()
        +----+----------------------------+
        | id |                   my_array |
        +----+----------------------------+
        |  1 | [{'a': 1, 'b': 2, 'c': 3}] |
        |  2 | [{'a': 1, 'b': 2, 'c': 3}] |
        |  3 | [{'a': 1, 'b': 2, 'c': 3}] |
        +----+----------------------------+

        >>> df2.show()
        +----+------------------------------------+
        | id |                           my_array |
        +----+------------------------------------+
        |  1 | [{'a': 1, 'b': 2, 'c': 3, 'd': 4}] |
        |  2 | [{'a': 2, 'b': 2, 'c': 3, 'd': 4}] |
        |  4 | [{'a': 1, 'b': 2, 'c': 3, 'd': 4}] |
        +----+------------------------------------+

        >>> diff_result = compare_dataframes(df1, df2)
        <BLANKLINE>
        Analyzing differences...
        No join_cols provided: trying to automatically infer a column that can be used for joining the two DataFrames
        Found the following column: id
        Generating the diff by joining the DataFrames together using the inferred column: id
        >>> diff_result.display()
        Schema has changed:
        @@ -1,2 +1,2 @@
        <BLANKLINE>
         id INTEGER
        -my_array ARRAY<STRUCT<a:INTEGER,b:INTEGER,c:INTEGER>>
        +my_array ARRAY<STRUCT<a:INTEGER,b:INTEGER,c:INTEGER,d:INTEGER>>
        WARNING: columns that do not match both sides will be ignored
        <BLANKLINE>
        diff NOT ok
        <BLANKLINE>
        Row count ok: 3 rows
        <BLANKLINE>
        0 (0.0%) rows are identical
        2 (50.0%) rows have changed
        1 (25.0%) rows are only in 'left'
        1 (25.0%) rows are only in 'right
        <BLANKLINE>
        Found the following changes:
        +-------------+---------------+-----------------------+-----------------------------+----------------+
        | column_name | total_nb_diff |            left_value |                 right_value | nb_differences |
        +-------------+---------------+-----------------------+-----------------------------+----------------+
        |    my_array |             2 | [{"a":1,"b":2,"c":3}] | [{"a":2,"b":2,"c":3,"d":4}] |              1 |
        |    my_array |             2 | [{"a":1,"b":2,"c":3}] | [{"a":1,"b":2,"c":3,"d":4}] |              1 |
        +-------------+---------------+-----------------------+-----------------------------+----------------+
        1 rows were only found in 'left' :
        Most frequent values in 'left' for each column :
        +-------------+-----------------------+----+
        | column_name |                 value | nb |
        +-------------+-----------------------+----+
        |          id |                     3 |  1 |
        |    my_array | [{"a":1,"b":2,"c":3}] |  1 |
        +-------------+-----------------------+----+
        1 rows were only found in 'right' :
        Most frequent values in 'right' for each column :
        +-------------+-----------------------------+----+
        | column_name |                       value | nb |
        +-------------+-----------------------------+----+
        |          id |                           4 |  1 |
        |    my_array | [{"a":1,"b":2,"c":3,"d":4}] |  1 |
        +-------------+-----------------------------+----+

        **Example 2: by adding `"my_array!.a"` to the join_cols argument, the array gets unnested for the diff **

        >>> diff_result_unnested = compare_dataframes(df1, df2, join_cols=["id", "my_array!.a"])
        <BLANKLINE>
        Analyzing differences...
        Generating the diff by joining the DataFrames together using the provided column: id
        Generating the diff by joining the DataFrames together using the provided columns: ['id', 'my_array!.a']
        >>> diff_result_unnested.display()
        Schema has changed:
        @@ -1,4 +1,5 @@
        <BLANKLINE>
         id INTEGER
         my_array!.a INTEGER
         my_array!.b INTEGER
         my_array!.c INTEGER
        +my_array!.d INTEGER
        WARNING: columns that do not match both sides will be ignored
        <BLANKLINE>
        diff NOT ok
        <BLANKLINE>
        WARNING: This diff has multiple granularity levels, we will print the results for each granularity level,
                 but we recommend to export the results to html for a much more digest result.
        <BLANKLINE>
        ##############################################################
        Granularity : root (4 rows)
        <BLANKLINE>
        Row count ok: 3 rows
        <BLANKLINE>
        2 (50.0%) rows are identical
        0 (0.0%) rows have changed
        1 (25.0%) rows are only in 'left'
        1 (25.0%) rows are only in 'right
        <BLANKLINE>
        1 rows were only found in 'left' :
        Most frequent values in 'left' for each column :
        +-------------+-------+----+
        | column_name | value | nb |
        +-------------+-------+----+
        |          id |     3 |  1 |
        | my_array!.a |     1 |  2 |
        | my_array!.b |     2 |  2 |
        | my_array!.c |     3 |  2 |
        +-------------+-------+----+
        1 rows were only found in 'right' :
        Most frequent values in 'right' for each column :
        +-------------+-------+----+
        | column_name | value | nb |
        +-------------+-------+----+
        |          id |     4 |  1 |
        | my_array!.a |     1 |  1 |
        | my_array!.a |     2 |  1 |
        | my_array!.b |     2 |  2 |
        | my_array!.c |     3 |  2 |
        | my_array!.d |     4 |  3 |
        +-------------+-------+----+
        ##############################################################
        Granularity : my_array (5 rows)
        <BLANKLINE>
        Row count ok: 3 rows
        <BLANKLINE>
        1 (20.0%) rows are identical
        0 (0.0%) rows have changed
        2 (40.0%) rows are only in 'left'
        2 (40.0%) rows are only in 'right
        <BLANKLINE>
        2 rows were only found in 'left' :
        Most frequent values in 'left' for each column :
        +-------------+-------+----+
        | column_name | value | nb |
        +-------------+-------+----+
        |          id |     3 |  1 |
        | my_array!.a |     1 |  2 |
        | my_array!.b |     2 |  2 |
        | my_array!.c |     3 |  2 |
        +-------------+-------+----+
        2 rows were only found in 'right' :
        Most frequent values in 'right' for each column :
        +-------------+-------+----+
        | column_name | value | nb |
        +-------------+-------+----+
        |          id |     4 |  1 |
        | my_array!.a |     1 |  1 |
        | my_array!.a |     2 |  1 |
        | my_array!.b |     2 |  2 |
        | my_array!.c |     3 |  2 |
        | my_array!.d |     4 |  3 |
        +-------------+-------+----+
    """
    print("\nAnalyzing differences...")

    if join_cols == []:
        join_cols = None
    specified_join_cols = join_cols

    if join_cols is None:
        left_flat = flatten(left_df, struct_separator=STRUCT_SEPARATOR_REPLACEMENT)
        right_flat = flatten(right_df, struct_separator=STRUCT_SEPARATOR_REPLACEMENT)
        join_cols, self_join_growth_estimate = _get_join_cols(
            left_flat,
            right_flat,
            join_cols,
        )
    else:
        validate_fields_exist(join_cols, nested.fields(left_df))
        validate_fields_exist(join_cols, nested.fields(right_df))

    global_schema_diff_result = diff_dataframe_schemas(left_df, right_df, join_cols)
    diff_dataframe_shards = _build_diff_dataframe_shards(
        left_df, right_df, global_schema_diff_result, join_cols, specified_join_cols, _max_number_of_col_per_shard
    )
    diff_result = DiffResult(
        global_schema_diff_result,
        diff_dataframe_shards,
        join_cols,
    )

    return diff_result


def __get_test_dfs() -> Tuple[DataFrame, DataFrame]:
    from bigquery_frame import BigQueryBuilder

    bq = BigQueryBuilder()

    df1 = bq.sql(
        """
        SELECT * FROM UNNEST([
            STRUCT(1 as id, [STRUCT(1 as a, 2 as b, 3 as c)] as my_array),
            STRUCT(2 as id, [STRUCT(1 as a, 2 as b, 3 as c)] as my_array),
            STRUCT(3 as id, [STRUCT(1 as a, 2 as b, 3 as c)] as my_array)
        ])
    """,
    )
    df2 = bq.sql(
        """
        SELECT * FROM UNNEST([
            STRUCT(1 as id, [STRUCT(1 as a, 2 as b, 3 as c, 4 as d)] as my_array),
            STRUCT(2 as id, [STRUCT(2 as a, 2 as b, 3 as c, 4 as d)] as my_array),
            STRUCT(4 as id, [STRUCT(1 as a, 2 as b, 3 as c, 4 as d)] as my_array)
       ])
    """,
    )
    return df1, df2
