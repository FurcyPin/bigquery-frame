import difflib
import traceback
from typing import Dict, List, Optional, Tuple, Union

from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import BadRequest
from tqdm import tqdm

from bigquery_frame import DataFrame
from bigquery_frame import functions as f
from bigquery_frame import transformations as df_transformations
from bigquery_frame.column import Column
from bigquery_frame.data_diff.diff_format_options import DiffFormatOptions
from bigquery_frame.data_diff.diff_results import DiffResult
from bigquery_frame.data_diff.package import (
    EXISTS_COL_NAME,
    IS_EQUAL_COL_NAME,
    STRUCT_SEPARATOR_ALPHA,
    canonize_col,
)
from bigquery_frame.data_type_utils import get_common_columns
from bigquery_frame.dataframe import cols_to_str, is_nullable, is_repeated
from bigquery_frame.transformations import (
    flatten_schema,
    harmonize_dataframes,
    normalize_arrays,
)
from bigquery_frame.utils import quote, quote_columns, strip_margin


class DataframeComparatorException(Exception):
    pass


class CombinatorialExplosionError(DataframeComparatorException):
    pass


def shard_list(lst: List, n: int):
    """Yield successive n-sized shards from lst."""
    for i in range(0, len(lst), n):
        # fmt: off
        yield lst[i: i + n]
        # fmt: on


class DataframeComparator:
    def __init__(
        self,
        diff_format_options: Optional[DiffFormatOptions] = DiffFormatOptions(),
        _shard_size: Optional[int] = 100,
    ):
        self.diff_format_options = diff_format_options
        self._shard_size = _shard_size
        pass

    @staticmethod
    def _schema_to_string(
        schema: List[SchemaField], include_nullable: bool = False, include_metadata: bool = False
    ) -> List[str]:
        """Return a list of strings representing the schema

        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> df_comparator = DataframeComparator()
        >>> df = bq.sql('''SELECT 1 as id, "a" as c1, 1 as c2''')
        >>> print('\\n'.join(DataframeComparator()._schema_to_string(df.schema)))
        id INTEGER
        c1 STRING
        c2 INTEGER
        >>> print('\\n'.join(df_comparator._schema_to_string(df.schema, include_nullable=True)))
        id INTEGER (nullable)
        c1 STRING (nullable)
        c2 INTEGER (nullable)
        >>> schema = [
        ...     SchemaField('id', 'INTEGER', 'NULLABLE', 'An id', (), None),
        ...     SchemaField('c1', 'STRING', 'REQUIRED', 'A string column', (), None),
        ...     SchemaField('c2', 'INTEGER', 'NULLABLE', 'An int column', (), None)
        ... ]
        >>> print('\\n'.join(df_comparator._schema_to_string(schema, include_nullable=True, include_metadata=True)))
        id INTEGER (nullable) An id
        c1 STRING (required) A string column
        c2 INTEGER (nullable) An int column

        :param schema: A DataFrame schema
        :param include_nullable: (default: False) indicate for each field if it is nullable
        :param include_metadata: (default: False) add field description
        :return:
        """
        res = []
        for field in schema:
            s = f"{field.name} {field.field_type}"
            if include_nullable:
                if is_nullable(field):
                    s += " (nullable)"
                else:
                    s += " (required)"
            if include_metadata:
                s += f" {field.description}"
            res.append(s)
        return res

    @staticmethod
    def _compare_schemas(left_df: DataFrame, right_df: DataFrame) -> bool:
        """Compares two DataFrames schemas and print out the differences.
        Ignore the nullable and comment attributes.

        Example:

        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> left_df = bq.sql('''SELECT 1 as id, "" as c1, "" as c2, [STRUCT(2 as a, "" as b)] as c4''')
        >>> right_df = bq.sql('''SELECT 1 as id, 2 as c1, "" as c3, [STRUCT(3 as a, "" as d)] as c4''')
        >>> res = DataframeComparator._compare_schemas(left_df, right_df)
        Schema has changed:
        @@ -1,5 +1,5 @@
        <BLANKLINE>
         id INTEGER
        -c1 STRING
        -c2 STRING
        +c1 INTEGER
        +c3 STRING
         c4!.a INTEGER
        -c4!.b STRING
        +c4!.d STRING
        WARNING: columns that do not match both sides will be ignored
        >>> res
        False

        :param left_df: a DataFrame
        :param right_df: another DataFrame
        :return: True if both DataFrames have the same schema, False otherwise.
        """
        left_schema_flat = flatten_schema(left_df.schema, explode=True)
        right_schema_flat = flatten_schema(right_df.schema, explode=True)
        left_string_schema = DataframeComparator._schema_to_string(left_schema_flat)
        right_string_schema = DataframeComparator._schema_to_string(right_schema_flat)

        diff = list(difflib.unified_diff(left_string_schema, right_string_schema))[2:]

        if len(diff) > 0:
            print("Schema has changed:\n%s" % "\n".join(diff))
            print("WARNING: columns that do not match both sides will be ignored")
            return False
        else:
            print("Schema: ok (%s columns)" % len(left_df.columns))
            return True

    @staticmethod
    def _get_self_join_growth_estimate(df: DataFrame, cols: Union[str, List[str]]) -> float:
        """Computes how much time bigger a DataFrame will be if we self-join it using the provided columns,
        rounded to 2 decimals

        Example: If a DataFrame with 6 rows has one value present on 2 rows and another value present on 3 rows,
        the growth factor will be (1*1 + 2*2 + 3*3) / 6 ~= 2.33.
        If a column unique on each row, it's number of duplicates will be 0.

        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as name),
        ...     STRUCT(2 as id, "b" as name),
        ...     STRUCT(3 as id, "b" as name),
        ...     STRUCT(4 as id, "c" as name),
        ...     STRUCT(5 as id, "c" as name),
        ...     STRUCT(6 as id, "c" as name)
        ... ])''')
        >>> DataframeComparator._get_self_join_growth_estimate(df, "id")
        1.0
        >>> DataframeComparator._get_self_join_growth_estimate(df, "name")
        2.33

        :param df: a DataFrame
        :param cols: a list of column names
        :return: number of duplicate rows
        """
        # TODO: rewrite with df.groupBy()
        if isinstance(cols, str):
            cols = [cols]
        query1 = strip_margin(
            f"""
        |SELECT
        |  COUNT(1) as nb
        |FROM {df._alias}
        |GROUP BY {cols_to_str(quote_columns(cols))}
        |"""
        )
        df1 = df._apply_query(query1)
        query2 = strip_margin(
            f"""
        |SELECT
        |  SUM(nb) as nb_rows,
        |  SUM(nb * nb) as nb_rows_after_self_join
        |FROM {df1._alias}
        |"""
        )
        df2 = df1._apply_query(query2)
        res = df2.take(1)[0]
        nb_rows = res.get("nb_rows")
        nb_rows_after_self_join = res.get("nb_rows_after_self_join")
        if nb_rows_after_self_join is None:
            nb_rows_after_self_join = 0
        if nb_rows is None or nb_rows == 0:
            return 1.0
        else:
            return round(nb_rows_after_self_join * 1.0 / nb_rows, 2)

    @staticmethod
    def _get_eligible_columns_for_join(df: DataFrame) -> Dict[str, float]:
        """Identifies the column with the least duplicates, in order to use it as the id for the comparison join.

        Eligible columns are all columns of type String, Int or Bigint that have an approximate distinct count of 90%
        of the number of rows in the DataFrame. Returns None if no such column is found.

        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as name),
        ...     STRUCT(2 as id, "b" as name),
        ...     STRUCT(3 as id, "b" as name)
        ... ])''')
        >>> DataframeComparator._get_eligible_columns_for_join(df)
        {'id': 1.0}
        >>> df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as name),
        ...     STRUCT(1 as id, "a" as name)
        ... ])''')
        >>> DataframeComparator._get_eligible_columns_for_join(df)
        {}

        :param df: a DataFrame
        :return: The name of the columns with less than 10% duplicates, and their
            corresponding self-join-growth-estimate
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
                    f.approx_count_distinct(quote(col)) * f.lit(100.0) / f.count(f.lit(1)) > distinct_count_threshold
                ).alias(col)
                for col in eligible_cols
            ]
        )
        columns_with_high_distinct_count = [key for key, value in eligibility_df.collect()[0].items() if value]
        cols_with_duplicates = {
            col: DataframeComparator._get_self_join_growth_estimate(df, col) for col in columns_with_high_distinct_count
        }
        return cols_with_duplicates

    @staticmethod
    def _merge_growth_estimate_dicts(left_dict: Dict[str, float], right_dict: Dict[str, float]):
        """Merge together two dicts giving for each column name the corresponding growth_estimate

        >>> DataframeComparator._merge_growth_estimate_dicts({"a": 10.0, "b": 1.0}, {"a": 1.0, "c": 1.0})
        {'a': 5.5, 'b': 1.0, 'c': 1.0}

        :param left_dict:
        :param right_dict:
        :return:
        """
        res = left_dict.copy()
        for x in right_dict:
            if x in left_dict:
                res[x] = (res[x] + right_dict[x]) / 2
            else:
                res[x] = right_dict[x]
        return res

    @staticmethod
    def _automatically_infer_join_col(left_df: DataFrame, right_df: DataFrame) -> Tuple[Optional[str], Optional[float]]:
        """Identify the column with the least duplicates, in order to use it as the id for the comparison join.

        Eligible columns are all columns of type String, Int or Bigint that have an approximate distinct count of 90%
        of the number of rows in the DataFrame. Returns None if no suche column is found.

        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
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
        >>> DataframeComparator._automatically_infer_join_col(left_df, right_df)
        ('id', 1.0)
        >>> left_df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as name),
        ...     STRUCT(1 as id, "a" as name)
        ... ])''')
        >>> right_df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as name),
        ...     STRUCT(1 as id, "a" as name)
        ... ])''')
        >>> DataframeComparator._automatically_infer_join_col(left_df, right_df)
        (None, None)

        :param left_df: a DataFrame
        :param right_df: a DataFrame
        :return: The name of the column with the least duplicates in both DataFrames if it has less than 10% duplicates.
        """
        left_col_dict = DataframeComparator._get_eligible_columns_for_join(left_df)
        right_col_dict = DataframeComparator._get_eligible_columns_for_join(left_df)
        merged_col_dict = DataframeComparator._merge_growth_estimate_dicts(left_col_dict, right_col_dict)

        if len(merged_col_dict) > 0:
            col, self_join_growth_estimate = sorted(merged_col_dict.items(), key=lambda x: -x[1])[0]
            return col, self_join_growth_estimate
        else:
            return None, None

    def _get_join_cols(self, left_df: DataFrame, right_df: DataFrame, join_cols: List[str]) -> Tuple[List[str], float]:
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
                "trying to automatically infer a column that can be used for joining the two DataFrames"
            )
            inferred_join_col, self_join_growth_estimate = DataframeComparator._automatically_infer_join_col(
                left_df, right_df
            )
            if inferred_join_col is None:
                raise DataframeComparatorException(
                    "Could not automatically infer a column sufficiently "
                    "unique to join the two DataFrames and perform a comparison. "
                    "Please specify manually the columns to use with the join_cols parameter"
                )
            else:
                print(f"Found the following column: {inferred_join_col}")
                join_cols = [inferred_join_col]
        else:
            self_join_growth_estimate = (
                DataframeComparator._get_self_join_growth_estimate(left_df, join_cols)
                + DataframeComparator._get_self_join_growth_estimate(right_df, join_cols)
            ) / 2
        return join_cols, self_join_growth_estimate

    def _check_join_cols(
        self, specified_join_cols: Optional[List[str]], join_cols: Optional[List[str]], self_join_growth_estimate: float
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
            join_cols_str = str(join_cols[0])
        else:
            plural_str = "s"
            join_cols_str = str(join_cols)

        if self_join_growth_estimate >= 2.0:
            raise CombinatorialExplosionError(
                f"Performing a join with the {inferred_provided_str} column{plural_str} {join_cols_str} "
                f"would increase the size of the table by a factor of {self_join_growth_estimate}. "
                f"Please provide join_cols that are truly unique for both DataFrames."
            )
        print(
            f"We will try to find the differences by joining the DataFrames together "
            f"using the {inferred_provided_str} column{plural_str}: {join_cols_str}"
        )
        if self_join_growth_estimate > 1.0:
            print(
                f"WARNING: duplicates have been detected in the joining key, the resulting DataFrame "
                f"will be {self_join_growth_estimate} bigger which might affect the diff results. "
                f"Please consider providing join_cols that are truly unique for both DataFrames."
            )

    @staticmethod
    def _build_diff_dataframe(left_df: DataFrame, right_df: DataFrame, join_cols: List[str]) -> DataFrame:
        """Perform a column-by-column comparison between two DataFrames.
        The two DataFrames must have the same columns with the same ordering.
        The column `join_col` will be used to join the two DataFrames together.
        Then we build a new DataFrame with the `join_col` and for each column, a struct with three elements:
        - `left_value`: the value coming from the `left_df`
        - `right_value`: the value coming from the `right_df`
        - `is_equal`: True if both values have the same hash, False otherwise.

        Example:

        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> left_df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as c1, 1 as c2),
        ...     STRUCT(2 as id, "b" as c1, 2 as c2),
        ...     STRUCT(3 as id, "c" as c1, 3 as c2)
        ... ])''')
        >>> right_df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as c1, 1 as c2),
        ...     STRUCT(2 as id, "b" as c1, 4 as c2),
        ...     STRUCT(4 as id, "f" as c1, 3 as c2)
        ... ])''')
        >>> left_df.show()
        +----+----+----+
        | id | c1 | c2 |
        +----+----+----+
        |  1 |  a |  1 |
        |  2 |  b |  2 |
        |  3 |  c |  3 |
        +----+----+----+
        >>> right_df.show()
        +----+----+----+
        | id | c1 | c2 |
        +----+----+----+
        |  1 |  a |  1 |
        |  2 |  b |  4 |
        |  4 |  f |  3 |
        +----+----+----+
        >>> DataframeComparator._build_diff_dataframe(left_df, right_df, ['id']).orderBy('id').show()  # noqa: E501
        +----+-------------------------------------------------------------+-----------------------------------------------------------+--------------------------------------------+--------------+
        | id |                                                          c1 |                                                        c2 |                                 __EXISTS__ | __IS_EQUAL__ |
        +----+-------------------------------------------------------------+-----------------------------------------------------------+--------------------------------------------+--------------+
        |  1 |   {'left_value': 'a', 'right_value': 'a', 'is_equal': True} |     {'left_value': 1, 'right_value': 1, 'is_equal': True} |  {'left_value': True, 'right_value': True} |         True |
        |  2 |   {'left_value': 'b', 'right_value': 'b', 'is_equal': True} |    {'left_value': 2, 'right_value': 4, 'is_equal': False} |  {'left_value': True, 'right_value': True} |        False |
        |  3 | {'left_value': 'c', 'right_value': None, 'is_equal': False} | {'left_value': 3, 'right_value': None, 'is_equal': False} | {'left_value': True, 'right_value': False} |        False |
        |  4 | {'left_value': None, 'right_value': 'f', 'is_equal': False} | {'left_value': None, 'right_value': 3, 'is_equal': False} | {'left_value': False, 'right_value': True} |        False |
        +----+-------------------------------------------------------------+-----------------------------------------------------------+--------------------------------------------+--------------+

        :param left_df: a DataFrame
        :param right_df: a DataFrame with the same columns
        :param join_cols: the columns to use to perform the join.
        :return: a DataFrame containing all the columns that differ, and a dictionary that gives the number of
            differing rows for each column
        """
        left_df = left_df.withColumn(EXISTS_COL_NAME, f.lit(True))
        right_df = right_df.withColumn(EXISTS_COL_NAME, f.lit(True))

        diff = left_df.join(right_df, join_cols, "full")

        compared_fields = [
            field for field in left_df.schema if field.name not in join_cols and field.name != EXISTS_COL_NAME
        ]

        def comparison_struct(field: SchemaField) -> Column:
            left_col: Column = left_df[field.name]
            right_col: Column = right_df[field.name]
            left_col_str: Column = canonize_col(left_col, field)
            right_col_str: Column = canonize_col(right_col, field)
            return f.struct(
                left_col.alias("left_value"),
                right_col.alias("right_value"),
                (
                    (left_col_str.isNull() & right_col_str.isNull())
                    | (left_col_str.isNotNull() & right_col_str.isNotNull() & (left_col_str == right_col_str))
                ).alias("is_equal"),
            ).alias(field.name)

        diff_df = diff.select(
            *[f.coalesce(left_df[col], right_df[col]).alias(col) for col in join_cols],
            *[comparison_struct(field) for field in compared_fields],
            f.struct(
                f.coalesce(left_df[EXISTS_COL_NAME], f.lit(False)).alias("left_value"),
                f.coalesce(right_df[EXISTS_COL_NAME], f.lit(False)).alias("right_value"),
            ).alias(EXISTS_COL_NAME),
        )

        row_is_equal = f.lit(True)
        for field in compared_fields:
            row_is_equal = row_is_equal & f.col(f"{field.name}.is_equal")
        return diff_df.withColumn(IS_EQUAL_COL_NAME, row_is_equal)

    def _build_diff_dataframe_for_shard(
        self,
        left_flat: DataFrame,
        right_flat: DataFrame,
        common_column_shard: List[Tuple[str, str]],
        join_cols: List[str],
        skip_make_dataframes_comparable: bool,
    ):
        if not skip_make_dataframes_comparable:
            left_flat, right_flat = harmonize_dataframes(left_flat, right_flat, common_column_shard)
        left_flat = normalize_arrays(left_flat)
        right_flat = normalize_arrays(right_flat)
        return self._build_diff_dataframe(left_flat, right_flat, join_cols)

    def _build_diff_dataframe_shards(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        common_columns: List[Tuple[str, str]],
        join_cols: List[str],
        same_schema: bool,
    ) -> List[DataFrame]:
        """TODO

        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> bq = BigQueryBuilder(get_bq_client())
        >>> left_df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as c1, 1 as c2),
        ...     STRUCT(2 as id, "b" as c1, 2 as c2),
        ...     STRUCT(3 as id, "c" as c1, 3 as c2)
        ... ])''')
        >>> right_df = bq.sql('''SELECT * FROM UNNEST([
        ...     STRUCT(1 as id, "a" as c1, 1 as c2),
        ...     STRUCT(2 as id, "b" as c1, 4 as c2),
        ...     STRUCT(4 as id, "f" as c1, 3 as c2)
        ... ])''')
        >>> left_df.show()
        +----+----+----+
        | id | c1 | c2 |
        +----+----+----+
        |  1 |  a |  1 |
        |  2 |  b |  2 |
        |  3 |  c |  3 |
        +----+----+----+
        >>> right_df.show()
        +----+----+----+
        | id | c1 | c2 |
        +----+----+----+
        |  1 |  a |  1 |
        |  2 |  b |  4 |
        |  4 |  f |  3 |
        +----+----+----+
        >>> shards = DataframeComparator(_shard_size=10)._build_diff_dataframe_shards(left_df, right_df,[('id', None), ('c1', None), ('c2', None)],['id'], same_schema=True)
        >>> for shard in shards: shard.orderBy('id').show()  # noqa: E501
        +----+-------------------------------------------------------------+-----------------------------------------------------------+--------------------------------------------+--------------+
        | id |                                                          c1 |                                                        c2 |                                 __EXISTS__ | __IS_EQUAL__ |
        +----+-------------------------------------------------------------+-----------------------------------------------------------+--------------------------------------------+--------------+
        |  1 |   {'left_value': 'a', 'right_value': 'a', 'is_equal': True} |     {'left_value': 1, 'right_value': 1, 'is_equal': True} |  {'left_value': True, 'right_value': True} |         True |
        |  2 |   {'left_value': 'b', 'right_value': 'b', 'is_equal': True} |    {'left_value': 2, 'right_value': 4, 'is_equal': False} |  {'left_value': True, 'right_value': True} |        False |
        |  3 | {'left_value': 'c', 'right_value': None, 'is_equal': False} | {'left_value': 3, 'right_value': None, 'is_equal': False} | {'left_value': True, 'right_value': False} |        False |
        |  4 | {'left_value': None, 'right_value': 'f', 'is_equal': False} | {'left_value': None, 'right_value': 3, 'is_equal': False} | {'left_value': False, 'right_value': True} |        False |
        +----+-------------------------------------------------------------+-----------------------------------------------------------+--------------------------------------------+--------------+
        >>> shards = DataframeComparator(_shard_size=1)._build_diff_dataframe_shards(left_df, right_df,[('id', None), ('c1', None), ('c2', None)],['id'], same_schema=False)
        >>> for shard in shards: shard.orderBy('id').show()
        +----+-------------------------------------------------------------+--------------------------------------------+--------------+
        | id |                                                          c1 |                                 __EXISTS__ | __IS_EQUAL__ |
        +----+-------------------------------------------------------------+--------------------------------------------+--------------+
        |  1 |   {'left_value': 'a', 'right_value': 'a', 'is_equal': True} |  {'left_value': True, 'right_value': True} |         True |
        |  2 |   {'left_value': 'b', 'right_value': 'b', 'is_equal': True} |  {'left_value': True, 'right_value': True} |         True |
        |  3 | {'left_value': 'c', 'right_value': None, 'is_equal': False} | {'left_value': True, 'right_value': False} |        False |
        |  4 | {'left_value': None, 'right_value': 'f', 'is_equal': False} | {'left_value': False, 'right_value': True} |        False |
        +----+-------------------------------------------------------------+--------------------------------------------+--------------+
        +----+-----------------------------------------------------------+--------------------------------------------+--------------+
        | id |                                                        c2 |                                 __EXISTS__ | __IS_EQUAL__ |
        +----+-----------------------------------------------------------+--------------------------------------------+--------------+
        |  1 |     {'left_value': 1, 'right_value': 1, 'is_equal': True} |  {'left_value': True, 'right_value': True} |         True |
        |  2 |    {'left_value': 2, 'right_value': 4, 'is_equal': False} |  {'left_value': True, 'right_value': True} |        False |
        |  3 | {'left_value': 3, 'right_value': None, 'is_equal': False} | {'left_value': True, 'right_value': False} |        False |
        |  4 | {'left_value': None, 'right_value': 3, 'is_equal': False} | {'left_value': False, 'right_value': True} |        False |
        +----+-----------------------------------------------------------+--------------------------------------------+--------------+

        :param left_df: a DataFrame
        :param right_df: a DataFrame with the same columns
        :param join_cols: the columns to use to perform the join.
        :return: a DataFrame containing all the columns that differ, and a dictionary that gives the number of
            differing rows for each column
        """
        join_columns = [(col, tpe) for (col, tpe) in common_columns if col in join_cols]
        non_join_columns = [(col, tpe) for (col, tpe) in common_columns if col not in join_cols]
        nb_cols = len(non_join_columns)

        if nb_cols <= self._shard_size:
            return [
                self._build_diff_dataframe_for_shard(
                    left_df, right_df, common_columns, join_cols, skip_make_dataframes_comparable=same_schema
                ).persist()
            ]
        else:
            columns_shard = [join_columns + shard for shard in shard_list(non_join_columns, self._shard_size)]
            dfs = [
                self._build_diff_dataframe_for_shard(
                    left_df, right_df, column_shard, join_cols, skip_make_dataframes_comparable=False
                ).persist()
                for column_shard in tqdm(columns_shard)
            ]
            return dfs

    def compare_df(
        self, left_df: DataFrame, right_df: DataFrame, join_cols: List[str] = None, show_examples: bool = False
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

        Tips:
        -----
        - If you want to test a column renaming, you can temporarily add renaming step to the DataFrame
          you want to test.
        - When comparing arrays, this algorithm ignores their ordering (e.g. `[1, 2, 3] == [3, 2, 1]`)
        - The algorithm is able to handle nested non-repeated records, such as STRUCT<STRUCT<>>, or even ARRAY<STRUCT>>
          but it doesn't support nested repeated structures, such as ARRAY<STRUCT<ARRAY<>>>.

        :param left_df: a DataFrame
        :param right_df: another DataFrame
        :param join_cols: [Optional] specifies the columns on which the two DataFrames should be joined to compare them
        :param show_examples: if set to True, print for each column examples of full rows where this column changes
        :return: a DiffResult object
        """
        if join_cols == []:
            join_cols = None
        specified_join_cols = join_cols

        same_schema = self._compare_schemas(left_df, right_df)

        left_flat = df_transformations.flatten(left_df, struct_separator=STRUCT_SEPARATOR_ALPHA)
        right_flat = df_transformations.flatten(right_df, struct_separator=STRUCT_SEPARATOR_ALPHA)

        print("\nAnalyzing differences...")
        join_cols, self_join_growth_estimate = self._get_join_cols(left_flat, right_flat, join_cols)
        self._check_join_cols(specified_join_cols, join_cols, self_join_growth_estimate)

        left_schema_flat = flatten_schema(left_flat.schema, explode=True)
        if not same_schema:
            # We apply a `limit(0).persist()` to prevent BigQuery from crashing on very large tables
            right_schema_flat = flatten_schema(right_flat.schema, explode=True)
            common_columns = get_common_columns(left_schema_flat, right_schema_flat)
        else:
            common_columns = [(field.name, None) for field in left_schema_flat]

        diff_shards = self._build_diff_dataframe_shards(left_flat, right_flat, common_columns, join_cols, same_schema)

        diff_result = DiffResult(same_schema, diff_shards, join_cols, self.diff_format_options)
        try:
            diff_result.display(show_examples)
        except BadRequest:
            traceback.print_exc()
            print(
                "An error occurred while displaying the diff results."
                "If you are in a Python console and did not assign the result of compare_df() to a value, "
                "you can retrieve the results by running this command:"
                "diff_result = _"
            )

        return diff_result
