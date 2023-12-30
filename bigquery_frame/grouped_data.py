import datetime
import decimal
from functools import cached_property
from typing import Any, Callable, List, Optional

from bigquery_frame import DataFrame, functions
from bigquery_frame.column import Column, ColumnOrName, LitOrColumn, cols_to_str
from bigquery_frame.conf import STRUCT_SEPARATOR
from bigquery_frame.data_type_utils import flatten_schema
from bigquery_frame.dataframe import is_numeric
from bigquery_frame.exceptions import AnalysisException, UnexpectedException, UnsupportedOperationException
from bigquery_frame.field_utils import substring_after_last_occurrence
from bigquery_frame.utils import assert_true, quote, str_to_cols, strip_margin


class GroupedData:
    def __init__(
        self,
        df: DataFrame,
        grouped_columns: List[ColumnOrName],
        pivot_column: Optional[str] = None,
        pivoted_columns: Optional[List[LitOrColumn]] = None,
    ):
        self.__df = df
        self.__grouped_columns: List[ColumnOrName] = grouped_columns
        self.__pivot_column = pivot_column
        self.__pivoted_columns: Optional[List[LitOrColumn]] = pivoted_columns

    @cached_property
    def __flat_schema(self):
        return flatten_schema(self.__df.schema, explode=False)

    def agg(self, *cols: Column) -> DataFrame:
        """Compute aggregates and returns the result as a :class:`DataFrame`.

        The available aggregate functions can be built-in aggregation functions, such as
        `avg`, `max`, `min`, `sum`, `count`.

        Args:
            cols : A list of aggregate :class:`Column` expressions.

        Raises:
            bigquery_frame.exceptions.AnalysisException: If incorrect arguments are detected.

        Examples:
            >>> from bigquery_frame.grouped_data import _get_test_df
            >>> from bigquery_frame import functions as f
            >>> df = _get_test_df().select("age", "name")
            >>> df.show()
            +-----+-------+
            | age |  name |
            +-----+-------+
            |   2 | Alice |
            |   3 | Alice |
            |   5 |   Bob |
            |  10 |   Bob |
            +-----+-------+

            Group-by name, and count each group.

            >>> df.groupBy("name").agg(f.count(f.lit(1)).alias("count")).sort("name").show()
            +-------+-------+
            |  name | count |
            +-------+-------+
            | Alice |     2 |
            |   Bob |     2 |
            +-------+-------+

            Group-by name, and calculate the minimum age.

            >>> df.groupBy(f.lower("name").alias("name")).agg(f.min("age").alias("min_age")).sort("name").show()
            +-------+---------+
            |  name | min_age |
            +-------+---------+
            | alice |       2 |
            |   bob |       5 |
            +-------+---------+
        """
        if self.__pivot_column is None:
            query = self.__generate_group_by_query(*cols)
        else:
            query = self.__generate_pivot_query(*cols)
        return self.__df._apply_query(query)

    def avg(self, *cols: str) -> DataFrame:
        """Computes average values for each numeric columns for each group.

        Args:
            *cols: column names. Non-numeric columns are ignored.

        Examples:

            >>> from bigquery_frame.grouped_data import _get_test_df
            >>> df = _get_test_df()
            >>> df.show()
            +-----+-------+--------+
            | age |  name | height |
            +-----+-------+--------+
            |   2 | Alice |     80 |
            |   3 | Alice |    100 |
            |   5 |   Bob |    120 |
            |  10 |   Bob |    140 |
            +-----+-------+--------+

            Group-by name, and calculate the average of the age in each group.

            >>> df.groupBy("name").avg("age").sort("name").show()
            +-------+---------+
            |  name | avg_age |
            +-------+---------+
            | Alice |     2.5 |
            |   Bob |     7.5 |
            +-------+---------+

            Calculate the average of the age and height in all data.

            >>> df.groupBy().avg("age", "height").show()
            +---------+------------+
            | avg_age | avg_height |
            +---------+------------+
            |     5.0 |      110.0 |
            +---------+------------+

            Calculate the average of all numeric columns.

            >>> df.groupBy().avg().show()
            +---------+------------+
            | avg_age | avg_height |
            +---------+------------+
            |     5.0 |      110.0 |
            +---------+------------+
        """
        return self.__numeric_agg(functions.avg, "avg", *cols)

    def count(self) -> DataFrame:
        """Counts the number of records for each group.

        Examples:

            >>> from bigquery_frame.grouped_data import _get_test_df
            >>> df = _get_test_df().select("age", "name")
            >>> df.show()
            +-----+-------+
            | age |  name |
            +-----+-------+
            |   2 | Alice |
            |   3 | Alice |
            |   5 |   Bob |
            |  10 |   Bob |
            +-----+-------+

            Group-by name, and count each group.
            >>> df.groupBy("name").count().sort("name").show()
            +-------+-------+
            |  name | count |
            +-------+-------+
            | Alice |     2 |
            |   Bob |     2 |
            +-------+-------+
        """
        return self.agg(functions.count(functions.lit(1)).alias("count"))

    def mean(self, *cols: str) -> DataFrame:
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        Args:
            *cols: column names. Non-numeric columns are ignored.

        Examples:

            >>> from bigquery_frame.grouped_data import _get_test_df
            >>> df = _get_test_df()
            >>> df.show()
            +-----+-------+--------+
            | age |  name | height |
            +-----+-------+--------+
            |   2 | Alice |     80 |
            |   3 | Alice |    100 |
            |   5 |   Bob |    120 |
            |  10 |   Bob |    140 |
            +-----+-------+--------+

            Group-by name, and calculate the mean of the age in each group.

            >>> df.groupBy("name").mean("age").sort("name").show()
            +-------+----------+
            |  name | mean_age |
            +-------+----------+
            | Alice |      2.5 |
            |   Bob |      7.5 |
            +-------+----------+

            Calculate the mean of the age and height in all data.

            >>> df.groupBy().mean("age", "height").show()
            +----------+-------------+
            | mean_age | mean_height |
            +----------+-------------+
            |      5.0 |       110.0 |
            +----------+-------------+

            Calculate the mean of all numeric columns.

            >>> df.groupBy().mean().show()
            +----------+-------------+
            | mean_age | mean_height |
            +----------+-------------+
            |      5.0 |       110.0 |
            +----------+-------------+
        """
        return self.__numeric_agg(functions.avg, "mean", *cols)

    def max(self, *cols: str) -> DataFrame:
        """Computes the max value for each numeric columns for each group.

        Args:
            *cols: column names. Non-numeric columns are ignored.

        Examples:

            >>> from bigquery_frame.grouped_data import _get_test_df
            >>> df = _get_test_df()
            >>> df.show()
            +-----+-------+--------+
            | age |  name | height |
            +-----+-------+--------+
            |   2 | Alice |     80 |
            |   3 | Alice |    100 |
            |   5 |   Bob |    120 |
            |  10 |   Bob |    140 |
            +-----+-------+--------+

            Group-by name, and calculate the max of the age in each group.

            >>> df.groupBy("name").max("age").sort("name").show()
            +-------+---------+
            |  name | max_age |
            +-------+---------+
            | Alice |       3 |
            |   Bob |      10 |
            +-------+---------+

            Calculate the max of the age and height in all data.

            >>> df.groupBy().max("age", "height").show()
            +---------+------------+
            | max_age | max_height |
            +---------+------------+
            |      10 |        140 |
            +---------+------------+

            Calculate the max of all numeric columns.

            >>> df.groupBy().max().show()
            +---------+------------+
            | max_age | max_height |
            +---------+------------+
            |      10 |        140 |
            +---------+------------+
        """
        return self.__numeric_agg(functions.max, "max", *cols)

    def min(self, *cols: str) -> DataFrame:
        """Computes the min value for each numeric columns for each group.

        Args:
            *cols: column names. Non-numeric columns are ignored.

        Examples:

            >>> from bigquery_frame.grouped_data import _get_test_df
            >>> df = _get_test_df()
            >>> df.show()
            +-----+-------+--------+
            | age |  name | height |
            +-----+-------+--------+
            |   2 | Alice |     80 |
            |   3 | Alice |    100 |
            |   5 |   Bob |    120 |
            |  10 |   Bob |    140 |
            +-----+-------+--------+

            Group-by name, and calculate the min of the age in each group.

            >>> df.groupBy("name").min("age").sort("name").show()
            +-------+---------+
            |  name | min_age |
            +-------+---------+
            | Alice |       2 |
            |   Bob |       5 |
            +-------+---------+

            Calculate the min of the age and height in all data.

            >>> df.groupBy().min("age", "height").show()
            +---------+------------+
            | min_age | min_height |
            +---------+------------+
            |       2 |         80 |
            +---------+------------+

            Calculate the min of all numeric columns.

            >>> df.groupBy().min().show()
            +---------+------------+
            | min_age | min_height |
            +---------+------------+
            |       2 |         80 |
            +---------+------------+
        """
        return self.__numeric_agg(functions.min, "min", *cols)

    def sum(self, *cols: str) -> DataFrame:
        """Computes the sum for each numeric columns for each group.

        Args:
            *cols: column names. Non-numeric columns are ignored.

        Examples:

            >>> from bigquery_frame.grouped_data import _get_test_df
            >>> df = _get_test_df()
            >>> df.show()
            +-----+-------+--------+
            | age |  name | height |
            +-----+-------+--------+
            |   2 | Alice |     80 |
            |   3 | Alice |    100 |
            |   5 |   Bob |    120 |
            |  10 |   Bob |    140 |
            +-----+-------+--------+

            Group-by name, and calculate the sum of the age in each group.

            >>> df.groupBy("name").sum("age").sort("name").show()
            +-------+---------+
            |  name | sum_age |
            +-------+---------+
            | Alice |       5 |
            |   Bob |      15 |
            +-------+---------+

            Calculate the sum of the age and height in all data.

            >>> df.groupBy().sum("age", "height").show()
            +---------+------------+
            | sum_age | sum_height |
            +---------+------------+
            |      20 |        440 |
            +---------+------------+

            Calculate the sum of all numeric columns.

            >>> df.groupBy().sum().show()
            +---------+------------+
            | sum_age | sum_height |
            +---------+------------+
            |      20 |        440 |
            +---------+------------+
        """
        return self.__numeric_agg(functions.sum, "sum", *cols)

    def pivot(self, pivot_column: str, pivoted_columns: Optional[List[LitOrColumn]] = None) -> "GroupedData":
        """

        Args:
            pivot_column:
            pivoted_columns:

        Raises:
            bigquery_frame.exceptions.UnsupportedOperationException: if `.pivot(...)` is called multiple times

        Returns:

            >>> from bigquery_frame import functions as f
            >>> from bigquery_frame import BigQueryBuilder
            >>> bq = BigQueryBuilder()
            >>> df1 = bq.sql('''
            ...     SELECT * FROM UNNEST([
            ...         STRUCT("dotNET" as course, 2012 as year, 10000 as earnings),
            ...         STRUCT("Java" as course, 2012 as year, 20000 as earnings),
            ...         STRUCT("dotNET" as course, 2012 as year, 5000 as earnings),
            ...         STRUCT("dotNET" as course, 2013 as year, 48000 as earnings),
            ...         STRUCT("Java" as course, 2013 as year, 30000 as earnings)
            ...     ])
            ... ''')

            Compute the sum of earnings for each year by course with each course as a separate column

            >>> df1.groupBy("year").pivot("course", ["dotNET", "Java"]).sum("earnings").show()
            +------+--------+-------+
            | year | dotNET |  Java |
            +------+--------+-------+
            | 2012 |  15000 | 20000 |
            | 2013 |  48000 | 30000 |
            +------+--------+-------+

            Or without specifying column values (less efficient)

            >>> df1.groupBy("year").pivot("course").sum("earnings").show()
            +------+-------+--------+
            | year |  Java | dotNET |
            +------+-------+--------+
            | 2012 | 20000 |  15000 |
            | 2013 | 30000 |  48000 |
            +------+-------+--------+

            >>> df1.groupBy(f.col("year")).pivot("course").agg(
            ...     f.min("earnings").alias("min_earnings"),
            ...     f.max("earnings").alias("max_earnings")
            ... ).show()
            +------+-------------------+---------------------+-------------------+---------------------+
            | year | min_earnings_Java | min_earnings_dotNET | max_earnings_Java | max_earnings_dotNET |
            +------+-------------------+---------------------+-------------------+---------------------+
            | 2012 |             20000 |                5000 |             20000 |               10000 |
            | 2013 |             30000 |               48000 |             30000 |               48000 |
            +------+-------------------+---------------------+-------------------+---------------------+
        """
        assert_true(
            self.__pivot_column is None,
            UnsupportedOperationException(
                "[REPEATED_CLAUSE] The PIVOT clause may be used at most once per SUBQUERY operation."
            ),
        )
        return GroupedData(self.__df, self.__grouped_columns, pivot_column, pivoted_columns)

    def __generate_group_by_query(self, *agg_cols: Column) -> str:
        grouped_columns = str_to_cols(self.__grouped_columns)
        if len(grouped_columns) == 0:
            group_by_str = ""
        else:
            group_by_str = f"\nGROUP BY {cols_to_str([col.expr for col in grouped_columns])}"
        query = strip_margin(
            f"""
            |SELECT
            |{cols_to_str(grouped_columns + list(agg_cols), indentation=2)}
            |FROM {self.__df._alias}{group_by_str}"""
        )
        return query

    def __generate_pivot_query(self, *agg_cols: Column) -> str:
        assert_true(len(agg_cols) > 0, AnalysisException("At least one `agg_col` must be specified, got 0."))
        assert_true(
            len(agg_cols) == 1 or all(agg_col.get_alias() is not None for agg_col in agg_cols),
            AnalysisException("`agg_cols` must specify an alias when more than one is used."),
        )
        for grouped_col in self.__grouped_columns:
            if isinstance(grouped_col, str):
                assert_true(
                    STRUCT_SEPARATOR not in grouped_col,
                    AnalysisException(
                        f"Cannot pivot after groupBy on struct field '{grouped_col}': "
                        f"BigQuery does not support this operation with struct fields. "
                        f"Please rename this field as a root level column first."
                    ),
                )
        grouped_columns = str_to_cols(self.__grouped_columns)
        pivot_column_type = next(field.field_type for field in self.__flat_schema if field.name == self.__pivot_column)
        pivoted_columns = self.__pivoted_columns
        if pivoted_columns is None:
            pivot_column_simple_name = substring_after_last_occurrence(self.__pivot_column, STRUCT_SEPARATOR)
            pivot_column_values_df = self.__df.select(self.__pivot_column).distinct().orderBy(pivot_column_simple_name)
            pivoted_columns = [row.get(pivot_column_simple_name) for row in pivot_column_values_df.collect()]
        pivoted_columns = [_aliased_lit(col, pivot_column_type) for col in pivoted_columns]
        pivoted_columns_expr = [col.expr for col in pivoted_columns]
        if len(agg_cols) == 1 and agg_cols[0].get_alias() is None:
            select_columns = [quote(f"{col.get_alias()}") for col in pivoted_columns]
        else:
            select_columns = [
                quote(f"{agg_col.get_alias()}_{col.get_alias()}") for agg_col in agg_cols for col in pivoted_columns
            ]
        query = strip_margin(
            f"""
            |SELECT
            |{cols_to_str(grouped_columns + select_columns, indentation=2)}
            |FROM {self.__df._alias}
            |PIVOT(
            |{cols_to_str(agg_cols, indentation=2)}
            |FOR {self.__pivot_column} IN ({cols_to_str(pivoted_columns_expr)}))
            |"""
        )
        return query

    def __numeric_agg(self, agg: Callable[[Column], Column], agg_name: str, *cols: str):
        if len(cols) == 0:
            cols = [field.name for field in self.__df.schema if is_numeric(field)]
        else:
            col_set = set(cols)
            fields = [field for field in self.__flat_schema if field.name in col_set]
            non_numeric_cols = [field.name for field in fields if not is_numeric(field)]
            assert_true(
                len(non_numeric_cols) == 0,
                AnalysisException(
                    f"""Aggregation function can only be applied on a numeric columns. *
                    The following columns are not numeric: {non_numeric_cols}"""
                ),
            )
        if self.__pivot_column is not None and len(cols) == 1:
            # In scenario `df.pivot(...).agg(col)`, we don't give an alias to the aggregated column expression
            return self.agg(*[agg(col) for col in cols])
        else:
            return self.agg(
                *[
                    agg(col).alias(f"{agg_name}_{substring_after_last_occurrence(col, STRUCT_SEPARATOR)}")
                    for col in cols
                ]
            )


def _aliased_lit(col: Any, field_type: str) -> Column:
    """Creates a :class:`Column` of literal value with an alias, when possible.

    If the passed value is a :class:`Column`, it is returned as is.

    When the values used in PIVOT are passed as literal, BigQuery automatically generates aliases for them,
    when possible.

    Some data types are not supported, such as FLOAT, TIME, DATETIME, TIMESTAMP and BYTES.
    In this case an alias must be specified by the user.

    Supported types are:
        - `NULL`
        - `STRING`
        - `BOOL`
        - `INTEGER`
        - `NUMERIC`
        - `BIGNUMERIC`
        - `DATE`

    Args:
        col : A basic Python type to convert into BigQuery literal.

    Returns:
        The literal instance with a compatible alias

    Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> from bigquery_frame import functions as f
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql("SELECT 1 as id")
        >>> df.select(
        ...     _aliased_lit(f.col("id"), "INTEGER"),
        ...     _aliased_lit(None, "STRING"),
        ...     _aliased_lit(True, "BOOLEAN"),
        ...     _aliased_lit("a+b", "STRING"),
        ...     _aliased_lit(1, "INTEGER"),
        ...     _aliased_lit(decimal.Decimal("123456789.123456789e3"), "NUMERIC"),
        ...     _aliased_lit(decimal.Decimal("123456789.123456789e3"), "BIGNUMERIC"),
        ...     _aliased_lit(datetime.date.fromisoformat("2024-01-01"), "DATE"),
        ... ).printSchema()
        root
         |-- id: INTEGER (NULLABLE)
         |-- NULL: INTEGER (NULLABLE)
         |-- true: BOOLEAN (NULLABLE)
         |-- a+b: STRING (NULLABLE)
         |-- 1: INTEGER (NULLABLE)
         |-- 123456789123_point_456789: NUMERIC (NULLABLE)
         |-- 123456789123_point_456789_1: BIGNUMERIC (NULLABLE)
         |-- 2024_01_01: DATE (NULLABLE)
        <BLANKLINE>

        >>> df.select(_aliased_lit(b"A", "BYTES"))
        Traceback (most recent call last):
          ...
        bigquery_frame.exceptions.AnalysisException: Pivoted columns type BYTES can not be automatically inferred.
        Their values must be provided manually as aliased columns via the pivoted_columns argument.

        Create a literal from a list.
    """  # noqa E502
    if col is None:
        return Column("NULL").alias("NULL")
    elif isinstance(col, Column):
        return col
    elif isinstance(col, str):
        safe_col = col.replace('"', r"\"")
        return Column(f'''r"""{safe_col}"""''').alias(quote(col))
    elif isinstance(col, bool):
        return Column(str(col)).alias(str(col).lower())
    elif isinstance(col, int):
        return Column(str(col)).alias(str(col))
    elif isinstance(col, decimal.Decimal):
        allowed_types = ["NUMERIC", "BIGNUMERIC"]
        assert_true(
            field_type in allowed_types,
            UnexpectedException(
                f"field_type is not one of the expected type {allowed_types} but is instead: {field_type}"
            ),
        )
        return Column(f"{field_type} '{str(col)}'").alias(
            str(col).replace(".", "_point_"),
        )
    elif isinstance(col, datetime.date):
        return Column(f"DATE '{col.isoformat()}'").alias(col.isoformat().replace("-", "_"))
    raise AnalysisException(
        f"Pivoted columns type {field_type} "
        f"can not be automatically inferred.\n"
        f"Their values must be provided manually as aliased columns "
        f"via the pivoted_columns argument."
    )


def _get_test_df() -> DataFrame:
    from bigquery_frame.bigquery_builder import BigQueryBuilder

    bq = BigQueryBuilder()
    query = """
        SELECT * FROM UNNEST([
            STRUCT(2 as age, "Alice" as name, 80 as height),
            STRUCT(3 as age, "Alice" as name, 100 as height),
            STRUCT(5 as age, "Bob" as name, 120 as height),
            STRUCT(10 as age, "Bob" as name, 140 as height)
        ])
    """
    return bq.sql(query)
