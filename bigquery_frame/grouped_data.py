from typing import Callable, List

from bigquery_frame import DataFrame, functions
from bigquery_frame.column import Column, StringOrColumn, cols_to_str
from bigquery_frame.dataframe import is_numeric
from bigquery_frame.utils import str_to_cols, strip_margin


class GroupedData:
    def __init__(
        self,
        df: DataFrame,
        grouped_columns: List[StringOrColumn],
    ):
        self.__df = df
        self.__grouped_columns: List[Column] = str_to_cols(grouped_columns)

    def agg(self, *cols: Column) -> DataFrame:
        """Compute aggregates and returns the result as a :class:`DataFrame`.

        The available aggregate functions can be built-in aggregation functions, such as
        `avg`, `max`, `min`, `sum`, `count`.

        Args:
            cols : A list of aggregate :class:`Column` expressions.

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
        if len(self.__grouped_columns) == 0:
            group_by_str = ""
        else:
            group_by_str = f"\nGROUP BY {cols_to_str([col.expr for col in self.__grouped_columns])}"
        query = strip_margin(
            f"""
            |SELECT
            |{cols_to_str(self.__grouped_columns + list(cols), indentation=2)}
            |FROM {self.__df._alias}{group_by_str}"""
        )
        return self.__df._apply_query(query)

    def __numeric_agg(self, agg: Callable[[Column], Column], agg_name: str, *cols: str):
        if len(cols) == 0:
            cols = self.__df.columns
        col_set = set(cols)
        numeric_cols = [field.name for field in self.__df.schema if field.name in col_set and is_numeric(field)]
        return self.agg(*[agg(col).alias(f"{agg_name}_{col}") for col in numeric_cols])

    def avg(self, *cols: str) -> DataFrame:
        """Computes average values for each numeric columns for each group.

        Args:
            *cols: column names. Non-numeric columns are not ignored.

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

            >>> df.groupBy("name").avg('age').sort("name").show()
            +-------+---------+
            |  name | avg_age |
            +-------+---------+
            | Alice |     2.5 |
            |   Bob |     7.5 |
            +-------+---------+

            Calculate the mean of the age and height in all data.

            >>> df.groupBy().avg('age', 'height').show()
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
            *cols: column names. Non-numeric columns are not ignored.

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

            >>> df.groupBy("name").mean('age').sort("name").show()
            +-------+----------+
            |  name | mean_age |
            +-------+----------+
            | Alice |      2.5 |
            |   Bob |      7.5 |
            +-------+----------+

            Calculate the mean of the age and height in all data.

            >>> df.groupBy().mean('age', 'height').show()
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
            *cols: column names. Non-numeric columns are not ignored.

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
        """
        return self.__numeric_agg(functions.max, "max", *cols)

    def min(self, *cols: str) -> DataFrame:
        """Computes the min value for each numeric columns for each group.

        Args:
            *cols: column names. Non-numeric columns are not ignored.

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
        """
        return self.__numeric_agg(functions.min, "min", *cols)

    def sum(self, *cols: str) -> DataFrame:
        """Computes the sum for each numeric columns for each group.

        Args:
            *cols: column names. Non-numeric columns are not ignored.

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
        """
        return self.__numeric_agg(functions.sum, "sum", *cols)


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
