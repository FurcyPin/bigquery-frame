import typing
import warnings
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Optional, TypeVar, Union

from google.cloud.bigquery import Row, SchemaField
from google.cloud.bigquery.table import RowIterator

from bigquery_frame import Column
from bigquery_frame.column import ColumnOrName, cols_to_str
from bigquery_frame.exceptions import AnalysisException
from bigquery_frame.printing import tabulate_results
from bigquery_frame.temp_names import DEFAULT_ALIAS_NAME, _get_alias, _get_temp_column_name
from bigquery_frame.utils import assert_true, indent, list_or_tuple_to_list, quote, str_to_cols, strip_margin

if TYPE_CHECKING:
    from bigquery_frame.bigquery_builder import BigQueryBuilder
    from bigquery_frame.grouped_data import GroupedData

A = TypeVar("A")
B = TypeVar("B")

SUPPORTED_JOIN_TYPES = {
    "inner": "INNER",
    "cross": "CROSS",
    "outer": "FULL OUTER",
    "full": "FULL OUTER",
    "fullouter": "FULL OUTER",
    "full_outer": "FULL OUTER",
    "left": "LEFT OUTER",
    "leftouter": "LEFT OUTER",
    "left_outer": "LEFT OUTER",
    "right": "RIGHT OUTER",
    "rightouter": "RIGHT OUTER",
    "right_outer": "RIGHT OUTER",
    "semi": "LEFT",
    "leftsemi": "LEFT",
    "left_semi": "LEFT",
    "anti": "LEFT",
    "leftanti": "LEFT",
    "left_anti": "LEFT",
}


def is_numeric(schema_field: SchemaField) -> bool:
    return schema_field.field_type in ["INTEGER", "NUMERIC", "BIGNUMERIC", "FLOAT"]


def is_repeated(schema_field: SchemaField) -> bool:
    return schema_field.mode == "REPEATED"


def is_struct(schema_field: SchemaField) -> bool:
    return schema_field.field_type == "RECORD"


def is_nullable(schema_field: SchemaField) -> bool:
    return schema_field.mode == "NULLABLE"


def schema_to_simple_string(schema: list[SchemaField]):
    """Transforms a BigQuery DataFrame schema into a new schema where all structs have been flattened.
    The field names are kept, with a '.' separator for struct fields.
    If `explode` option is set, arrays are exploded with a '!' separator.

    Example:
    >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
    >>> bq = BigQueryBuilder()
    >>> df = bq.sql('SELECT 1 as id, STRUCT(1 as a, [STRUCT(2 as c, 3 as d)] as b, [4, 5] as e) as s')
    >>> print(df.schema)  # noqa: E501
    [SchemaField('id', 'INTEGER', 'NULLABLE', None, None, (), None), SchemaField('s', 'RECORD', 'NULLABLE', None, None, (SchemaField('a', 'INTEGER', 'NULLABLE', None, None, (), None), SchemaField('b', 'RECORD', 'REPEATED', None, None, (SchemaField('c', 'INTEGER', 'NULLABLE', None, None, (), None), SchemaField('d', 'INTEGER', 'NULLABLE', None, None, (), None)), None), SchemaField('e', 'INTEGER', 'REPEATED', None, None, (), None)), None)]
    >>> schema_to_simple_string(df.schema)
    'id:INTEGER,s:STRUCT<a:INTEGER,b:ARRAY<STRUCT<c:INTEGER,d:INTEGER>>,e:ARRAY<INTEGER>>'

    :param schema:
    :return:
    """

    def schema_field_to_simple_string(schema_field: SchemaField):
        if is_struct(schema_field):
            if is_repeated(schema_field):
                return f"ARRAY<STRUCT<{schema_to_simple_string(schema_field.fields)}>>"
            else:
                return f"STRUCT<{schema_to_simple_string(schema_field.fields)}>"
        else:
            if is_repeated(schema_field):
                return f"ARRAY<{schema_field.field_type}>"
            else:
                return schema_field.field_type

    cols = [f"{field.name}:{schema_field_to_simple_string(field)}" for field in schema]
    cols_to_string = ",".join(cols)
    return f"{cols_to_string}"


def schema_to_tree_string(schema: list[SchemaField]) -> str:
    """Generates a string representing the schema in tree format"""

    def str_gen_schema_field(schema_field: SchemaField, prefix: str) -> list[str]:
        res = [f"{prefix}{schema_field.name}: {schema_field.field_type} ({schema_field.mode})"]
        if is_struct(schema_field):
            res += str_gen_schema(schema_field.fields, " |   " + prefix)
        return res

    def str_gen_schema(schema: list[SchemaField], prefix: str) -> list[str]:
        return [str for schema_field in schema for str in str_gen_schema_field(schema_field, prefix)]

    res = ["root"] + str_gen_schema(schema, " |-- ")

    return "\n".join(res) + "\n"


def _dedup_key_value_list(items: list[tuple[A, B]]) -> list[tuple[A, B]]:
    """Deduplicate a list of key, values by their keys.
    Unlike `list(set(l))`, this does preserve ordering.

    Example:
    >>> _dedup_key_value_list([('a', 1), ('b', 2), ('a', 3)])
    [('a', 3), ('b', 2)]

    :param items: an iterable of couples
    :return: an iterable of couples
    """
    return list({k: v for k, v in items}.items())


class DataFrame:
    __deps: list[tuple[str, "DataFrame"]]
    _alias: str

    def __init__(
        self,
        query: str,
        alias: Optional[str],
        bigquery: "BigQueryBuilder",
        deps: Optional[list["DataFrame"]] = None,
    ):
        self.__query = query
        if deps is None:
            deps = []
        deps_with_aliases = [dep for df in deps for dep in df.__deps] + [(df._alias, df) for df in deps]
        self.__deps: list[tuple[str, "DataFrame"]] = _dedup_key_value_list(deps_with_aliases)
        if alias is None:
            alias = _get_alias()
        else:
            bigquery._check_alias(alias, self.__deps)
        self._alias = alias
        self.bigquery: "BigQueryBuilder" = bigquery
        self._schema: Optional[list[SchemaField]] = None
        if self.bigquery.debug:
            self.__validate()

    def __validate(self) -> None:
        """Compiles this :class:`DataFrame's` SQL and send it to BigQuery to validate that the query is correct.
        Used in debug mode.
        """
        self.bigquery._get_query_schema(self.compile())

    def __repr__(self):
        return f"""DataFrame('{self.__query}) as {self._alias}')"""

    def __getitem__(self, item: Union[ColumnOrName, Iterable[ColumnOrName], int]):
        """Returns the column as a :class:`Column`.

        Examples:
        --------
        >>> df = __get_test_df()
        >>> df.select(df['id']).show()
        +----+
        | id |
        +----+
        |  1 |
        |  2 |
        |  3 |
        +----+
        >>> df[["id", "name"]].show()
        +----+-----------+
        | id |      name |
        +----+-----------+
        |  1 | Bulbasaur |
        |  2 |   Ivysaur |
        |  3 |  Venusaur |
        +----+-----------+
        >>> df[df["id"] > 1 ].show()
        +----+----------+
        | id |     name |
        +----+----------+
        |  2 |  Ivysaur |
        |  3 | Venusaur |
        +----+----------+
        >>> df[df[0] > 1].show()
        +----+----------+
        | id |     name |
        +----+----------+
        |  2 |  Ivysaur |
        |  3 | Venusaur |
        +----+----------+
        """
        if isinstance(item, str):
            return Column(f"{quote(self._alias)}.{quote(item)}")
        elif isinstance(item, Column):
            return self.filter(item)
        elif isinstance(item, Iterable):
            return self.select(*item)
        elif isinstance(item, int):
            return Column(f"{quote(self._alias)}.{quote(self.schema[item].name)}")
        else:
            raise TypeError("unexpected item type: %s" % type(item))

    def _apply_query(self, query: str, deps: Optional[list["DataFrame"]] = None) -> "DataFrame":
        if deps is None:
            deps = [self]
        return DataFrame(query, None, self.bigquery, deps=deps)

    def _compute_schema(self) -> list[SchemaField]:
        return self.bigquery._get_query_schema(self.compile())

    def _compile_deps(self) -> dict[str, str]:
        return {
            alias: strip_margin(
                f"""{quote(alias)} AS (
                |{indent(cte.__query, 2)}
                |)""",
            )
            for (alias, cte) in self.__deps
        }

    def _compile_with_ctes(self, ctes: dict[str, str]) -> str:
        if len(ctes) > 0:
            query = "WITH " + "\n, ".join(ctes.values()) + "\n" + self.__query
        else:
            query = self.__query
        deps_replacements = {
            alias[1:-1]: DEFAULT_ALIAS_NAME.format(num=i + 1)
            for (i, (alias, _)) in enumerate(ctes.items())
            if alias[0] == "{" and alias[-1] == "}"
        }
        return query.format(**deps_replacements)

    def _compile_with_deps(self) -> str:
        ctes = self._compile_deps()
        return self._compile_with_ctes(ctes)

    @property
    def columns(self) -> list[str]:
        """Returns all column names as a list."""
        return [field.name for field in self.schema]

    @property
    def schema(self) -> list[SchemaField]:
        """Returns the schema of this :class:`DataFrame` as a list of :class:`google.cloud.bigquery.SchemaField`."""
        if self._schema is None:
            self._schema = self._compute_schema()
        return self._schema

    def agg(self, *cols: Column) -> "DataFrame":
        """Aggregate on the entire :class:`DataFrame` without groups (shorthand for `df.groupBy().agg()`).

        Args:
            *cols: Columns or expressions to aggregate DataFrame by.

        Returns:
            Aggregated DataFrame.

        Examples:
            >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
            >>> from bigquery_frame import functions as f
            >>> bq = BigQueryBuilder()
            >>> df = bq.sql('''
            ...     SELECT 2 as age, "Alice" as name
            ...     UNION ALL
            ...     SELECT 5 as age, "Bob" as name
            ...  ''')
            >>> df.agg(f.max("age").alias("max_age"), f.min("age").alias("min_age")).show()
            +---------+---------+
            | max_age | min_age |
            +---------+---------+
            |       5 |       2 |
            +---------+---------+
        """
        return self.groupBy().agg(*cols)  # type: ignore[arg-type]

    def alias(self, alias) -> "DataFrame":
        """Returns a new :class:`DataFrame` with an alias set."""
        return DataFrame(self.__query, alias, self.bigquery, deps=[df for alias, df in self.__deps])

    def collect(self) -> list[Row]:
        """Returns all the records as list of :class:`Row`."""
        return list(self.bigquery._execute_query(self.compile()))

    def collect_iterator(self) -> RowIterator:
        """Returns all the records as :class:`RowIterator`."""
        return self.bigquery._execute_query(self.compile())

    def count(self) -> int:
        """Returns the number of rows in this :class:`DataFrame`."""
        query = f"SELECT COUNT(1) FROM {quote(self._alias)}"
        return self._apply_query(query).collect()[0][0]

    def compile(self) -> str:
        """Returns the sql query that will be executed to materialize this :class:`DataFrame`"""
        ctes = {**self.bigquery._compile_views(), **self._compile_deps()}
        return self._compile_with_ctes(ctes)

    def createOrReplaceTempTable(self, alias: str) -> None:
        """Creates or replace a persisted temporary table.

        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql("SELECT 1 as id")
        >>> df.createOrReplaceTempTable("temp_table")
        >>> bq.sql("SELECT * FROM temp_table").show()
        +----+
        | id |
        +----+
        |  1 |
        +----+

        :return: a new :class:`DataFrame`
        """
        self.bigquery._registerDataFrameAsTempTable(self, alias)

    def createOrReplaceTempView(self, name: str) -> None:
        """Creates or replaces a local temporary view with this :class:`DataFrame`.

        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql("SELECT 1 as id")
        >>> df.createOrReplaceTempView("temp_view")
        >>> bq.sql("SELECT * FROM temp_view").show()
        +----+
        | id |
        +----+
        |  1 |
        +----+

        Limitations compared to Spark
        -----------------------------
        Replacing an existing temp view with a new one will make the old view inaccessible to future instruction
        (like Spark), but also to past instructions (unlike Spark). It means that the newly defined view cannot
        derive from the view it is replacing. For instance, the following code works in Spark but not here :

        >>> bq.sql("SELECT 2 as id").createOrReplaceTempView("temp_view")
        >>> bq.sql("SELECT * FROM temp_view").show()
        +----+
        | id |
        +----+
        |  2 |
        +----+
        >>> bq.sql("SELECT * FROM temp_view").createOrReplaceTempView("temp_view")
        >>> import google
        >>> try:  #doctest: +ELLIPSIS
        ...   bq.sql("SELECT * FROM temp_view").show()
        ... except google.api_core.exceptions.BadRequest as e:
        ...   print(e)
        400 Table "temp_view" must be qualified with a dataset (e.g. dataset.table)...
        ...

        In this project, temporary views are implemented as CTEs in the final compiled query.
        As such, BigQuery does not allow to define two CTEs with the same name.

        :param name: Name of the temporary view. It must contain only alphanumeric and lowercase characters, no dots.
        :return: Nothing
        """
        self.bigquery._registerDataFrameAsTempView(self, name)

    def distinct(self) -> "DataFrame":
        """Returns a new :class:`DataFrame` containing the distinct rows in this :class:`DataFrame`.

        Limitations compared to Spark
        -----------------------------
        In BigQuery, the DISTINCT statement does not work on complex types like STRUCT and ARRAY.
        """
        query = f"""SELECT DISTINCT * FROM {quote(self._alias)}"""
        return self._apply_query(query)

    def drop(self, *cols: str) -> "DataFrame":
        """Returns a new :class:`DataFrame` that drops the specified column.
        This is a no-op if schema doesn't contain the given column name(s).
        """
        schema_cols = set(self.columns)
        cols_in_schema = [col for col in cols if col in schema_cols]
        if len(cols_in_schema) == 0:
            return self
        else:
            query = strip_margin(
                f"""SELECT
                |  * EXCEPT ({cols_to_str(cols_in_schema)})
                |FROM {quote(self._alias)}""",
            )
            return self._apply_query(query)

    def filter(self, condition: ColumnOrName) -> "DataFrame":
        """Filters rows using the given condition."""
        query = strip_margin(
            f"""
            |SELECT *
            |FROM {quote(self._alias)}
            |WHERE {condition!s}""",
        )
        return self._apply_query(query)

    def groupBy(self, *cols: Union[ColumnOrName, list[ColumnOrName]]) -> "GroupedData":
        """Groups the :class:`DataFrame` using the specified columns, so we can run aggregation on them.
        See :class:`GroupedData` for all the available aggregate functions.

        Args:
            *cols: Columns to group by.
                Each element should be a column name (string) or an expression (:class:`Column`).

        Returns:
            Grouped data by given columns.

        Examples:
            >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
            >>> bq = BigQueryBuilder()
            >>> df = bq.sql('''
            ...     SELECT * FROM UNNEST([
            ...         STRUCT(2 as age, "Alice" as name, 80 as height),
            ...         STRUCT(2 as age, "Alice" as name, 100 as height),
            ...         STRUCT(2 as age, "Bob" as name, 120 as height),
            ...         STRUCT(5 as age, "Bob" as name, 140 as height)
            ...     ])
            ... ''')
            >>> df.show()
            +-----+-------+--------+
            | age |  name | height |
            +-----+-------+--------+
            |   2 | Alice |     80 |
            |   2 | Alice |    100 |
            |   2 |   Bob |    120 |
            |   5 |   Bob |    140 |
            +-----+-------+--------+

            Empty grouping columns triggers a global aggregation.

            >>> df.groupBy().avg().show()
            +---------+------------+
            | avg_age | avg_height |
            +---------+------------+
            |    2.75 |      110.0 |
            +---------+------------+

            Group-by 'name', and calculate maximum values.

            >>> df.groupBy(df["name"]).max().sort("name").show()
            +-------+---------+------------+
            |  name | max_age | max_height |
            +-------+---------+------------+
            | Alice |       2 |        100 |
            |   Bob |       5 |        140 |
            +-------+---------+------------+

            Group-by 'name' and 'age', and calculate the number of rows in each group.

            >>> df.groupBy(["name", df["age"]]).count().sort("name", "age").show()
            +-------+-----+-------+
            |  name | age | count |
            +-------+-----+-------+
            | Alice |   2 |     2 |
            |   Bob |   2 |     1 |
            |   Bob |   5 |     1 |
            +-------+-----+-------+
        """
        cols = list_or_tuple_to_list(*cols)
        from bigquery_frame.grouped_data import GroupedData

        return GroupedData(self, cols)

    def join(
        self,
        other: "DataFrame",
        on: Optional[Union[list[ColumnOrName], ColumnOrName]] = None,
        how: Optional[str] = None,
    ):
        """Joins with another :class:`DataFrame`, using the given join expression.

        !!! warning: "Limitations compared to Spark"
            This method has several limitations when compared to Spark:
            - When doing a select after a join, table prefixes MUST always be used on column names.
              For this reason, users SHOULD always make sure the DataFrames they are joining on are properly aliased
            - When chaining multiple joins, the name of the first DataFrame is not available in the select clause

            Some of these limitations might be removed in future versions.

        Args:
            other: Right side of the join
            on: A string for the join column name, a list of column names,
                a join expression (Column), or a list of Columns.
                If `on` is a string or a list of strings indicating the name of the join column(s),
                the column(s) must exist on both sides, and this performs an equi-join.
            how: (default `inner`). Must be one of: `inner`, `cross`, `outer`,
            `full`, `fullouter`, `full_outer`, `left`, `leftouter`, `left_outer`,
            `right`, `rightouter`, `right_outer`, `semi`, `leftsemi`, `left_semi`,
            `anti`, `leftanti` and `left_anti`.

        Returns:
            Joined DataFrame.

        Examples:
            The following performs a full outer join between ``df1`` and ``df2``.
            >>> from bigquery_frame import functions as f
            >>> df1, df2, df3, df4 = __get_test_dfs()
            >>> df1 = df1.alias("df1")
            >>> df2 = df2.alias("df2")
            >>> df3 = df3.alias("df3")
            >>> df4 = df4.alias("df4")

            >>> df1.show()
            +-----+-------+
            | age |  name |
            +-----+-------+
            |   2 | Alice |
            |   5 |   Bob |
            +-----+-------+
            >>> df2.show()
            +--------+------+
            | height | name |
            +--------+------+
            |     80 |  Tom |
            |     85 |  Bob |
            +--------+------+
            >>> df3.show()
            +-----+-------+
            | age |  name |
            +-----+-------+
            |   2 | Alice |
            |   5 |   Bob |
            +-----+-------+
            >>> df4.show()
            +------+--------+-------+
            |  age | height |  name |
            +------+--------+-------+
            |   10 |     80 | Alice |
            |    5 |   null |   Bob |
            | null |   null |   Tom |
            | null |   null |  null |
            +------+--------+-------+
            >>> (df1.join(df2, f.col("df1.name") == f.col("df2.name"), "left")
            ...      .select(f.col("df1.name"), f.col("df2.height"))
            ...      .sort(f.desc("name"))).show()
            +-------+--------+
            |  name | height |
            +-------+--------+
            |   Bob |     85 |
            | Alice |   null |
            +-------+--------+
            >>> df1.join(df2, "name", "left").select(f.col("df1.name"), f.col("df2.height")).sort(f.desc("name")).show()
            +-------+--------+
            |  name | height |
            +-------+--------+
            |   Bob |     85 |
            | Alice |   null |
            +-------+--------+

            >>> df1.join(df2, 'name', 'outer').select('df1.name', 'df2.height').sort(f.desc("name")).show()
            +-------+--------+
            |  name | height |
            +-------+--------+
            |   Bob |     85 |
            | Alice |   null |
            |  null |     80 |
            +-------+--------+

            >>> df1.join(df2, 'name', 'semi').select('df1.name').sort(f.desc("name")).show()
            +-------+
            |  name |
            +-------+
            |   Bob |
            | Alice |
            +-------+

            >>> df1.join(df2, 'name', 'anti').select('df1.name').sort(f.desc("name")).show()
            +-------+
            |  name |
            +-------+
            | Alice |
            +-------+

            >>> df1.join(df2, 'name').select("df1.name", "df2.height").show()
            +------+--------+
            | name | height |
            +------+--------+
            |  Bob |     85 |
            +------+--------+
            >>> df1.join(df4, ["name", "age"]).select("df1.name", "df1.age").show()
            +------+-----+
            | name | age |
            +------+-----+
            |  Bob |   5 |
            +------+-----+
            >>> df1.join(
            ...     df4,
            ...     on = [df1["name"] == df4["name"], df1["age"] == df4["age"]]
            ... ).select("df1.name", "df1.age").show()
            +------+-----+
            | name | age |
            +------+-----+
            |  Bob |   5 |
            +------+-----+

        Examples: Examples of limitations compared to Spark

            Unlike in Spark, the following examples do not work:
            - Not specifying the table prefix in the select clause after a join (even for "JOIN ... USING (...)")
            >>> df1.join(df2, "name", 'outer').select("name").show()  # doctest: +ELLIPSIS
            Traceback (most recent call last):
              ...
            google.api_core.exceptions.BadRequest: 400 ...

            - When chaining multiple joins, the name of the first DataFrame is not available in the select clause
            >>> df1.join(df2, "name").join(df3, "name").select("df1.name").show()  # doctest: +ELLIPSIS
            Traceback (most recent call last):
              ...
            google.api_core.exceptions.BadRequest: 400 ...

            To work around this, one can do:
            >>> df12 = df1.join(df2, "name").select("df1.*", "df2.height").alias("df12")
            >>> df12.join(df3, "name").select("df12.name", "df12.height", "df3.age").show()
            +------+--------+-----+
            | name | height | age |
            +------+--------+-----+
            |  Bob |     85 |   5 |
            +------+--------+-----+
        """
        # The ANTI JOIN syntax doesn't exist in BigQuery, so to simulate it we add an extra column that is always
        # equal to 1 in the other DataFrame and apply a filter where this column is NULL
        anti_join_presence_col_name = _get_temp_column_name()
        semi = False
        anti = False
        if how is not None:
            how = how.lower()
            assert_true(
                how in SUPPORTED_JOIN_TYPES,
                f"join_type should be one of the following [{', '.join(SUPPORTED_JOIN_TYPES.keys())}]",
            )
            join_str = SUPPORTED_JOIN_TYPES[how] + " JOIN"
            if how.endswith("semi"):
                semi = True
            if how.endswith("anti"):
                anti = True
                other = other.withColumn(anti_join_presence_col_name, Column("TRUE"))
        else:
            join_str = "JOIN"
        on_clause = ""
        if on is not None:
            on = list_or_tuple_to_list(on)
            if len(on) > 0:
                if isinstance(on[0], Column):
                    bool_clause = on[0]
                    for col in on[1:]:
                        bool_clause &= col
                    on_clause = f"\nON {bool_clause}"
                else:
                    on_clause = f"\nUSING ({cols_to_str(on)})"
        self_short_alias = quote(self._alias.replace("`", "").split(".")[-1])
        other_short_alias = quote(other._alias.replace("`", "").split(".")[-1])
        if semi or anti:
            selected_columns = [self_short_alias]
        else:
            selected_columns = [self_short_alias, other_short_alias]
        if anti:
            where_str = f"WHERE {other_short_alias}.{anti_join_presence_col_name} IS NULL"
        else:
            where_str = ""
        query = strip_margin(
            f"""
            |SELECT
            |{cols_to_str(selected_columns, 2)}
            |FROM {quote(self._alias)}
            |{join_str} {quote(other._alias)}{on_clause}
            |{where_str}""",
        )
        return self._apply_query(query, deps=[self, other])

    def limit(self, num: int) -> "DataFrame":
        """Returns a new :class:`DataFrame` with a result count limited to the specified number of rows."""
        query = f"""SELECT * FROM {quote(self._alias)} LIMIT {num}"""
        return self._apply_query(query)

    def persist(self) -> "DataFrame":
        """Persist the contents of the :class:`DataFrame` in a temporary table and returns a new DataFrame reading
        from that table.

        Limitations compared to Spark
        -----------------------------
        Unlike with Spark, this operation is an action and the temporary table is created immediately.
        Like Spark, however, the current DataFrame is not persisted, the new DataFrame returned by this
        method must be used.

        :return: a new :class:`DataFrame`
        """
        return self.bigquery._registerDataFrameAsTempTable(self)

    def printSchema(self) -> None:
        """Prints out the schema in tree format.

        Examples:
        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
            >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''SELECT 1 as id, STRUCT(1 as a, [STRUCT(1 as c)] as b) as s''')
        >>> df.printSchema()
        root
         |-- id: INTEGER (NULLABLE)
         |-- s: RECORD (NULLABLE)
         |    |-- a: INTEGER (NULLABLE)
         |    |-- b: RECORD (REPEATED)
         |    |    |-- c: INTEGER (NULLABLE)
        <BLANKLINE>

        """
        print(self.treeString())

    def print_query(self) -> None:
        """Prints out the SQL query generated to materialize this :class:`DataFrame`.

        Examples:
        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> from bigquery_frame import functions as f
        >>> df = (
        ...     bq.sql('''SELECT 1 as a''')
        ...     .select('a', f.col('a') + f.lit(1).alias('b'))
        ...     .withColumn('c', f.expr('a + b'))
        ... )
        >>> df.print_query()
        WITH `_default_alias_1` AS (
          SELECT 1 as a
        )
        , `_default_alias_2` AS (
          SELECT
            `a`,
            (`a`) + (1)
          FROM `_default_alias_1`
        )
        SELECT
          `_default_alias_2`.*,
          a + b AS `c`
        FROM `_default_alias_2`
        """
        print(self.compile())

    def _select_with_exploded_columns(self, cols: list[ColumnOrName]) -> "DataFrame":
        from bigquery_frame.column import ExplodedColumn

        exploded_cols = [col for col in cols if isinstance(col, ExplodedColumn)]

        def exploded_col_to_unnest_str(col: ExplodedColumn) -> str:
            alias = col.get_alias()
            assert_true(alias is not None, AnalysisException("Exploded columns must be aliased."))
            join_str = "LEFT OUTER JOIN" if col.outer else "INNER JOIN"
            pos_str = f" WITH OFFSET as {quote(alias + '_pos')}" if col.with_index else ""
            return f"{join_str} UNNEST({col.exploded_col.expr}) AS {alias}{pos_str}"

        def col_to_select(col: Column) -> list[ColumnOrName]:
            if isinstance(col, ExplodedColumn):
                alias = col.get_alias()
                if col.with_index:
                    return [alias, quote(alias + "_pos")]
                else:
                    return [alias]
            else:
                return [col]

        cols = [c for col in cols for c in col_to_select(col)]
        unnest_clause = [exploded_col_to_unnest_str(col) for col in exploded_cols]
        cols = str_to_cols(cols)
        query = strip_margin(
            f"""SELECT
            |{cols_to_str(cols, 2)}
            |FROM {quote(self._alias)}
            |{cols_to_str(unnest_clause, indentation=0, sep="")}
            | """,
        )
        return self._apply_query(query)

    def select(self, *columns: Union[ColumnOrName, list[ColumnOrName]]) -> "DataFrame":
        """Projects a set of expressions and returns a new :class:`DataFrame`.

        Args:
            *columns: Column names (string) or expressions (:class:`Column`), or list of those.
                If one of the column names is '*', that column is expanded to include all columns
                in the current :class:`DataFrame`.

        Returns:
            A DataFrame with subset (or all) of columns.

        Examples:
        >>> from bigquery_frame import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql("SELECT * FROM UNNEST([STRUCT(2 as age, 'Alice' as name), STRUCT(5 as age, 'Bob' as name)])")

        Select all columns in the DataFrame.

        >>> df.select('*').show()
        +-----+-------+
        | age |  name |
        +-----+-------+
        |   2 | Alice |
        |   5 |   Bob |
        +-----+-------+

        Select a column with other expressions in the DataFrame.

        >>> df.select(df["name"], (df["age"] + 10).alias('age')).show()
        +-------+-----+
        |  name | age |
        +-------+-----+
        | Alice |  12 |
        |   Bob |  15 |
        +-------+-----+
        """
        cols = list_or_tuple_to_list(*columns)
        if _has_exploded_columns(cols):
            return self._select_with_exploded_columns(cols)
        else:
            cols = str_to_cols(cols)
            query = strip_margin(
                f"""SELECT
                |{cols_to_str(cols, 2)}
                |FROM {quote(self._alias)}""",
            )
            return self._apply_query(query)

    def select_nested_columns(self, fields: Mapping[str, ColumnOrName]) -> "DataFrame":
        """Projects a set of expressions and returns a new :class:`DataFrame`.
        Unlike the :func:`select` method, this method works on repeated elements
        and records (arrays and arrays of struct).

        The syntax for column names works as follows:
        - "." is the separator for struct elements
        - "!" must be appended at the end of fields that are repeated
        - Corresponding column expressions must use the complete field names for non-repeated structs
        - Corresponding column expressions must use short field names for repeated structs
        - Corresponding column expressions must use the name `_` for array elements

        !!! warning
            This method is deprecated since version 0.5.0 and will be removed in version 0.6.0.
            Please use bigquery_frame.nested.select instead.


        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> from bigquery_frame import functions as f
        >>> df = bq.sql('''
        ...  SELECT * FROM UNNEST([
        ...    STRUCT(1 as id, STRUCT(1 as a, 2 as b) as s)
        ...  ])
        ... ''')
        >>> df.show()
        +----+------------------+
        | id |                s |
        +----+------------------+
        |  1 | {'a': 1, 'b': 2} |
        +----+------------------+
        >>> df.select_nested_columns({
        ...     "id": "id",
        ...     "s.c": f.col("s.a") + f.col("s.b")
        ... }).show()
        +----+----------+
        | id |        s |
        +----+----------+
        |  1 | {'c': 3} |
        +----+----------+

        >>> from bigquery_frame import functions as f
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''
        ...  SELECT * FROM UNNEST([
        ...    STRUCT(1 as id, [STRUCT(1 as a, 2 as b), STRUCT(3 as a, 4 as b)] as s)
        ...  ])
        ... ''')
        >>> df.show()
        +----+--------------------------------------+
        | id |                                    s |
        +----+--------------------------------------+
        |  1 | [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}] |
        +----+--------------------------------------+
        >>> df.select_nested_columns({"s!.c": f.col("a") + f.col("b")}).show()
        +----------------------+
        |                    s |
        +----------------------+
        | [{'c': 3}, {'c': 7}] |
        +----------------------+

        >>> df = bq.sql('''
        ...  SELECT * FROM UNNEST([
        ...    STRUCT(1 as id, [STRUCT([1, 2, 3] as e)] as s)
        ...  ])
        ... ''')
        >>> df.show()
        +----+--------------------+
        | id |                  s |
        +----+--------------------+
        |  1 | [{'e': [1, 2, 3]}] |
        +----+--------------------+
        >>> df.select_nested_columns({"s!.e!": lambda c: c.cast("FLOAT64")}).show()
        +--------------------------+
        |                        s |
        +--------------------------+
        | [{'e': [1.0, 2.0, 3.0]}] |
        +--------------------------+

        :param fields: a Dict[column_alias, column_expression] of columns to select
        :return:
        """
        warning_message = (
            "The method DataFrame.select_nested_columns is deprecated since version 0.5.0 "
            "and will be removed in version 0.6.0. "
            "Please use bigquery_frame.nested.select instead."
        )
        warnings.warn(warning_message, category=DeprecationWarning)
        from bigquery_frame import nested

        return nested.select(self, fields)

    def show(self, n: int = 20, format_args=None, simplify_structs=False) -> None:
        """Prints the first ``n`` rows to the console. This uses the awesome Python library called `tabulate
        <https://pythonrepo.com/repo/astanin-python-tabulate-python-generating-and-working-with-logs>`_.

        Formating options may be set using `format_args`.

        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''SELECT 1 as id, STRUCT(1 as a, [STRUCT(1 as c)] as b) as s''')
        >>> df.show()
        +----+---------------------------+
        | id |                         s |
        +----+---------------------------+
        |  1 | {'a': 1, 'b': [{'c': 1}]} |
        +----+---------------------------+
        >>> df.show(format_args={"tablefmt": 'fancy_grid'})
        ╒══════╤═══════════════════════════╕
        │   id │ s                         │
        ╞══════╪═══════════════════════════╡
        │    1 │ {'a': 1, 'b': [{'c': 1}]} │
        ╘══════╧═══════════════════════════╛
        >>> df.show(simplify_structs=True)
        +----+------------+
        | id |          s |
        +----+------------+
        |  1 | {1, [{1}]} |
        +----+------------+

        :param n: Number of rows to show.
        :param format_args: extra arguments that may be passed to the function tabulate.tabulate()
        :param simplify_structs: if set to true, struct field names are not displayed
        :return: None
        """
        print(self.show_string(n, format_args, simplify_structs))

    def show_string(self, n: int = 20, format_args=None, simplify_structs=False) -> str:
        """Returns a string representation of the first ``n`` rows.
        This is equivalent to `DataFrame.show()` but it returns the result instead of printing it.

        This uses the awesome Python library called `tabulate
        <https://pythonrepo.com/repo/astanin-python-tabulate-python-generating-and-working-with-logs>`_.

        Formating options may be set using `format_args`.

        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''SELECT 1 as id, STRUCT(1 as a, [STRUCT(1 as c)] as b) as s''')
        >>> print(df.show_string())
        +----+---------------------------+
        | id |                         s |
        +----+---------------------------+
        |  1 | {'a': 1, 'b': [{'c': 1}]} |
        +----+---------------------------+
        >>> print(df.show_string(format_args={"tablefmt": 'fancy_grid'}))
        ╒══════╤═══════════════════════════╕
        │   id │ s                         │
        ╞══════╪═══════════════════════════╡
        │    1 │ {'a': 1, 'b': [{'c': 1}]} │
        ╘══════╧═══════════════════════════╛
        >>> print(df.show_string(simplify_structs=True))
        +----+------------+
        | id |          s |
        +----+------------+
        |  1 | {1, [{1}]} |
        +----+------------+

        :param n: Number of rows to show.
        :param format_args: extra arguments that may be passed to the function tabulate.tabulate()
        :param simplify_structs: if set to true, struct field names are not displayed
        :return: None
        """
        res = self.limit(n + 1).collect_iterator()
        return tabulate_results(res, format_args, limit=n, simplify_structs=simplify_structs)

    def sort(self, *cols: Union[ColumnOrName, list[ColumnOrName]]):
        """Returns a new :class:`DataFrame` sorted by the specified column(s).

        Args:
            *cols: List of :class:`Column` or column names to sort by.

        Returns:
            Sorted DataFrame.

        Examples:
            >>> df = __get_test_df_2()
            >>> df.show()
            +------+-------------+
            | name | nb_children |
            +------+-------------+
            | John |           0 |
            | Mary |           1 |
            | Jack |           1 |
            +------+-------------+
            >>> from bigquery_frame import functions as f
            >>> df.sort("name").show()
            +------+-------------+
            | name | nb_children |
            +------+-------------+
            | Jack |           1 |
            | John |           0 |
            | Mary |           1 |
            +------+-------------+
            >>> df.sort(["nb_children", df["name"]]).show()
            +------+-------------+
            | name | nb_children |
            +------+-------------+
            | John |           0 |
            | Jack |           1 |
            | Mary |           1 |
            +------+-------------+
        """
        cols = list_or_tuple_to_list(*cols)
        str_cols = [col.expr for col in str_to_cols(cols)]
        query = strip_margin(
            f"""
            |SELECT *
            |FROM {quote(self._alias)}
            |ORDER BY {cols_to_str(str_cols)}""",
        )
        return self._apply_query(query)

    def take(self, num):
        """Returns the first ``num`` rows as a :class:`list` of :class:`Row`."""
        return self.limit(num).collect()

    def toPandas(self, **kwargs):
        """Returns the contents of this :class:`DataFrame` as Pandas :class:`pandas.DataFrame`.

        This method requires to have following extra dependencies installed
        - pandas
        - pyarrow
        - db-dtypes

        Optional extra arguments (kwargs) will be passed directly to the
        :func:`bigquery.table.RowIterator.to_dataframe` method.
        Please check its documentation for further information.

        By default, the BigQuery client will use the BigQuery Storage API to download data faster.
        This requires to have the extra role "BigQuery Read Session User".
        You can disable this behavior and use the regular, slower download method (which does not require additionnal
        rights) by passing the argument `create_bqstorage_client=False`.

        >>> df = __get_test_df()
        >>> import tabulate
        >>> pdf = df.toPandas()
        >>> type(pdf)
        <class 'pandas.core.frame.DataFrame'>
        >>> pdf
           id       name
        0   1  Bulbasaur
        1   2    Ivysaur
        2   3   Venusaur

        """
        return self.collect_iterator().to_dataframe(**kwargs)

    def transform(
        self,
        func: typing.Callable[..., "DataFrame"],
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> "DataFrame":
        """Returns a new :class:`DataFrame`. Concise syntax for chaining custom transformations.

        Args:
            func: a function that takes and returns a :class:`DataFrame`.
            *args: Positional arguments to pass to func.
            **kwargs: Keyword arguments to pass to func.

        Returns:
            Transformed DataFrame.

        Examples:
            >>> from bigquery_frame import BigQueryBuilder
            >>> from bigquery_frame import functions as f
            >>> bq = BigQueryBuilder()
            >>> df = bq.sql("SELECT * FROM UNNEST([STRUCT(1 as int, 1.0 as float), STRUCT(2 as int, 2.0 as float)])")
            >>> def cast_all_to_int(input_df):
            ...     return input_df.select(
            ...         [f.col(col_name).cast("int").alias(col_name) for col_name in input_df.columns]
            ...     )
            ...
            >>> def sort_columns_asc(input_df):
            ...     return input_df.select(*sorted(input_df.columns))
            ...
            >>> df.transform(cast_all_to_int).transform(sort_columns_asc).show()
            +-------+-----+
            | float | int |
            +-------+-----+
            |     1 |   1 |
            |     2 |   2 |
            +-------+-----+

            >>> def add_n(input_df, n):
            ...     return input_df.select([(f.col(col_name) + n).alias(col_name) for col_name in input_df.columns])
            ...
            >>> df.transform(add_n, 1).transform(add_n, n=10).show()
            +-----+-------+
            | int | float |
            +-----+-------+
            |  12 |  12.0 |
            |  13 |  13.0 |
            +-----+-------+
        """
        result = func(self, *args, **kwargs)
        assert isinstance(
            result,
            DataFrame,
        ), "Func returned an instance of type [%s], should have been DataFrame." % type(result)
        return result

    def treeString(self):
        """Generates a string representing the schema in tree format"""
        return schema_to_tree_string(self.schema)

    def union(self, other: "DataFrame") -> "DataFrame":
        """Return a new :class:`DataFrame` containing union of rows in this and another :class:`DataFrame`.

        This is equivalent to `UNION ALL` in SQL. To do a SQL-style `UNION DISTINCT`
        (that does deduplication of elements), use this function followed by :func:`distinct`.

        Also as standard in SQL, this function resolves columns by position (not by name).
        To get column resolution by name, use the function `unionByName` instead.

        :param other:
        :return: a new :class:`DataFrame`
        """
        query = f"""SELECT * FROM {quote(self._alias)} UNION ALL SELECT * FROM {quote(other._alias)}"""
        return self._apply_query(query, deps=[self, other])

    def unionByName(self, other: "DataFrame", allowMissingColumns: bool = False) -> "DataFrame":
        """Returns a new :class:`DataFrame` containing union of rows in this and another :class:`DataFrame`.

        This is different from both `UNION ALL` and `UNION DISTINCT` in SQL. To do a SQL-style set
        union (that does deduplication of elements), use this function followed by :func:`distinct`.

        Examples:
        --------
        The difference between this function and :func:`union` is that this function
        resolves columns by name (not by position):

        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
            >>> bq = BigQueryBuilder()
        >>> df1 = bq.sql('''SELECT 1 as col0, 2 as col1, 3 as col2''')
        >>> df2 = bq.sql('''SELECT 4 as col1, 5 as col2, 6 as col0''')
        >>> df1.union(df2).show()
        +------+------+------+
        | col0 | col1 | col2 |
        +------+------+------+
        |    1 |    2 |    3 |
        |    4 |    5 |    6 |
        +------+------+------+
        >>> df1.unionByName(df2).show()
        +------+------+------+
        | col0 | col1 | col2 |
        +------+------+------+
        |    1 |    2 |    3 |
        |    6 |    4 |    5 |
        +------+------+------+

        When the parameter `allowMissingColumns` is ``True``, the set of column names
        in this and other :class:`DataFrame` can differ; missing columns will be filled with null.
        Further, the missing columns of this :class:`DataFrame` will be added at the end
        in the schema of the union result:

        >>> df1 = bq.sql('''SELECT 1 as col0, 2 as col1, 3 as col2''')
        >>> df2 = bq.sql('''SELECT 4 as col1, 5 as col2, 6 as col3''')
        >>> df1.unionByName(df2, allowMissingColumns=True).show()
        +------+------+------+------+
        | col0 | col1 | col2 | col3 |
        +------+------+------+------+
        |    1 |    2 |    3 | null |
        | null |    4 |    5 |    6 |
        +------+------+------+------+

        :param other: Another DataFrame
        :param allowMissingColumns: If False, the columns that are not in both DataFrames are dropped, if True,
          they are added to the other DataFrame as null values.
        :return: a new :class:`DataFrame`
        """
        self_cols = self.columns
        other_cols = other.columns
        self_cols_set = set(self.columns)
        other_cols_set = set(other.columns)

        self_only_cols = [col for col in self_cols if col not in other_cols_set]
        common_cols = [col for col in self_cols if col in other_cols_set]
        other_only_cols = [col for col in other_cols if col not in self_cols_set]

        if (len(self_only_cols) > 0 or len(other_only_cols) > 0) and not allowMissingColumns:
            raise ValueError(
                f"UnionByName: dataFrames must have the same columns, "
                f"unless allowMissingColumns is set to True.\n"
                f"Columns in first DataFrame: [{cols_to_str(self.columns)}]\n"
                f"Columns in second DataFrame: [{cols_to_str(other.columns)}]",
            )

        def optional_comma(_list: Sequence[object]):
            return "," if len(_list) > 0 else ""

        query = strip_margin(
            f"""
            |SELECT
            |  {cols_to_str(self_only_cols, 2)}{optional_comma(self_only_cols)}
            |  {cols_to_str(common_cols, 2)}{optional_comma(common_cols)}
            |  {cols_to_str([f"NULL as {col}" for col in other_only_cols], 2)}
            |FROM {quote(self._alias)}
            |UNION ALL
            |SELECT
            |  {cols_to_str([f"NULL as {col}" for col in self_only_cols], 2)}{optional_comma(self_only_cols)}
            |  {cols_to_str(common_cols, 2)}{optional_comma(common_cols)}
            |  {cols_to_str(other_only_cols, 2)}
            |FROM {quote(other._alias)}
            |""",
        )
        return self._apply_query(query, deps=[self, other])

    def withColumn(self, col_name: str, col_expr: Column, replace: bool = False) -> "DataFrame":
        """Returns a new :class:`DataFrame` by adding a column or replacing the existing column that has the same name.

        The column expression must be an expression over this :class:`DataFrame`; attempting to add a column from
        some other :class:`DataFrame` will raise an error.

        Limitations compared to Spark
        -----------------------------
        - Replace an existing column must be done explicitly by passing the argument `replace=True`.
        - Each time this function is called, a new CTE will be created. This may lead to reaching BigQuery's
          query size limit very quickly.

        TODO: This project is just a POC. Future versions may bring improvements to these features but this will require more on-the-fly schema inspections.  # noqa: E501

        Args:
            col_name: Name of the new column.
            col_expr: Expression defining the new column
            replace: Set to true when replacing an already existing column

        Returns:
            DataFrame with new or replaced column.

        Examples:
            >>> from bigquery_frame import BigQueryBuilder
            >>> bq = BigQueryBuilder()
            >>> df = bq.sql('''
            ...     SELECT 2 as age, 'Alice' as name
            ...     UNION ALL
            ...     SELECT 5 as age, 'Bob' as name
            ... ''')
            >>> df.withColumn('age2', df["age"] + 2).show()
            +-----+-------+------+
            | age |  name | age2 |
            +-----+-------+------+
            |   2 | Alice |    4 |
            |   5 |   Bob |    7 |
            +-----+-------+------+
            >>> df.withColumn('age', df["age"] + 2, replace=True).show()
            +-----+-------+
            | age |  name |
            +-----+-------+
            |   4 | Alice |
            |   7 |   Bob |
            +-----+-------+
        """
        from bigquery_frame.column import ExplodedColumn

        assert_true(
            not (isinstance(col_expr, ExplodedColumn) and replace),
            AnalysisException("Exploded columns cannot be used with in withColumn(replace=True)"),
        )
        if replace:
            query = f"SELECT * REPLACE ({col_expr} AS {quote(col_name)}) FROM {quote(self._alias)}"
        else:
            return self.select(f"{self._alias}.*", col_expr.alias(col_name))
        return self._apply_query(query)

    def with_nested_columns(self, fields: dict[str, ColumnOrName]) -> "DataFrame":
        """Returns a new :class:`DataFrame` by adding or replacing (when they already exist) columns.

        Unlike the :func:`withColumn` method, this method works on repeated elements
        and records (arrays and arrays of struct).

        The syntax for column names works as follows:
        - "." is the separator for struct elements
        - "!" must be appended at the end of fields that are repeated
        - Corresponding column expressions must use the complete field names for non-repeated structs
        - Corresponding column expressions must use short field names for repeated structs
        - Corresponding column expressions must use the name `_` for array elements

        !!! warning
            This methtod is deprecated since version 0.5.0 and will be removed in version 0.6.0.
            Please use bigquery_frame.nested.with_fields instead.

        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> bq = BigQueryBuilder()
        >>> from bigquery_frame import functions as f
        >>> df = bq.sql('''
        ...  SELECT * FROM UNNEST([
        ...    STRUCT(1 as id, STRUCT(1 as a, 2 as b) as s)
        ...  ])
        ... ''')
        >>> df.show()
        +----+------------------+
        | id |                s |
        +----+------------------+
        |  1 | {'a': 1, 'b': 2} |
        +----+------------------+
        >>> df.with_nested_columns({
        ...     "id": "id",
        ...     "s.c": f.col("s.a") + f.col("s.b")
        ... }).show()
        +----+--------------------------+
        | id |                        s |
        +----+--------------------------+
        |  1 | {'a': 1, 'b': 2, 'c': 3} |
        +----+--------------------------+

        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
            >>> from bigquery_frame import functions as f
        >>> bq = BigQueryBuilder()
        >>> df = bq.sql('''
        ...  SELECT * FROM UNNEST([
        ...    STRUCT(1 as id, [STRUCT(1 as a, 2 as b), STRUCT(3 as a, 4 as b)] as s)
        ...  ])
        ... ''')
        >>> df.show()
        +----+--------------------------------------+
        | id |                                    s |
        +----+--------------------------------------+
        |  1 | [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}] |
        +----+--------------------------------------+
        >>> df.with_nested_columns({"s!.c": f.col("a") + f.col("b")}).show()
        +----+------------------------------------------------------+
        | id |                                                    s |
        +----+------------------------------------------------------+
        |  1 | [{'a': 1, 'b': 2, 'c': 3}, {'a': 3, 'b': 4, 'c': 7}] |
        +----+------------------------------------------------------+

        >>> df = bq.sql('''
        ...  SELECT * FROM UNNEST([
        ...    STRUCT(1 as id, [STRUCT([1, 2, 3] as e)] as s)
        ...  ])
        ... ''')
        >>> df.show()
        +----+--------------------+
        | id |                  s |
        +----+--------------------+
        |  1 | [{'e': [1, 2, 3]}] |
        +----+--------------------+
        >>> df.with_nested_columns({"s!.e!": lambda c: c.cast("FLOAT64")}).show()
        +----+--------------------------+
        | id |                        s |
        +----+--------------------------+
        |  1 | [{'e': [1.0, 2.0, 3.0]}] |
        +----+--------------------------+

        :param fields: a Dict[column_alias, column_expression] of columns to select
        :return:
        """
        warning_message = (
            "The method DataFrame.with_nested_columns is deprecated since version 0.5.0 "
            "and will be removed in version 0.6.0. "
            "Please use bigquery_frame.nested.with_fields instead."
        )
        warnings.warn(warning_message, category=DeprecationWarning)
        from bigquery_frame import nested

        return nested.with_fields(self, fields)

    @property
    def write(self):
        """Interface for saving the content of the :class:`DataFrame` out into external storage.

        Examples:
        --------
        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> from bigquery_frame.dataframe_writer import __setup_test_dataset, __teardown_test_dataset
        >>> client = get_bq_client()
        >>> bq = BigQueryBuilder(client)
        >>> test_dataset = __setup_test_dataset(client)
        >>> df = bq.sql("SELECT 1 as a")

        >>> df.write.mode('overwrite').save(f"{test_dataset.dataset_id}.my_table")
        >>> df.write.mode('append').save(f"{test_dataset.dataset_id}.my_table")
        >>> bq.table(f"{test_dataset.dataset_id}.my_table").show()
        +---+
        | a |
        +---+
        | 1 |
        | 1 |
        +---+
        >>> __teardown_test_dataset(client, test_dataset)

        """
        from bigquery_frame.dataframe_writer import DataframeWriter

        return DataframeWriter(self)

    orderBy = sort

    unionAll = union

    where = filter


def __get_test_df() -> DataFrame:
    from bigquery_frame.auth import get_bq_client
    from bigquery_frame.bigquery_builder import BigQueryBuilder

    bq = BigQueryBuilder(get_bq_client())
    query = """
        SELECT
            *
        FROM UNNEST ([
            STRUCT(1 as id, "Bulbasaur" as name),
            STRUCT(2 as id, "Ivysaur" as name),
            STRUCT(3 as id, "Venusaur" as name)
        ])
    """
    return bq.sql(query)


def _has_exploded_columns(cols: list[Column]):
    from bigquery_frame.column import ExplodedColumn

    return any(isinstance(col, ExplodedColumn) for col in cols)


def __get_test_dfs() -> tuple[DataFrame, ...]:
    from bigquery_frame.auth import get_bq_client
    from bigquery_frame.bigquery_builder import BigQueryBuilder

    bq = BigQueryBuilder(get_bq_client())
    df1 = bq.sql(
        strip_margin(
            """
        |SELECT 2 as age, "Alice" as name
        |UNION ALL
        |SELECT 5 as age, "Bob" as name
        |""",
        ),
    )
    df2 = bq.sql(
        strip_margin(
            """
        |SELECT 80 as height, "Tom" as name
        |UNION ALL
        |SELECT 85 as height, "Bob" as name
        |""",
        ),
    )
    df3 = df1
    df4 = bq.sql(
        strip_margin(
            """
        |SELECT * FROM UNNEST([
        |    STRUCT(10 as age, 80 as height, "Alice" as name),
        |    STRUCT(5 as age, NULL as height, "Bob" as name),
        |    STRUCT(NULL as age, NULL as height, "Tom" as name),
        |    STRUCT(NULL as age, NULL as height, NULL as name)
        |])
        |""",
        ),
    )
    return df1, df2, df3, df4


def __get_test_df_2() -> DataFrame:
    from bigquery_frame.auth import get_bq_client
    from bigquery_frame.bigquery_builder import BigQueryBuilder

    bq = BigQueryBuilder(get_bq_client())
    query = """
        SELECT
            *
        FROM UNNEST ([
            STRUCT("John" as name, 0 as nb_children),
            STRUCT("Mary" as name, 1 as nb_children),
            STRUCT("Jack" as name, 1 as nb_children)
        ])
    """
    return bq.sql(query)
