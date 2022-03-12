import re
from typing import List, Tuple, Optional, Union, Set, Dict

import google
from google.cloud.bigquery import SchemaField, Client, Row
from google.cloud.bigquery.table import RowIterator

from bigquery_frame.auth import get_bq_client
from bigquery_frame.column import Column
from bigquery_frame.has_bigquery_client import HasBigQueryClient
from bigquery_frame.printing import print_results


Column = Union[str, Column]


def indent(str, nb) -> str:
    return " " * nb + str.replace("\n", "\n" + " " * nb)


def strip_margin(text):
    s = re.sub('\n[ \t]*\|', '\n', text)
    if s.startswith("\n"):
        return s[1:]
    else:
        return s


def quote(str) -> str:
    return "`" + str + "`"


def cols_to_str(cols, indentation: Optional[int] = None) -> str:
    cols = [str(col) for col in cols]
    if indentation is not None:
        return indent(",\n".join(cols), indentation)
    else:
        return ", ".join(cols)


def is_repeated(schema_field: SchemaField):
    return schema_field.mode == "REPEATED"


def is_struct(schema_field: SchemaField):
    return schema_field.field_type == "RECORD"


def is_nullable(schema_field: SchemaField):
    return schema_field.mode == "NULLABLE"


def schema_to_simple_string(schema: List[SchemaField]):
    """Transforms a BigQuery DataFrame schema into a new schema where all structs have been flattened.
    The field names are kept, with a '.' separator for struct fields.
    If `explode` option is set, arrays are exploded with a '!' separator.

    Example:

    >>> bq = BigQueryBuilder(get_bq_client())
    >>> df = bq.sql('SELECT 1 as id, STRUCT(1 as a, [STRUCT(2 as c, 3 as d)] as b, [4, 5] as e) as s')
    >>> print(df.schema)
    [SchemaField('id', 'INTEGER', 'NULLABLE', None, (), None), SchemaField('s', 'RECORD', 'NULLABLE', None, (SchemaField('a', 'INTEGER', 'NULLABLE', None, (), None), SchemaField('b', 'RECORD', 'REPEATED', None, (SchemaField('c', 'INTEGER', 'NULLABLE', None, (), None), SchemaField('d', 'INTEGER', 'NULLABLE', None, (), None)), None), SchemaField('e', 'INTEGER', 'REPEATED', None, (), None)), None)]
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


def schema_to_tree_string(schema: List[SchemaField]) -> str:
    """Generates a string representing the schema in tree format"""

    def str_gen_schema_field(schema_field: SchemaField, prefix: str) -> List[str]:
        res = [f"{prefix}{schema_field.name}: {schema_field.field_type} ({schema_field.mode})"]
        if is_struct(schema_field):
            res += str_gen_schema(schema_field.fields, " |   " + prefix)
        return res

    def str_gen_schema(schema: List[SchemaField], prefix: str) -> List[str]:
        return [
            str
            for schema_field in schema
            for str in str_gen_schema_field(schema_field, prefix)
        ]

    res = ["root"] + str_gen_schema(schema, " |-- ")

    return "\n".join(res) + "\n"


def _dedup_key_value_list(l: List[Tuple[object, object]]):
    """Deduplicate a list of key, values by their keys.
    Unlike `list(set(l))`, this does preserve ordering.

    Example:
    >>> _dedup_key_value_list([('a', 1), ('b', 2), ('a', 3)])
    [('a', 3), ('b', 2)]

    :param l:
    :return:
    """
    return list({k: v for k, v in l}.items())


class BigQueryBuilder(HasBigQueryClient):
    DEFAULT_ALIAS_NAME = "_default_alias_{num}"
    DEFAULT_TEMP_TABLE_NAME = "_default_temp_table_{num}"

    def __init__(self, client: Client):
        super().__init__(client)
        self._alias_count = 0
        self._temp_table_count = 0
        self._views: Dict[str, 'DataFrame'] = {}
        self._temp_tables: Set[str] = set()

    def table(self, full_table_name: str) -> 'DataFrame':
        """Returns the specified table as a :class:`DataFrame`.
        """
        query = f"""SELECT * FROM {quote(full_table_name)}"""
        return DataFrame(query, alias=None, bigquery=self)

    def sql(self, sql_query) -> 'DataFrame':
        """Returns a :class:`DataFrame` representing the result of the given query.
        """
        return DataFrame(sql_query, None, self)

    def _registerDataFrameAsTempView(self, df: 'DataFrame', alias: str) -> None:
        # self._check_alias(alias, [])
        self._views[alias] = df

    def _registerDataFrameAsTempTable(self, df: 'DataFrame', alias: Optional[str]=None) -> 'DataFrame':
        if alias is None:
            alias = self._get_temp_table_alias()
        query = f"CREATE OR REPLACE TEMP TABLE {quote(alias)} AS \n" + df.compile()
        self._execute_query(query)
        return self.table(alias)

    def _compile_views(self) -> List[str]:
        return [
            strip_margin(f"""{quote(alias)} AS (
            |{indent(df._compile_with_deps(), 2)}
            |)""")
            for alias, df in self._views.items()
        ]

    def _get_alias(self) -> str:
        self._alias_count += 1
        return self.DEFAULT_ALIAS_NAME.format(num=self._alias_count)

    def _get_temp_table_alias(self) -> str:
        self._temp_table_count += 1
        return self.DEFAULT_TEMP_TABLE_NAME.format(num=self._temp_table_count)

    def _check_alias(self, new_alias, deps: List[Tuple[str, 'DataFrame']]) -> None:
        """Checks that the alias follows BigQuery constraints, such as:

        - BigQuery does not allow having two CTEs with the same name in a query.

        :param new_alias:
        :param deps:
        :return: None
        :raises: an Exception if something that does not comply with BigQuery's rules is found.
        """
        collisions = [alias for alias, df in list(self._views.items()) + deps if alias == new_alias]
        if len(collisions) > 0:
            raise Exception(f"Duplicate alias {new_alias}")


class DataFrame:

    _deps: List[Tuple[str, 'DataFrame']]
    _alias: str

    def __init__(self, query: str, alias: Optional[str], bigquery: BigQueryBuilder, deps: Optional[List['DataFrame']] = None):
        self.query = query
        if deps is None:
            deps = []
        deps = [dep for df in deps for dep in df._deps] + [(df._alias, df) for df in deps]
        self._deps: List[Tuple[str, 'DataFrame']] = _dedup_key_value_list(deps)
        if alias is None:
            alias = bigquery._get_alias()
        else:
            bigquery._check_alias(alias, self._deps)
        self._alias = alias
        self.bigquery: BigQueryBuilder = bigquery
        self._schema = None

    def __repr__(self):
        return f"""DataFrame('{self.query}) as {self._alias}')"""

    def _apply_query(self, query: str, deps: Optional[List['DataFrame']] = None) -> 'DataFrame':
        if deps is None:
            deps = [self]
        return DataFrame(query, None, self.bigquery, deps=deps)

    def _compute_schema(self):
        df = self.limit(0)
        return df.bigquery._execute_query(df.compile()).schema

    def _compile_deps(self) -> List[str]:
        return [
            strip_margin(f"""{quote(alias)} AS (
            |{indent(cte.query, 2)}
            |)""")
            for (alias, cte) in self._deps
        ]

    def _compile_with_ctes(self, ctes: List[str]) -> str:
        if len(ctes) > 0:
            return "WITH " + "\n, ".join(ctes) + "\n" + self.query
        else:
            return self.query

    def _compile_with_deps(self) -> str:
        ctes = self._compile_deps()
        return self._compile_with_ctes(ctes)

    @property
    def schema(self) -> List[SchemaField]:
        """Returns the schema of this :class:`DataFrame` as a list of :class:`google.cloud.bigquery.SchemaField`."""
        if self._schema is None:
            self._schema = self._compute_schema()
        return self._schema

    def compile(self) -> str:
        """Returns the sql query that will be executed to materialize this :class:`DataFrame`"""
        ctes = self.bigquery._compile_views() + self._compile_deps()
        return self._compile_with_ctes(ctes)

    def alias(self, alias) -> 'DataFrame':
        """Returns a new :class:`DataFrame` with an alias set."""
        return DataFrame(self.query, alias, self.bigquery)

    def persist(self) -> 'DataFrame':
        """Persist the contents of the :class:`DataFrame` in a temporary table and returns a new DataFrame reading
        from that table.

        Limitations compared to Spark
        -----------------------------
        Unlike with Spark, this operation is an action and the temporary table is created immediately.
        Like Spark, however, the current DataFrame is not persisted, the new DataFrame returned by this method must be used.

        :return: a new :class:`DataFrame`
        """
        return self.bigquery._registerDataFrameAsTempTable(self)

    def createOrReplaceTempTable(self, alias: str) -> 'DataFrame':
        """Creates or replace a persisted temporary table.

        >>> bq = BigQueryBuilder(get_bq_client())
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

        >>> bq = BigQueryBuilder(get_bq_client())
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
        >>> try:  #doctest: +ELLIPSIS
        ...   bq.sql("SELECT * FROM temp_view").show()
        ... except google.api_core.exceptions.BadRequest as e:
        ...   print(e)
        400 Table name "temp_view" missing dataset while no default dataset is set in the request.
        ...

        In this project, temporary views are implemented as CTEs in the final compiled query.
        As such, BigQuery does not allow to define two CTEs with the same name.

        :param name: Name of the temporary view. It must contain only alphanumeric and lowercase characters, no dots.
        :return: Nothing
        """
        self.bigquery._registerDataFrameAsTempView(self, name)

    def select(self, *columns: Union[List[Column], Column]) -> 'DataFrame':
        """Projects a set of expressions and returns a new :class:`DataFrame`."""
        if isinstance(columns[0], list):
            if len(columns) == 1:
                columns = columns[0]
            else:
                raise TypeError(f"Wrong argument type: {type(columns)}")
        query = strip_margin(
            f"""SELECT
            |{cols_to_str(columns, 2)}
            |FROM {quote(self._alias)}""")
        return self._apply_query(query)

    def drop(self, *cols: str) -> 'DataFrame':
        """Returns a new :class:`DataFrame` that drops the specified column.
        This is a no-op if schema doesn't contain the given column name(s).
        """
        schema_cols = set(self.columns)
        cols = [col for col in cols if col in schema_cols]
        query = strip_margin(
            f"""SELECT 
            |  * EXCEPT ({cols_to_str(cols)})
            |FROM {quote(self._alias)}""")
        return self._apply_query(query)

    def union(self, other: 'DataFrame') -> 'DataFrame':
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

    unionAll = union

    def unionByName(self, other: 'DataFrame', allowMissingColumns: bool = False) -> 'DataFrame':
        """Returns a new :class:`DataFrame` containing union of rows in this and another :class:`DataFrame`.

        This is different from both `UNION ALL` and `UNION DISTINCT` in SQL. To do a SQL-style set
        union (that does deduplication of elements), use this function followed by :func:`distinct`.

        .. versionadded:: 2.3.0

        Examples
        --------
        The difference between this function and :func:`union` is that this function
        resolves columns by name (not by position):

        >>> bq = BigQueryBuilder(get_bq_client())
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
            raise ValueError(f"UnionByName: dataFrames must have the same columns, "
                             f"unless allowMissingColumns is set to True.\n"
                             f"Columns in first DataFrame: [{cols_to_str(self.columns)}]\n"
                             f"Columns in second DataFrame: [{cols_to_str(other.columns)}]")

        def optional_comma(l: list):
            return ',' if len(l) > 0 else ''

        query = strip_margin(f"""
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
        |""")
        return self._apply_query(query, deps=[self, other])

    def limit(self, num: int) -> 'DataFrame':
        """Returns a new :class:`DataFrame` with a result count limited to the specified number of rows."""
        query = f"""SELECT * FROM {quote(self._alias)} LIMIT {num}"""
        return self._apply_query(query)

    def distinct(self) -> 'DataFrame':
        """Returns a new :class:`DataFrame` containing the distinct rows in this :class:`DataFrame`.

        Limitations compared to Spark
        -----------------------------
        In BigQuery, the DISTINCT statement does not work on complex types like STRUCT and ARRAY.
        """
        query = f"""SELECT DISTINCT * FROM {quote(self._alias)}"""
        return self._apply_query(query)

    def sort(self, *cols: str):
        """Returns a new :class:`DataFrame` sorted by the specified column(s).

        :param cols:
        :return:
        """
        query = strip_margin(f"""
        |SELECT * 
        |FROM {quote(self._alias)} 
        |ORDER BY {cols_to_str(cols)}""")
        return self._apply_query(query)

    orderBy = sort

    def filter(self, expr: str) -> 'DataFrame':
        """Filters rows using the given condition."""
        query = strip_margin(f"""
        |SELECT * 
        |FROM {quote(self._alias)} 
        |WHERE {expr}""")
        return self._apply_query(query)

    where = filter

    def withColumn(self, col_name: str, col_expr: str, replace: bool = False) -> 'DataFrame':
        """Returns a new :class:`DataFrame` by adding a column or replacing the existing column that has the same name.

        The column expression must be an expression over this :class:`DataFrame`; attempting to add a column from
        some other :class:`DataFrame` will raise an error.

        Limitations compared to Spark
        -----------------------------
        - Replace an existing column must be done explicitly by passing the argument `replace=True`.
        - Each time this function is called, a new CTE will be created. This may lead to reaching BigQuery's
          query size limit very quickly.

        TODO: This project is just a POC. Future versions may bring improvements to these features but this will require more on-the-fly schema inspections.

        :param col_name: Name of the new column
        :param col_expr: Expression defining the new column
        :param replace: Set to true when replacing an already existing column
        :return: a new :class:`DataFrame`
        """
        if replace:
            query = f"SELECT * REPLACE ({col_expr} AS {col_name}) FROM {quote(self._alias)}"
        else:
            query = f"SELECT *, {col_expr} AS {col_name} FROM {quote(self._alias)}"
        return self._apply_query(query)

    def count(self) -> int:
        """Returns the number of rows in this :class:`DataFrame`."""
        query = f"SELECT COUNT(1) FROM {quote(self._alias)}"
        return self._apply_query(query).collect()[0][0]

    def collect_iterator(self) -> RowIterator:
        """Returns all the records as :class:`RowIterator`."""
        return self.bigquery._execute_query(self.compile())

    def collect(self) -> List[Row]:
        """Returns all the records as list of :class:`Row`."""
        return list(self.bigquery._execute_query(self.compile()))

    def take(self, num):
        """Returns the first ``num`` rows as a :class:`list` of :class:`Row`."""
        return self.limit(num).collect()

    def show(self, n: int = 20, format_args=None) -> None:
        """Prints the first ``n`` rows to the console. This uses the awesome Python library called `tabulate
        <https://pythonrepo.com/repo/astanin-python-tabulate-python-generating-and-working-with-logs>`_.

        Formating options may be set using `format_args`.

        >>> bq = BigQueryBuilder(get_bq_client())
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

        :param n: Number of rows to show.
        :param format_args: extra arguments that may be passed to the function tabulate.tabulate()
        :return: Nothing
        """
        res = self.limit(n+1).collect_iterator()
        print_results(res, format_args, limit=n)

    def toPandas(self):
        """Returns the contents of this :class:`DataFrame` as Pandas ``pandas.DataFrame``.

        This method requires to have following extra dependencies installed
        - pandas
        - pyarrow

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
        return self.collect_iterator().to_dataframe()

    def treeString(self):
        """Generates a string representing the schema in tree format"""
        return schema_to_tree_string(self.schema)

    def printSchema(self) -> None:
        """Prints out the schema in tree format.

        Examples:

        >>> bq = BigQueryBuilder(get_bq_client())
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

        >>> bq = BigQueryBuilder(get_bq_client())
        >>> from bigquery_frame import functions as f
        >>> df = bq.sql('''SELECT 1 as a''').select('a', f.col('a')+f.lit(1).alias('b')).withColumn('c', f.expr('a + b'))
        >>> df.print_query()
        WITH `_default_alias_1` AS (
          SELECT 1 as a
        )
        , `_default_alias_2` AS (
          SELECT
            a,
            a + 1
          FROM `_default_alias_1`
        )
        SELECT *, a + b AS c FROM `_default_alias_2`

        """
        print(self.compile())

    @property
    def columns(self) -> List[str]:
        """Returns all column names as a list."""
        return [field.name for field in self.schema]


def __get_test_df() -> DataFrame:
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
