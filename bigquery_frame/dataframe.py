import re
from typing import List, Tuple, Optional, Union

from google.cloud.bigquery import SchemaField, Client, Row
from google.cloud.bigquery.table import RowIterator

from bigquery_frame.auth import get_bq_client
from bigquery_frame.has_bigquery_client import HasBigQueryClient
from bigquery_frame.printing import print_results


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


def cols_to_str(cols, indentation: int = None) -> str:
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

class BigQueryBuilder(HasBigQueryClient):
    DEFAULT_ALIAS_NAME = "_default_alias_{num}"

    def __init__(self, client: Client):
        super().__init__(client)
        self._alias_count = 0
        self._views: List[Tuple[str, 'DataFrame']] = []

    def table(self, full_table_name: str) -> 'DataFrame':
        """Returns the specified table as a :class:`DataFrame`.
        """
        query = f"""SELECT * FROM {quote(full_table_name)}"""
        return DataFrame(query, alias=None, bigquery=self)

    def sql(self, sql_query) -> 'DataFrame':
        """Returns a :class:`DataFrame` representing the result of the given query.
        """
        return DataFrame(sql_query, None, self)

    def _registerDataFrameAsTable(self, df: 'DataFrame', alias: str) -> None:
        self._check_alias(alias, [])
        self._views.append((alias, df))

    def _compile_views(self) -> List[str]:
        return [
            strip_margin(f"""{quote(alias)} AS (
            |{indent(df._compile_with_deps(), 2)}
            |)""")
            for alias, df in self._views
        ]

    def _get_alias(self) -> str:
        self._alias_count += 1
        return self.DEFAULT_ALIAS_NAME.format(num=self._alias_count)

    def _check_alias(self, new_alias, deps: List[Tuple[str, 'DataFrame']]) -> None:
        """Checks that the alias follows BigQuery constraints, such as:

        - BigQuery does not allow having two CTEs with the same name in a query.

        :param new_alias:
        :param deps:
        :return: None
        :raises: an Exception if something that does not comply with BigQuery's rules is found.
        """
        collisions = [alias for alias, df in self._views + deps if alias == new_alias]
        if len(collisions) > 0:
            raise Exception(f"Duplicate alias {new_alias}")


class DataFrame:

    def __init__(self, query: str, alias: Optional[str], bigquery: BigQueryBuilder, **kwargs):
        self.query = query
        self._deps: List[Tuple[str, 'DataFrame']] = []
        deps = kwargs.get("_deps")
        if deps is not None:
            self._deps = deps
        if alias is None:
            alias = bigquery._get_alias()
        else:
            bigquery._check_alias(alias, self._deps)
        self._alias = alias
        self.bigquery = bigquery
        self._schema = None

    def __repr__(self):
        return f"""({self.query}) as {self._alias}"""

    def _apply_query(self, query: str) -> 'DataFrame':
        return DataFrame(query, None, self.bigquery, _deps=self._deps + [(self._alias, self)])

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

    # TODO: persist would create a temporary table
    def persist(self, alias) -> 'DataFrame':
        pass

    def createOrReplaceTempView(self, alias: str) -> None:
        """Creates or replaces a local temporary view with this :class:`DataFrame`.

        Limitations compared to Spark
        -----------------------------
        - Currently, replacing an existing temp view with a new one will raise an error.
          In this project, temporary views are implemented as CTEs in the final compiled query.
          As such, BigQuery does not allow to define two CTEs with the same name.

        :param alias:
        :return:
        """
        self.bigquery._registerDataFrameAsTable(self, alias)

    def select(self, *columns: Union[List[str], str]) -> 'DataFrame':
        """Projects a set of expressions and returns a new :class:`DataFrame`."""
        if len(columns) == 1 and isinstance(columns[0], list):
            columns = columns[0]
        col_str = ',\n'.join(columns)
        query = strip_margin(
            f"""SELECT 
            |{indent(col_str, 2)}
            |FROM {self._alias}""")
        return self._apply_query(query)

    def limit(self, num: int) -> 'DataFrame':
        """Returns a new :class:`DataFrame` with a result count limited to the specified number of rows."""
        query = f"""SELECT * FROM {self._alias} LIMIT {num}"""
        return self._apply_query(query)

    def sort(self, *cols: str):
        """Returns a new :class:`DataFrame` sorted by the specified column(s)."""
        query = strip_margin(f"""
        |SELECT * 
        |FROM {self._alias} 
        |ORDER BY {cols_to_str(cols)}""")
        return self._apply_query(query)

    orderBy = sort

    def filter(self, expr: str):
        """Filters rows using the given condition."""
        query = strip_margin(f"""
        |SELECT * 
        |FROM {self._alias} 
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

        :param col_name:
        :param col_expr:
        :param replace:
        :return:
        """
        if replace:
            query = f"SELECT * REPLACE ({col_expr} AS {col_name}) FROM {self._alias}"
        else:
            query = f"SELECT *, {col_expr} AS {col_name} FROM {self._alias}"
        return self._apply_query(query)

    def count(self):
        """Returns the number of rows in this :class:`DataFrame`."""
        query = f"SELECT COUNT(1) FROM {self._alias}"
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

    def show(self, n: int = 20, format_args=None):
        """Prints the first ``n`` rows to the console. This uses the awesome Python library called `tabulate
        <https://pythonrepo.com/repo/astanin-python-tabulate-python-generating-and-working-with-logs>`_.

        Formatting options may be used using `format_args`.

        >>> bq = BigQueryBuilder(get_bq_client())
        >>> df = bq.sql('''SELECT 1 as id, STRUCT(1 as a, [STRUCT(1 as c)] as b) as s''')
        >>> df.show()
        +----+---------------------------+
        | id |             s             |
        +----+---------------------------+
        | 1  | {'a': 1, 'b': [{'c': 1}]} |
        +----+---------------------------+
        >>> df.show(format_args={"tablefmt": 'fancy_grid'})
        ╒══════╤═══════════════════════════╕
        │   id │ s                         │
        ╞══════╪═══════════════════════════╡
        │    1 │ {'a': 1, 'b': [{'c': 1}]} │
        ╘══════╧═══════════════════════════╛

        :param n: Number of rows to show.
        :param format_args: extra arguments that may be passed to the function tabulate.tabulate()

        :return:
        """
        res = self.limit(n).collect_iterator()
        print_results(res, format_args)

    def treeString(self) -> str:
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

        res = ["root"] + str_gen_schema(self.schema, " |-- ")

        return "\n".join(res) + "\n"

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

    @property
    def columns(self) -> List[str]:
        """Returns all column names as a list."""
        return [field.name for field in self.schema]

