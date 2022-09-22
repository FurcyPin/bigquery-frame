from dataclasses import dataclass
from typing import Any, Dict, Optional
from uuid import uuid4

from google.cloud.bigquery import Client, Dataset

from bigquery_frame.column import cols_to_str
from bigquery_frame.dataframe import DataFrame
from bigquery_frame.utils import assert_true, quote

SUPPORTED_MODES = {
    "append",
    "overwrite",
    "error",
    "errorifexists",
    "ignore",
}

CREATE_STR_FOR_MODE = {
    "overwrite": "CREATE OR REPLACE TABLE",
    "error": "CREATE TABLE",
    "errorifexists": "CREATE TABLE",
    "ignore": "CREATE TABLE IF NOT EXISTS",
}

DEFAULT_MODE = "error"


@dataclass
class DataframeWriterOptions:
    partition_by: Optional[str]
    options: Dict[str, Any]
    mode: str = DEFAULT_MODE

    def __init__(self):
        self.partition_by = None
        self.options = dict()


class DataframeWriter:
    def __init__(self, df: DataFrame):
        self.__df = df
        self.__options = DataframeWriterOptions()

    def mode(self, mode: str) -> "DataframeWriter":
        """Specifies the behavior when data or table already exists.

        Options include:

        - `append`: Append contents of this :class:`DataFrame` to existing table.
        - `overwrite`: Replace destination table with the new data if it already exists.
        - `error` or `errorifexists`: Throw an exception if destination table already exists.
        - `ignore`: Silently ignore this operation if destination table already exists.

        We recommend using 'overwrite', as it is the only mode that is idempotent.

        Limitations
        -----------
        - `append`: must be used on a table that already exists. The inserted DataFrame must have the exact same
           schema as the existing table. It cannot change it's existing partitioning nor options, which means
           that any `partition_by` and `options` that are passed will be ignored.
        - `overwrite`: Overwrite existing data. This will erase and replace the table with a new table,
          which means that any pre-existing schema, partitioning, clustering, table or column option
          (including column description) will be removed and replaced with the new table

        Examples
        --------
        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> client = get_bq_client()
        >>> test_dataset = __setup_test_dataset(client)
        >>> bq = BigQueryBuilder(client)

        >>> bq.sql("SELECT 1 as a").write.save(f"{test_dataset.dataset_id}.my_table", mode="OVERWRITE")
        >>> bq.table(f"{test_dataset.dataset_id}.my_table").show()
        +---+
        | a |
        +---+
        | 1 |
        +---+
        >>> bq.sql("SELECT 2 as b").write.mode("overwrite").save(f"{test_dataset.dataset_id}.my_table")
        >>> bq.table(f"{test_dataset.dataset_id}.my_table").show()
        +---+
        | b |
        +---+
        | 2 |
        +---+
        >>> bq.sql("SELECT 3 as b").write.mode("append").save(f"{test_dataset.dataset_id}.my_table")
        >>> bq.table(f"{test_dataset.dataset_id}.my_table").orderBy("b").show()
        +---+
        | b |
        +---+
        | 2 |
        | 3 |
        +---+
        >>> __teardown_test_dataset(client, test_dataset)


        :param mode:
        :return:
        """
        mode = mode.lower()
        assert_true(
            mode in SUPPORTED_MODES, f"Write mode '{mode}' is not supported. Supported modes are: {SUPPORTED_MODES}"
        )
        self.__options.mode = mode
        return self

    def options(self, **options) -> "DataframeWriter":
        """Adds output options.

        For a comprehensive list of settable options, please refer to
        `BigQuery's documentation
        <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#table_set_options_list>`_.  # noqa: E501

        Examples:
        ---------
        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> client = get_bq_client()
        >>> test_dataset = __setup_test_dataset(client)
        >>> bq = BigQueryBuilder(client)

        >>> df = bq.sql("SELECT 1 as id")
        >>> options = {
        ...     "description": "this is a test table",
        ...     "labels": {"org_unit": "development"}
        ... }
        >>> df.write.options(**options).mode("OVERWRITE").save(f"{test_dataset.dataset_id}.my_table")
        >>> table = client.get_table(f"{test_dataset.dataset_id}.my_table")
        >>> table.description
        'this is a test table'
        >>> table.labels
        {'org_unit': 'development'}
        >>> __teardown_test_dataset(client, test_dataset)

        """
        self.__options.options = {**self.__options.options, **options}
        return self

    def partition_by(self, col: str) -> "DataframeWriter":
        """Partitions the output by the given columns.

        For a comprehensive list of possible partitioning settings, please refer to
        `BigQuery's documentation
        <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#partition_expression>`_

        Limitations:
        ------------
        - Unlike Spark, only one level of partitioning is possible in BigQuery
        - It is not possible to create an ingestion-time partitioned table from a DataFrame.
          Instead, use a CREATE TABLE DDL statement to create the table, and then use an INSERT DML statement
          to insert data into it.
        - It is not possible to use the "OVERWRITE" mode to replace a table with a different kind of partitioning.
          Instead, DROP the table, and then use a "ERROR" mode statement to recreate it.

        Examples
        --------
        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> client = get_bq_client()
        >>> test_dataset = __setup_test_dataset(client)
        >>> bq = BigQueryBuilder(client)

        >>> df = bq.sql("SELECT 1 as id, DATE('2022-01-01') as day")

        It is not possible to use the "OVERWRITE" mode to replace a table with a different kind of partitioning.
        So we make sure the table does not exist
        >>> df.write.partition_by("day").mode("OVERWRITE").save(f"{test_dataset.dataset_id}.my_partitioned_table")
        >>> table = client.get_table(f"{test_dataset.dataset_id}.my_partitioned_table")
        >>> table.time_partitioning.type_
        'DAY'
        >>> __teardown_test_dataset(client, test_dataset)

        """
        self.__options.partition_by = col
        return self

    def __compile_options_str(self):
        options = self.__options.options

        def to_str(v: Any):
            if isinstance(v, Dict):
                return repr(list(v.items()))
            else:
                return repr(v)

        if len(options) > 0:
            options_str_list = [f"{k}={to_str(v)}" for k, v in options.items()]
            options_str = f"\n OPTIONS(\n{cols_to_str(options_str_list, 2)}\n)"
        else:
            options_str = ""
        return options_str

    def __compile_partition_by_str(self):
        partition_by = self.__options.partition_by
        if partition_by is not None:
            partition_by_str = f"\n PARTITION BY {partition_by}"
        else:
            partition_by_str = ""
        return partition_by_str

    def compile(self, table_name: str):
        mode = self.__options.mode
        table_name = quote(table_name)
        select_str = self.__df.compile()
        if mode == "append":
            insert_statement = "INSERT INTO {table_name}\n{select_str}"
            return insert_statement.format(table_name=table_name, select_str=select_str)
        else:
            create_str = CREATE_STR_FOR_MODE[mode]
            create_statement = "{create_str} {table_name}{partition_by_str}{options_str} AS\n{select_str}"
            options_str = self.__compile_options_str()
            partition_by_str = self.__compile_partition_by_str()
            return create_statement.format(
                create_str=create_str,
                table_name=table_name,
                partition_by_str=partition_by_str,
                options_str=options_str,
                select_str=select_str,
            )

    def save(self, table_name: str, mode=None, partition_by=None, **options) -> None:
        """Saves the content of the :class:`DataFrame` to a BigQuery table.

        Examples
        --------
        >>> from bigquery_frame.bigquery_builder import BigQueryBuilder
        >>> from bigquery_frame.auth import get_bq_client
        >>> client = get_bq_client()
        >>> test_dataset = __setup_test_dataset(client)
        >>> bq = BigQueryBuilder(client)


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

        :param table_name: name of the table where the DataFrame will be written.
            (Example: "my_schema.my_table" or "my-project.my_schema.my_table")
        :param mode: Specifies the behavior when data or table already exists. Cf method DataframeWriter.mode.
        :param partition_by: Partitions the output by the given columns.
        :param options: Adds output options. Cf method DataframeWriter.options.
        :return:
        """
        if mode is not None:
            self.mode(mode)
        if options is not None:
            self.options(**options)
        if partition_by is not None:
            self.partition_by(partition_by)
        query = self.compile(table_name)
        self.__df.bigquery._execute_query(query)


def __setup_test_dataset(client: Client) -> Dataset:
    random_id = uuid4()
    test_dataset_name = "test_dataset_" + str(random_id).replace("-", "_")
    test_dataset = Dataset(f"{client.project}.{test_dataset_name}")
    test_dataset.location = "EU"
    client.create_dataset(test_dataset, exists_ok=True)
    return test_dataset


def __teardown_test_dataset(client: Client, test_dataset: Dataset):
    client.delete_dataset(test_dataset, delete_contents=True)
    client.close()
