from google.cloud.bigquery import SchemaField

from bigquery_frame import BigQueryBuilder
import unittest

from bigquery_frame.auth import get_bq_client


class TestDataFrame(unittest.TestCase):

    def setUp(self) -> None:
        self.bigquery = BigQueryBuilder(get_bq_client())

    def tearDown(self) -> None:
        self.bigquery.close()

    def test_createOrReplaceTempView(self):
        df = self.bigquery.sql("""SELECT 1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, NULL as other_col""")
        df.createOrReplaceTempView("pokedex")
        df2 = self.bigquery.sql("""SELECT * FROM pokedex""")

        expected = [
            SchemaField('id', 'INTEGER', 'NULLABLE', None, (), None),
            SchemaField('name', 'STRING', 'NULLABLE', None, (), None),
            SchemaField('types', 'STRING', 'REPEATED', None, (), None),
            SchemaField('other_col', 'INTEGER', 'NULLABLE', None, (), None)
        ]

        self.assertEqual(df2.schema, expected)

    def test_createOrReplaceTempView_with_reserved_keyword_alias(self):
        """Some words like 'ALL' are reserved by BigQuery and may not be used as table names without being backticked."""
        self.bigquery.sql("""SELECT 1 as id""").createOrReplaceTempView("all")
        df = self.bigquery.table("all")

        expected = [SchemaField('id', 'INTEGER', 'NULLABLE', None, (), None)]

        self.assertEqual(df.schema, expected)

    def test_select(self):
        """Select should work with either multiple args or a single argument which is a list"""
        df = self.bigquery.sql("""SELECT 1 as c1, 2 as c2""").select("c1", "c2").select(["c1", "c2"])

        expected = [
            SchemaField('c1', 'INTEGER', 'NULLABLE', None, (), None),
            SchemaField('c2', 'INTEGER', 'NULLABLE', None, (), None),
        ]

        self.assertEqual(df.schema, expected)

        with self.assertRaises(TypeError):
            df.select(["c1"], ["c2"])

    def test_2(self):
        df = self.bigquery.sql("""SELECT 1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, NULL as other_col""")
        df2 = df.select("id", "name", "types")
        df2.createOrReplaceTempView("pokedex")
        df3 = self.bigquery.sql("""SELECT * FROM pokedex""")
        df4 = df3.withColumn("nb_types", "ARRAY_LENGTH(types)")
        df5 = df4.withColumn("name", "LOWER(name)", replace=True)

        expected = [
            SchemaField('id', 'INTEGER', 'NULLABLE', None, (), None),
            SchemaField('name', 'STRING', 'NULLABLE', None, (), None),
            SchemaField('types', 'STRING', 'REPEATED', None, (), None),
            SchemaField('nb_types', 'INTEGER', 'NULLABLE', None, (), None)
        ]

        self.assertEqual(df5.schema, expected)

    def test_count(self):
        df = self.bigquery.sql("""SELECT 1 as id UNION ALL SELECT 2 as id""")
        expected = 2
        self.assertEqual(df.count(), expected)

    def test_take(self):
        df = self.bigquery.sql("""SELECT id FROM UNNEST(GENERATE_ARRAY(1, 10, 1)) as id""")
        expected = 5
        self.assertEqual(len(df.take(5)), expected)

    def test_union_with_common_dependency(self):
        """Corner case that requires to use `_dedup_key_value_list` in `_apply_query`"""
        df1 = self.bigquery.sql("""SELECT id FROM UNNEST(GENERATE_ARRAY(1, 10, 1)) as id""")
        df2 = df1.where("MOD(id, 2) = 0")
        df3 = df1.where("MOD(id, 2) = 1")
        df4 = df2.union(df3)

        self.assertEqual(10, df4.count())

    def test_drop(self):
        df = self.bigquery.sql("""SELECT 1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, NULL as other_col""")
        df2 = df.drop("other_col", "col_that_does_not_exists")

        expected = [
            SchemaField('id', 'INTEGER', 'NULLABLE', None, (), None),
            SchemaField('name', 'STRING', 'NULLABLE', None, (), None),
            SchemaField('types', 'STRING', 'REPEATED', None, (), None)
        ]

        self.assertEqual(df2.schema, expected)
