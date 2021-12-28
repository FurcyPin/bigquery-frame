from google.cloud.bigquery import SchemaField

from bigquery_frame import BigQueryBuilder
import unittest

from bigquery_frame.auth import get_bq_client


class TestDataFrame(unittest.TestCase):

    def setUp(self) -> None:
        self.bigquery = BigQueryBuilder(get_bq_client())

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
