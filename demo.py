import google
from bigquery_frame import BigQueryBuilder, DataFrame
from bigquery_frame.transfo_utils import sort_columns

PROJECT_NAME = "bigquery-frame"
client = google.cloud.bigquery.Client(PROJECT_NAME)

bigquery = BigQueryBuilder(client)


df = bigquery.sql("""
    SELECT 1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, NULL as other_col
    UNION ALL
    SELECT 2 as id, "Ivysaur" as name, ["Grass", "Poison"] as types, NULL as other_col
""")
df2 = df.select("id", "name", "types")
df2.createOrReplaceTempView("pokedex")
df3 = bigquery.sql("""SELECT * FROM pokedex""")
df4 = df3.withColumn("nb_types", "ARRAY_LENGTH(types)")
df5 = df4.withColumn("name", "LOWER(name)", replace=True)
df6 = df5.withColumn("my_struct", "(SELECT AS STRUCT 1 as a, 2 as b)")

# print(df5.compile())
print(df5.schema)
df5.show()

df5.show()
sort_columns(df5).show()

