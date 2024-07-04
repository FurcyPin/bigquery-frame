from bigquery_frame import BigQueryBuilder
from bigquery_frame import functions as f
from bigquery_frame.auth import get_bq_client

bigquery = BigQueryBuilder(get_bq_client())


df = bigquery.sql(
    """
    SELECT 1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, NULL as other_col
    UNION ALL
    SELECT 2 as id, "Ivysaur" as name, ["Grass", "Poison"] as types, NULL as other_col
""",
)
df.select("id", "name", "types").createOrReplaceTempView("pokedex")

df2 = (
    bigquery.sql("""SELECT * FROM pokedex""")
    .withColumn("nb_types", f.expr("ARRAY_LENGTH(types)"))
    .withColumn("name", f.expr("LOWER(name)"), replace=True)
)

df2.show()
# +----+-----------+---------------------+----------+
# | id |      name |               types | nb_types |
# +----+-----------+---------------------+----------+
# |  1 | bulbasaur | ['Grass', 'Poison'] |        2 |
# |  2 |   ivysaur | ['Grass', 'Poison'] |        2 |
# +----+-----------+---------------------+----------+

print(df2.compile())
