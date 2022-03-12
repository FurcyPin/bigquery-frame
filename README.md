# Bigquery-frame

## What is it ?

This project is a POC that aims to showcase the wonders
that could be done if BigQuery provided a DataFrame API in 
Python similar to the one already available with PySpark
or Snowpark (for which the Python API will come out soon).

I tried to reproduce the most commonly used methods of the Spark DataFrame object. 
I aimed at making something as close as possible as PySpark, and tried to keep exactly
the same naming and docstrings as PySpark's DataFrames.
 

For instance, this is a working example of PySpark code :
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.master("local[1]").getOrCreate()

df = spark.sql("""
    SELECT 1 as id, "Bulbasaur" as name, ARRAY("Grass", "Poison") as types, NULL as other_col
    UNION ALL
    SELECT 2 as id, "Ivysaur" as name, ARRAY("Grass", "Poison") as types, NULL as other_col
""")

df.select("id", "name", "types").createOrReplaceTempView("pokedex")

df2 = spark.sql("""SELECT * FROM pokedex""")\
    .withColumn("nb_types", f.expr("SIZE(types)"))\
    .withColumn("name", f.expr("LOWER(name)"))

df2.show()
# +---+---------+---------------+--------+
# | id|     name|          types|nb_types|
# +---+---------+---------------+--------+
# |  1|bulbasaur|[Grass, Poison]|       2|
# |  2|  ivysaur|[Grass, Poison]|       2|
# +---+---------+---------------+--------+
```

And this is an equivalent working example using bigquery_frame, that runs on Google Big Query! 
```python
from bigquery_frame import BigQueryBuilder
from bigquery_frame.auth import get_bq_client
from bigquery_frame import functions as f

bigquery = BigQueryBuilder(get_bq_client())

df = bigquery.sql("""
    SELECT 1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, NULL as other_col
    UNION ALL
    SELECT 2 as id, "Ivysaur" as name, ["Grass", "Poison"] as types, NULL as other_col
""")

df.select("id", "name", "types").createOrReplaceTempView("pokedex")

df2 = bigquery.sql("""SELECT * FROM pokedex""")\
    .withColumn("nb_types", f.expr("ARRAY_LENGTH(types)"))\
    .withColumn("name", f.expr("LOWER(name)"), replace=True)

df2.show()
# +----+-----------+---------------------+----------+
# | id |      name |               types | nb_types |
# +----+-----------+---------------------+----------+
# |  1 | bulbasaur | ['Grass', 'Poison'] |        2 |
# |  2 |   ivysaur | ['Grass', 'Poison'] |        2 |
# +----+-----------+---------------------+----------+
```

## What's so cool about DataFrames ?

I believe that DataFrames are super cool to organise SQL code as it allows us to 
several things that are much harder, or even impossible, in pure-SQL:

- on-the-fly introspection
- chaining operations
- generic transformations
- higher level abstraction

But that deserves a blog article (coming soon).

## I want to try this POC, how do I use it ?

Just clone this repository, open PyCharm, and follow the
instructions in the [AUTH.md](/AUTH.md) documentation
to set up your connection to BigQuery. Then, go fiddle
with the [demo](/examples/demo.py), or have a look at the [examples](/examples).


## How does it work ?

Very simply, by generating SQL queries that are sent to BigQuery.
You can get the query by calling the method `DataFrame.compile()`.

For instance, if we reuse the example from the beginning:
```
print(df2.compile())
```

This will print the following SQL query:
```SQL
WITH pokedex AS (
  WITH _default_alias_1 AS (
    
        SELECT 1 as id, "Bulbasaur" as name, ["Grass", "Poison"] as types, NULL as other_col
        UNION ALL
        SELECT 2 as id, "Ivysaur" as name, ["Grass", "Poison"] as types, NULL as other_col
    
  )
  SELECT 
    id,
    name,
    types
  FROM _default_alias_1
)
, _default_alias_3 AS (
  SELECT * FROM pokedex
)
, _default_alias_4 AS (
  SELECT 
    *,
    ARRAY_LENGTH(types) AS nb_types
  FROM _default_alias_3
)
SELECT 
  * REPLACE (
    LOWER(name) AS name
  )
FROM _default_alias_4
```

## Facturation

The examples in this code only use generated data and don't ready any "real" table.
This means that you won't be charged a cent running them.

Also, even when reading "real" tables, any one-the-fly introspection (such as
getting a DataFrame's schema), will trigger a query on BigQuery but will read
0 rows, and will thus be billed 0 cent.

## Known limitations

Since this is a POC, I took some shortcuts and did not try to optimize the query length.
In particular, this uses _**a lot**_ of CTEs, and any serious project trying to use it
might reach the maximum query length very quickly.

Here is a list of other known limitations, please also see the 
[Further developments](#further-developments) section for a list of missing features.

- `DataFrame.withColumn`: 
  - unlike in Spark, replacing an existing column is  
    not done automatically, an extra argument `replace=True` must be passed.
- `DataFrame.createOrReplaceTempView`: 
  - I kept the same name as Spark for consistency, but it does not create an actual view on BigQuery, it just emulates 
    Spark's behaviour by using a CTE. Because of this, if you replace a temp view that already exists, the new view
    can not derive from the old view (while in Spark it is possible). 

## Further developments

Functions not supported yet :

- `DataFrame.join`
- `DataFrame.groupBy`
- `DataFrame.printSchema`

Also, it would be cool to expand this to other SQL engines than BigQuery 
(contributors are welcome ;-) ).


## Why did I make this ?

I hope that it will motivate the teams working on BigQuery (or Redshift, 
or Azure Synapse) to propose a real python DataFrame API on top of their 
massively parallel SQL engines. But not something ugly like this POC,
that generates SQL strings, more something like Spark Catalyst, which directly
generates logical plans out of the DataFrame API without passing through the 
"SQL string" step.

After starting this POC, I realized Snowflake already understood this and 
developed Snowpark, a Java/Scala (and soon Python) API to run complex workflows
on Snowflake, and [Snowpark's DataFrame API](https://docs.snowflake.com/en/developer-guide/snowpark/reference/scala/com/snowflake/snowpark/DataFrame.html)
which was clearly borrowed from [Spark's DataFrame (= DataSet[Row]) API](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html)
(we recognize several key method names: cache, createOrReplaceTempView, 
where/filter, collect, toLocalIterator). 

I believe such project could open the gate to hundreds of very cool applications.
For instance, did you know that, in its early versions at least, Dataiku Shaker 
was just a GUI that chained transformations on Pandas DataFrame, and later 
Spark DataFrame ? 

Another possible evolution would be to make a DataFrame API capable of speaking
multiple SQL dialects. By using it, projects that generate SQL for multiple 
platforms, like [Malloy](https://github.com/looker-open-source/malloy), could
all use the same DataFrame abstraction. Adding support for a new SQL platform
would immediately allow all the project based on it to support this new platform.

**I would be very interested if someone could make a similar POC with, 
RedShift, Postgres, Azure Synapse, or any other SQL engines 
(aside from Spark-SQL and Snowpark, of course :-p).**
