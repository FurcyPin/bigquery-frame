# Bigquery-frame

[![PyPI version](https://badge.fury.io/py/bigquery-frame.svg)](https://badge.fury.io/py/bigquery-frame)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/bigquery-frame.svg)](https://pypi.org/project/bigquery-frame/)
[![GitHub Build](https://img.shields.io/github/workflow/status/FurcyPin/bigquery-frame/Build%20and%20Validate)](https://github.com/FurcyPin/bigquery-frame/actions)
[![SonarCloud Coverage](https://sonarcloud.io/api/project_badges/measure?project=FurcyPin_bigquery-frame&metric=coverage)](https://sonarcloud.io/component_measures?id=FurcyPin_bigquery-frame&metric=coverage&view=list)
[![SonarCloud Bugs](https://sonarcloud.io/api/project_badges/measure?project=FurcyPin_bigquery-frame&metric=bugs)](https://sonarcloud.io/component_measures?metric=reliability_rating&view=list&id=FurcyPin_bigquery-frame)
[![SonarCloud Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=FurcyPin_bigquery-frame&metric=vulnerabilities)](https://sonarcloud.io/component_measures?metric=security_rating&view=list&id=FurcyPin_bigquery-frame)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/bigquery-frame)](https://pypi.org/project/bigquery-frame/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

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

But that deserves [a blog article](https://towardsdatascience.com/sql-jinja-is-not-enough-why-we-need-dataframes-4d71a191936d).


## Cool Features

This feature list is arbitrarily sorted by decreasing order of coolness.

### Data Diff _(NEW!)_
_Just like git diff, but for data!_

Performs a diff between two dataframes.

**Example:**
```python
from bigquery_frame.data_diff import DataframeComparator
from bigquery_frame import BigQueryBuilder
from bigquery_frame.auth import get_bq_client

bq = BigQueryBuilder(get_bq_client())
df1 = bq.sql("""
    SELECT * FROM UNNEST ([
        STRUCT(1 as id, [STRUCT(1 as a, 2 as b, 3 as c)] as my_array),
        STRUCT(2 as id, [STRUCT(1 as a, 2 as b, 3 as c)] as my_array),
        STRUCT(3 as id, [STRUCT(1 as a, 2 as b, 3 as c)] as my_array)
    ])
""")
df2 = bq.sql("""
    SELECT * FROM UNNEST ([
        STRUCT(1 as id, [STRUCT(1 as a, 2 as b, 3 as c, 4 as d)] as my_array),
        STRUCT(2 as id, [STRUCT(2 as a, 2 as b, 3 as c, 4 as d)] as my_array),
        STRUCT(4 as id, [STRUCT(1 as a, 2 as b, 3 as c, 4 as d)] as my_array)
   ])
""")

df1.show()
# +----+----------------------------+
# | id |                   my_array |
# +----+----------------------------+
# |  1 | [{'a': 1, 'b': 2, 'c': 3}] |
# |  2 | [{'a': 1, 'b': 2, 'c': 3}] |
# |  3 | [{'a': 1, 'b': 2, 'c': 3}] |
# +----+----------------------------+
df2.show()
# +----+------------------------------------+
# | id |                           my_array |
# +----+------------------------------------+
# |  1 | [{'a': 1, 'b': 2, 'c': 3, 'd': 4}] |
# |  2 | [{'a': 2, 'b': 2, 'c': 3, 'd': 4}] |
# |  4 | [{'a': 1, 'b': 2, 'c': 3, 'd': 4}] |
# +----+------------------------------------+
diff_result = DataframeComparator().compare_df(df1, df2)
diff_result.display()
```

Will produce the following output:

```
Schema has changed:
@@ -2,3 +2,4 @@
 my_array!.a INTEGER
 my_array!.b INTEGER
 my_array!.c INTEGER
+my_array!.d INTEGER
WARNING: columns that do not match both sides will be ignored
diff NOT ok
Summary:
Row count ok: 3 rows
1 (25.0%) rows are identical
1 (25.0%) rows have changed
1 (25.0%) rows are only in 'left'
1 (25.0%) rows are only in 'right
100%|██████████| 1/1 [00:04<00:00,  4.26s/it]
Found the following differences:
+-------------+---------------+-----------------------+-----------------------+----------------+
| column_name | total_nb_diff |                  left |                 right | nb_differences |
+-------------+---------------+-----------------------+-----------------------+----------------+
|    my_array |             1 | [{"a":1,"b":2,"c":3}] | [{"a":2,"b":2,"c":3}] |              1 |
+-------------+---------------+-----------------------+-----------------------+----------------+
1 rows were only found in 'left' :
Analyzing 4 columns ...
+---------------+-------------+-------------+-------+----------------+------------+-----+-----+------------------------------+
| column_number | column_name | column_type | count | count_distinct | count_null | min | max |               approx_top_100 |
+---------------+-------------+-------------+-------+----------------+------------+-----+-----+------------------------------+
|             0 |          id |     INTEGER |     1 |              1 |          0 |   3 |   3 | [{'value': '3', 'count': 1}] |
|             1 | my_array!.a |     INTEGER |     1 |              1 |          0 |   1 |   1 | [{'value': '1', 'count': 1}] |
|             2 | my_array!.b |     INTEGER |     1 |              1 |          0 |   2 |   2 | [{'value': '2', 'count': 1}] |
|             3 | my_array!.c |     INTEGER |     1 |              1 |          0 |   3 |   3 | [{'value': '3', 'count': 1}] |
+---------------+-------------+-------------+-------+----------------+------------+-----+-----+------------------------------+
1 rows were only found in 'right':
Analyzing 4 columns ...
+---------------+-------------+-------------+-------+----------------+------------+-----+-----+------------------------------+
| column_number | column_name | column_type | count | count_distinct | count_null | min | max |               approx_top_100 |
+---------------+-------------+-------------+-------+----------------+------------+-----+-----+------------------------------+
|             0 |          id |     INTEGER |     1 |              1 |          0 |   4 |   4 | [{'value': '4', 'count': 1}] |
|             1 | my_array!.a |     INTEGER |     1 |              1 |          0 |   1 |   1 | [{'value': '1', 'count': 1}] |
|             2 | my_array!.b |     INTEGER |     1 |              1 |          0 |   2 |   2 | [{'value': '2', 'count': 1}] |
|             3 | my_array!.c |     INTEGER |     1 |              1 |          0 |   3 |   3 | [{'value': '3', 'count': 1}] |
+---------------+-------------+-------------+-------+----------------+------------+-----+-----+------------------------------+
```

- Full support of nested records
- (improvable) support for repeated records
- Optimized to work even on huge table with more than 1000 columns

### Analyze

Perform an analysis on a DataFrame, return aggregated stats for each column,
such as count, count distinct, count null, min, max, top 100 most frequent values. 

- Optimized to work on wide tables
- Custom aggregation functions can be added
- Aggregations can be grouped against one or a tuple of columns

**Example:**
```python
from bigquery_frame.transformations_impl.analyze import __get_test_df
from bigquery_frame.transformations import analyze

df = __get_test_df()

df.show()
# +----+------------+---------------------+--------------------------------------------+
# | id |       name |               types |                                  evolution |
# +----+------------+---------------------+--------------------------------------------+
# |  1 |  Bulbasaur | ['Grass', 'Poison'] | {'can_evolve': True, 'evolves_from': None} |
# |  2 |    Ivysaur | ['Grass', 'Poison'] |    {'can_evolve': True, 'evolves_from': 1} |
# |  3 |   Venusaur | ['Grass', 'Poison'] |   {'can_evolve': False, 'evolves_from': 2} |
# |  4 | Charmander |            ['Fire'] | {'can_evolve': True, 'evolves_from': None} |
# |  5 | Charmeleon |            ['Fire'] |    {'can_evolve': True, 'evolves_from': 4} |
# |  6 |  Charizard |  ['Fire', 'Flying'] |   {'can_evolve': False, 'evolves_from': 5} |
# |  7 |   Squirtle |           ['Water'] | {'can_evolve': True, 'evolves_from': None} |
# |  8 |  Wartortle |           ['Water'] |    {'can_evolve': True, 'evolves_from': 7} |
# |  9 |  Blastoise |           ['Water'] |   {'can_evolve': False, 'evolves_from': 8} |
# +----+------------+---------------------+--------------------------------------------+

analyze(df).show()
# +---------------+------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# | column_number |            column_name | column_type | count | count_distinct | count_null |       min |       max |                                                                                                                                                                                                                                                                                                                     approx_top_100 |
# +---------------+------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |             0 |                     id |     INTEGER |     9 |              9 |          0 |         1 |         9 |                                                                       [{'value': '1', 'count': 1}, {'value': '2', 'count': 1}, {'value': '3', 'count': 1}, {'value': '4', 'count': 1}, {'value': '5', 'count': 1}, {'value': '6', 'count': 1}, {'value': '7', 'count': 1}, {'value': '8', 'count': 1}, {'value': '9', 'count': 1}] |
# |             1 |                   name |      STRING |     9 |              9 |          0 | Blastoise | Wartortle | [{'value': 'Bulbasaur', 'count': 1}, {'value': 'Ivysaur', 'count': 1}, {'value': 'Venusaur', 'count': 1}, {'value': 'Charmander', 'count': 1}, {'value': 'Charmeleon', 'count': 1}, {'value': 'Charizard', 'count': 1}, {'value': 'Squirtle', 'count': 1}, {'value': 'Wartortle', 'count': 1}, {'value': 'Blastoise', 'count': 1}] |
# |             2 |                 types! |      STRING |    13 |              5 |          0 |      Fire |     Water |                                                                                                                                                                  [{'value': 'Grass', 'count': 3}, {'value': 'Poison', 'count': 3}, {'value': 'Fire', 'count': 3}, {'value': 'Water', 'count': 3}, {'value': 'Flying', 'count': 1}] |
# |             3 |   evolution.can_evolve |     BOOLEAN |     9 |              2 |          0 |     false |      true |                                                                                                                                                                                                                                                                    [{'value': 'true', 'count': 6}, {'value': 'false', 'count': 3}] |
# |             4 | evolution.evolves_from |     INTEGER |     9 |              6 |          3 |         1 |         8 |                                                                                                                            [{'value': 'NULL', 'count': 3}, {'value': '1', 'count': 1}, {'value': '2', 'count': 1}, {'value': '4', 'count': 1}, {'value': '5', 'count': 1}, {'value': '7', 'count': 1}, {'value': '8', 'count': 1}] |
# +---------------+------------------------+-------------+-------+----------------+------------+-----------+-----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```


**Example with custom aggregation and column groups:**
```python
from bigquery_frame.transformations_impl import analyze_aggs
from bigquery_frame.transformations import analyze
aggs = [
     analyze_aggs.column_number,
     analyze_aggs.column_name,
     analyze_aggs.count,
     analyze_aggs.count_distinct,
     analyze_aggs.count_null,
]
analyze(df, group_by="evolution.can_evolve", _aggs=aggs).orderBy('group.can_evolve', 'column_number').show()
# +-----------------------+---------------+------------------------+-------+----------------+------------+
# |                 group | column_number |            column_name | count | count_distinct | count_null |
# +-----------------------+---------------+------------------------+-------+----------------+------------+
# | {'can_evolve': False} |             0 |                     id |     3 |              3 |          0 |
# | {'can_evolve': False} |             1 |                   name |     3 |              3 |          0 |
# | {'can_evolve': False} |             2 |                 types! |     5 |              5 |          0 |
# | {'can_evolve': False} |             4 | evolution.evolves_from |     3 |              3 |          0 |
# |  {'can_evolve': True} |             0 |                     id |     6 |              6 |          0 |
# |  {'can_evolve': True} |             1 |                   name |     6 |              6 |          0 |
# |  {'can_evolve': True} |             2 |                 types! |     8 |              4 |          0 |
# |  {'can_evolve': True} |             4 | evolution.evolves_from |     6 |              3 |          3 |
# +-----------------------+---------------+------------------------+-------+----------------+------------+
```


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

## Billing

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
- `DataFrame.join`:
  - When doing a select after a join, table prefixes MUST always be used on column names.
    For this reason, users SHOULD always make sure the DataFrames they are joining on are properly aliased
  - When chaining multiple joins, the name of the first DataFrame is not available in the select clause

## Further developments

Functions not supported yet :

- `DataFrame.groupBy`

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


## Release Notes

### 0.4.0


#### New exciting features!

Several new features that make working with nested structure easier were added.

- Added `DataFrame.select_nested_columns` and `DataFrame.with_nested_columns`, which 
  make transformation of nested values much easier
- Added `functions.transform`, useful to transform arrays
- Added `transformations.normalize_arrays` which automatically sort all arrays in a DataFrame, including
  arrays of arrays of arrays...
- Added `transformations.harmonize_dataframes` which takes two DataFrames and transform them into DataFrames
  with the same schema.
- Experimental: Added data_diff capabilities, including a DataFrameComparator which can perform a diff
  between two DataFrames. Extremely useful for non-regression testing.
- Generated queries are now deterministic, this means that if you re-run the
  same DataFrame code twice, the exact same query will be sent to BigQuery twice,
  thus leveraging query caching.


#### Other features

- Added automatic retry when BigQuery returns an InternalServerError. We now do 3 tries by default.
- Added `functions.to_base32` and `functions.to_base64`. 
  `from_base_32` and `from_base_64` will be added later, 
  once a [bug in python-tabulate](https://github.com/astanin/python-tabulate/issues/192) is fixed.
- Added `Column.__mod__` (e.g. `f.when(c % 2 == 0)`) 


#### Breaking changes

- Improved typing of `Column.when.otherwise`. Now `functions.when` returns a `WhenColumn`, a
  special type of `Column` with two extra methods: `when` and `otherwise`.
- Changed `functions.sort_arrays`'s signature to make it consistent with `transform`


### 0.3.4

#### Features

- added `Column[...]` (`__getitem`) that can be used to access struct or array elements.

#### Bugfixes

- fixed various bugs in transformations.analyze
  - Was crashing on ARRAY<STRUCT<ARRAY<...>>>
  - Was crashing on columns of type BYTES
  - Columns used in group_by were analyzed, which is useless because the group is constant


### 0.3.3

#### Breaking changes

- The `bigquery_frame.transformations.analyze` now return one extra column named `column_number`

#### Features

- Add various Column methods: `asc`, `desc`
- Add various functions methods: `replace`, `array`, 

#### Bugfixes

- `Dataframe.sort` now works on aliased columns
- `Column.alias` now works on sql keywords
- `functions.eqNullSafe` now never returns NULL
- fix incorrect line numbering when query was displayed on error


### 0.3.2

#### Breaking changes

- `Column` constructor no longer accept `alias` as argument. Use `Column.alias()` instead.

#### Features

- Add various Column methods: `cast`, `~`, `isNull`, `isNotNull`, `eqNullSafe`
- Add various functions methods: `concat`, `length`, `sort_array`, `substring`

#### Bugfixes
- Fix broken `Column.when().otherwise().alias()` 

### 0.3.1

#### Breaking changes
- Dropped support for Python 3.6
- Bumped dependencies versions
- DataFrame.toPandas() now requires extra permissions by default
  (the BigQuery ReadSession User role), but downloads data faster.

#### Features
- Added `functions.cast()` method
- We now print the whole query in the error message when it fails
- Added `DataFrame.join()`. This is a first implementation which is a little clumsy.

#### Bugfixes
- Fix DataFrame deps being lost when using `df.alias()`

