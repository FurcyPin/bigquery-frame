{% include-markdown "../README.md" %}

## Usage

Bigquery-frame contains several utility methods, all documented in the [reference](/reference). 
There are grouped into several modules:

- [functions](reference#bigquery_framefunctions): 
  Extra functions similar to [pyspark.sql.functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html).
- [graph](reference#bigquery_framegraph):
  Implementations of graph algorithms.
- [transformations](reference#bigquery_frametransformations):
  Generic transformations taking one or more input DataFrames as argument and returning a new DataFrame.
- [schema_utils](reference#bigquery_frameschema_utils):
  Methods useful for manipulating DataFrame schemas.

