Unlike those in [bigquery_frame.functions](/spark-frame/reference/functions), the methods in this module all take at least one
[DataFrame][pyspark.sql.DataFrame] as argument and return a new transformed DataFrame.
These methods generally offer _higher order_ transformation that requires to inspect the schema or event the content
of the input DataFrame(s) before generating the next transformation. Those are typically generic operations 
that _cannot_ be implemented with one single SQL query.

!!! tip

    Since Spark 3.3.0, all transformations can be inlined using 
    [DataFrame.transform][pyspark.sql.DataFrame.transform], like this:

    ```python
    df.transform(flatten).withColumn(
        "base_stats.Total",
        f.col("`base_stats.Attack`") + f.col("`base_stats.Defense`") + f.col("`base_stats.HP`") +
        f.col("`base_stats.Sp Attack`") + f.col("`base_stats.Sp Defense`") + f.col("`base_stats.Speed`")
    ).transform(unflatten).show(vertical=True, truncate=False)
    ```
    *This example is taken*

---

### ::: bigquery_frame.transformations_impl.analyze.analyze
---
### ::: bigquery_frame.transformations_impl.flatten.flatten
---
### ::: bigquery_frame.transformations_impl.harmonize_dataframes.harmonize_dataframes
---
### ::: bigquery_frame.transformations_impl.normalize_arrays.normalize_arrays
---
### ::: bigquery_frame.transformations_impl.pivot_unpivot.pivot
---
### ::: bigquery_frame.transformations_impl.pivot_unpivot.unpivot
---
### ::: bigquery_frame.transformations_impl.sort_all_arrays.sort_all_arrays
---
### ::: bigquery_frame.transformations_impl.sort_columns.sort_columns
---
### ::: bigquery_frame.transformations_impl.transform_all_fields.transform_all_fields
---
### ::: bigquery_frame.transformations_impl.union_dataframes.union_dataframes
---
