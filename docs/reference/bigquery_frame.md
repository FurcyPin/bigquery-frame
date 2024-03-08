The core of BigQuery reproduces the pyspark.sql API, as summarized by this correspondence table:


| pyspark                  | bigquery_frame                 |
|--------------------------|--------------------------------|
| pyspark.sql.SparkSession | bigquery_frame.BigQueryBuilder |
| pyspark.sql.DataFrame    | bigquery_frame.DataFrame       |
| pyspark.sql.Column       | bigquery_frame.Column          |
| pyspark.sql.functions    | bigquery_frame.functions       |

---
### ::: bigquery_frame.BigQueryBuilder
    options:
      filters:
        - "!^_"

---
### ::: bigquery_frame.DataFrame
    options:
      filters:
        - "!^_"

---
### ::: bigquery_frame.grouped_data.GroupedData
    options:
      filters:
        - "!^_"

---
### ::: bigquery_frame.Column
    options:
      filters:
        - "!^_"

---
### ::: bigquery_frame.column.WhenColumn
    options:
      filters:
        - "!^_"
