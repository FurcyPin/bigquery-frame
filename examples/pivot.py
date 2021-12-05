from bigquery_frame import BigQueryBuilder
from bigquery_frame.auth import get_bq_client
from bigquery_frame.transformations import unpivot, pivot

bigquery = BigQueryBuilder(get_bq_client())

# The data in this example has been inspired by:
# https://sparkbyexamples.com/spark/how-to-pivot-table-and-unpivot-a-spark-dataframe/

df = bigquery.sql("""
    SELECT 
        *
    FROM UNNEST ([
        STRUCT(2018 as year,  "Orange" as product, null as Canada, 4000 as China,  null as Mexico),
        STRUCT(2018 as year,   "Beans" as product, null as Canada, 1500 as China,  2000 as Mexico),
        STRUCT(2018 as year,  "Banana" as product, 2000 as Canada,  400 as China,  null as Mexico),
        STRUCT(2018 as year, "Carrots" as product, 2000 as Canada, 1200 as China,  null as Mexico),
        STRUCT(2019 as year,  "Orange" as product, 5000 as Canada, null as China,  5000 as Mexico),
        STRUCT(2019 as year,   "Beans" as product, null as Canada, 1500 as China,  2000 as Mexico),
        STRUCT(2019 as year,  "Banana" as product, null as Canada, 1400 as China,   400 as Mexico),
        STRUCT(2019 as year, "Carrots" as product, null as Canada,  200 as China,  null as Mexico)
    ])
""")
df.show()
# +------+---------+--------+-------+--------+
# | year | product | Canada | China | Mexico |
# +------+---------+--------+-------+--------+
# | 2018 | Orange  |        | 4000  |        |
# | 2018 |  Beans  |        | 1500  |  2000  |
# | 2018 | Banana  |  2000  |  400  |        |
# | 2018 | Carrots |  2000  | 1200  |        |
# | 2019 | Orange  |  5000  |       |  5000  |
# | 2019 |  Beans  |        | 1500  |  2000  |
# | 2019 | Banana  |        | 1400  |  400   |
# | 2019 | Carrots |        |  200  |        |
# +------+---------+--------+-------+--------+

unpivotted = unpivot(df, ['year', 'product'], key_alias='country', value_alias='Amount')
unpivotted.show(100)
# +------+---------+---------+--------+
# | year | product | country | Amount |
# +------+---------+---------+--------+
# | 2018 | Orange  | Canada  |        |
# | 2018 | Orange  |  China  |  4000  |
# | 2018 | Orange  | Mexico  |        |
# | 2018 |  Beans  | Canada  |        |
# | 2018 |  Beans  |  China  |  1500  |
# | 2018 |  Beans  | Mexico  |  2000  |
# | 2018 | Banana  | Canada  |  2000  |
# | 2018 | Banana  |  China  |  400   |
# | 2018 | Banana  | Mexico  |        |
# | 2018 | Carrots | Canada  |  2000  |
# | 2018 | Carrots |  China  |  1200  |
# | 2018 | Carrots | Mexico  |        |
# | 2019 | Orange  | Canada  |  5000  |
# | 2019 | Orange  |  China  |        |
# | 2019 | Orange  | Mexico  |  5000  |
# | 2019 |  Beans  | Canada  |        |
# | 2019 |  Beans  |  China  |  1500  |
# | 2019 |  Beans  | Mexico  |  2000  |
# | 2019 | Banana  | Canada  |        |
# | 2019 | Banana  |  China  |  1400  |
# | 2019 | Banana  | Mexico  |  400   |
# | 2019 | Carrots | Canada  |        |
# | 2019 | Carrots |  China  |  200   |
# | 2019 | Carrots | Mexico  |        |
# +------+---------+---------+--------+

repivotted = pivot(unpivotted, group_columns=["year", "product"], pivot_column="country", agg_fun="sum", agg_col="amount")
repivotted.show()
# +------+---------+--------+-------+--------+
# | year | product | Canada | China | Mexico |
# +------+---------+--------+-------+--------+
# | 2018 | Orange  |        | 4000  |        |
# | 2018 |  Beans  |        | 1500  |  2000  |
# | 2018 | Banana  |  2000  |  400  |        |
# | 2018 | Carrots |  2000  | 1200  |        |
# | 2019 | Orange  |  5000  |       |  5000  |
# | 2019 |  Beans  |        | 1500  |  2000  |
# | 2019 | Banana  |        | 1400  |  400   |
# | 2019 | Carrots |        |  200  |        |
# +------+---------+--------+-------+--------+

assert(df.collect() == repivotted.collect())
