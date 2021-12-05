import unittest

from bigquery_frame import BigQueryBuilder
from bigquery_frame.auth import get_bq_client
from bigquery_frame.transformations import unpivot, pivot

pivoted_df_query = """
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
"""

unpivoted_df_query = """
    SELECT 
        *
    FROM UNNEST ([
        STRUCT(2018 as year, "Orange"  as product, "Canada" as country, null as amount),
        STRUCT(2018 as year, "Orange"  as product,  "China" as country, 4000 as amount),
        STRUCT(2018 as year, "Orange"  as product, "Mexico" as country, null as amount),
        STRUCT(2018 as year,  "Beans"  as product, "Canada" as country, null as amount),
        STRUCT(2018 as year,  "Beans"  as product,  "China" as country, 1500 as amount),
        STRUCT(2018 as year,  "Beans"  as product, "Mexico" as country, 2000 as amount),
        STRUCT(2018 as year, "Banana"  as product, "Canada" as country, 2000 as amount),
        STRUCT(2018 as year, "Banana"  as product,  "China" as country, 400 as amount),
        STRUCT(2018 as year, "Banana"  as product, "Mexico" as country, null as amount),
        STRUCT(2018 as year, "Carrots" as product, "Canada" as country, 2000 as amount),
        STRUCT(2018 as year, "Carrots" as product,  "China" as country, 1200 as amount),
        STRUCT(2018 as year, "Carrots" as product, "Mexico" as country, null as amount),
        STRUCT(2019 as year, "Orange"  as product, "Canada" as country, 5000 as amount),
        STRUCT(2019 as year, "Orange"  as product,  "China" as country, null as amount),
        STRUCT(2019 as year, "Orange"  as product, "Mexico" as country, 5000 as amount),
        STRUCT(2019 as year,  "Beans"  as product, "Canada" as country, null as amount),
        STRUCT(2019 as year,  "Beans"  as product,  "China" as country, 1500 as amount),
        STRUCT(2019 as year,  "Beans"  as product, "Mexico" as country, 2000 as amount),
        STRUCT(2019 as year, "Banana"  as product, "Canada" as country, null as amount),
        STRUCT(2019 as year, "Banana"  as product,  "China" as country, 1400 as amount),
        STRUCT(2019 as year, "Banana"  as product, "Mexico" as country, 400 as amount),
        STRUCT(2019 as year, "Carrots" as product, "Canada" as country, null as amount),
        STRUCT(2019 as year, "Carrots" as product,  "China" as country, 200 as amount),
        STRUCT(2019 as year, "Carrots" as product, "Mexico" as country, null as amount)
    ])
"""


class TestTransformations(unittest.TestCase):

    bigquery = BigQueryBuilder(get_bq_client())

    def test_pivot(self):
        df = self.bigquery.sql(unpivoted_df_query)
        pivoted = pivot(df, group_columns=["year", "product"], pivot_column="country", agg_fun="sum", agg_col="amount")
        expected = self.bigquery.sql(pivoted_df_query)
        self.assertEqual(expected.collect(), pivoted.collect())

    def test_unpivot(self):
        df = self.bigquery.sql(pivoted_df_query)
        unpivoted = unpivot(df, ['year', 'product'], key_alias='country', value_alias='amount')
        expected = self.bigquery.sql(unpivoted_df_query)
        self.assertEqual(expected.collect(), unpivoted.collect())
