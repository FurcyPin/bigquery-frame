from bigquery_frame import BigQueryBuilder
from bigquery_frame.transformations_impl.union_dataframes import union_dataframes


def test_union_dataframes(bq: BigQueryBuilder):
    df1 = bq.sql("""SELECT 1 as id""")
    df2 = bq.sql("""SELECT 2 as id""")
    df3 = bq.sql("""SELECT 3 as id""")
    actual = union_dataframes([df1, df2, df3])
    expected = bq.sql("""SELECT id FROM UNNEST([1, 2, 3]) as id""")
    assert actual.collect() == expected.collect()
