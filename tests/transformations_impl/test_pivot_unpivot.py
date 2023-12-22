from google.cloud.bigquery import Row

from bigquery_frame import BigQueryBuilder
from bigquery_frame import functions as f
from bigquery_frame.transformations_impl.pivot_unpivot import __get_test_pivoted_df as get_test_pivoted_df
from bigquery_frame.transformations_impl.pivot_unpivot import __get_test_unpivoted_df as get_test_unpivoted_df
from bigquery_frame.transformations_impl.pivot_unpivot import pivot, unpivot


def test_pivot_v2(bq: BigQueryBuilder):
    df = get_test_unpivoted_df(bq)
    pivoted = pivot(
        df,
        pivot_column="country",
        aggs=["sum(amount)"],
    )
    expected = get_test_pivoted_df(bq)
    assert pivoted.collect() == expected.collect()


def test_pivot_v2_case_sensitive(bq: BigQueryBuilder):
    df = get_test_unpivoted_df(bq)
    pivoted = pivot(
        df,
        pivot_column="COUNTRY",
        aggs=["SUM(AMOUNT)"],
    )
    expected = get_test_pivoted_df(bq)
    assert pivoted.collect() == expected.collect()


def test_pivot_v2_with_col_aggs(bq: BigQueryBuilder):
    df = get_test_unpivoted_df(bq)
    pivoted = pivot(
        df,
        pivot_column="country",
        aggs=[f.sum(f.col("amount"))],
    )
    expected = get_test_pivoted_df(bq)
    assert pivoted.collect() == expected.collect()


def test_pivot_v2_with_multiple_aggs(bq: BigQueryBuilder):
    df = get_test_unpivoted_df(bq)
    pivoted = pivot(
        df.drop("product"),
        pivot_column="country",
        aggs=["sum(amount) as total_amount", f.count(f.col("year")).alias("nb_years")],
    )
    print(pivoted.collect())
    expected = [
        Row(
            (9000, 8, 10200, 8, 9400, 8),
            {
                'total_amount_Canada': 0,
                'nb_years_Canada': 1,
                'total_amount_China': 2,
                'nb_years_China': 3,
                'total_amount_Mexico': 4,
                'nb_years_Mexico': 5
            }
        )
    ]
    assert pivoted.collect() == expected


def test_unpivot_v1(bq: BigQueryBuilder):
    df = get_test_pivoted_df(bq)
    unpivoted = unpivot(
        df,
        ["year", "product"],
        key_alias="country",
        value_alias="amount",
        implem_version=1,
    )
    expected = get_test_unpivoted_df(bq)
    assert unpivoted.collect() == expected.collect()


def test_unpivot_v2(bq: BigQueryBuilder):
    df = get_test_pivoted_df(bq)
    unpivoted = unpivot(
        df,
        ["year", "product"],
        key_alias="country",
        value_alias="amount",
        implem_version=2,
    )
    unpivoted = unpivoted.select("year", "product", "country", "amount")
    expected = get_test_unpivoted_df(bq)
    assert (
        unpivoted.sort("year", "product", "country").collect() == expected.sort("year", "product", "country").collect()
    )


def test_unpivot_v1_exclude_nulls(bq: BigQueryBuilder):
    df = get_test_pivoted_df(bq)
    unpivoted = unpivot(
        df,
        ["year", "product"],
        key_alias="country",
        value_alias="amount",
        exclude_nulls=True,
        implem_version=1,
    )
    expected = get_test_unpivoted_df(bq).where("amount IS NOT NULL")
    assert unpivoted.collect() == expected.collect()


def test_unpivot_v2_exclude_nulls(bq: BigQueryBuilder):
    df = get_test_pivoted_df(bq)
    unpivoted = unpivot(
        df,
        ["year", "product"],
        key_alias="country",
        value_alias="amount",
        exclude_nulls=True,
        implem_version=2,
    )
    unpivoted = unpivoted.select("year", "product", "country", "amount")
    expected = get_test_unpivoted_df(bq).where("amount IS NOT NULL")
    assert (
        unpivoted.sort("year", "product", "country").collect() == expected.sort("year", "product", "country").collect()
    )
