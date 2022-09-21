from bigquery_frame import BigQueryBuilder
from bigquery_frame.transformations_impl.pivot_unpivot import (
    __get_test_pivoted_df as get_test_pivoted_df,
)
from bigquery_frame.transformations_impl.pivot_unpivot import (
    __get_test_unpivoted_df as get_test_unpivoted_df,
)
from bigquery_frame.transformations_impl.pivot_unpivot import pivot, unpivot


def test_pivot_v1(bq: BigQueryBuilder):
    df = get_test_unpivoted_df(bq)
    pivoted = pivot(
        df,
        pivot_column="country",
        agg_fun="sum",
        agg_col="amount",
        implem_version=1,
    )
    expected = get_test_pivoted_df(bq)
    assert pivoted.collect() == expected.collect()


def test_pivot_v1_case_sensitive(bq: BigQueryBuilder):
    df = get_test_unpivoted_df(bq)
    pivoted = pivot(
        df,
        pivot_column="COUNTRY",
        agg_fun="SUM",
        agg_col="AMOUNT",
        implem_version=1,
    )
    expected = get_test_pivoted_df(bq)
    assert pivoted.collect() == expected.collect()


def test_pivot_v2(bq: BigQueryBuilder):
    df = get_test_unpivoted_df(bq)
    pivoted = pivot(
        df,
        pivot_column="country",
        agg_fun="sum",
        agg_col="amount",
        implem_version=2,
    )
    expected = get_test_pivoted_df(bq)
    assert pivoted.collect() == expected.collect()


def test_pivot_v2_case_sensitive(bq: BigQueryBuilder):
    df = get_test_unpivoted_df(bq)
    pivoted = pivot(
        df,
        pivot_column="COUNTRY",
        agg_fun="SUM",
        agg_col="AMOUNT",
        implem_version=2,
    )
    expected = get_test_pivoted_df(bq)
    assert pivoted.collect() == expected.collect()


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
