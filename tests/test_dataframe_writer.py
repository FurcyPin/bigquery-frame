import google
import pytest
from google.cloud.bigquery import Client

from bigquery_frame import BigQueryBuilder


@pytest.fixture(autouse=True)
def clean(bq: BigQueryBuilder, random_test_dataset: str):
    bq._execute_query(f"DROP TABLE IF EXISTS {random_test_dataset}.my_table")
    return


def test_write_with_mode_overwrite(bq: BigQueryBuilder, random_test_dataset: str):
    bq.sql("SELECT 1 as a").write.save(f"{random_test_dataset}.my_table", mode="OVERWRITE")
    bq.sql("SELECT 2 as b").write.mode("overwrite").save(f"{random_test_dataset}.my_table")
    df = bq.table(f"{random_test_dataset}.my_table")
    assert [r["b"] for r in df.collect()] == [2]


def test_write_with_mode_append(bq: BigQueryBuilder, random_test_dataset: str):
    bq.sql("SELECT 1 as a").write.save(f"{random_test_dataset}.my_table", mode="OVERWRITE")
    bq.sql("SELECT 2 as a").write.mode("append").save(f"{random_test_dataset}.my_table")
    df = bq.table(f"{random_test_dataset}.my_table").orderBy("a")
    assert [r["a"] for r in df.collect()] == [1, 2]


def test_write_with_mode_ignore(bq: BigQueryBuilder, random_test_dataset: str):
    bq.sql("SELECT 1 as a").write.save(f"{random_test_dataset}.my_table", mode="OVERWRITE")
    bq.sql("SELECT 2 as b").write.mode("ignore").save(f"{random_test_dataset}.my_table")
    df = bq.table(f"{random_test_dataset}.my_table")
    assert [r["a"] for r in df.collect()] == [1]


def test_write_with_mode_error(bq: BigQueryBuilder, random_test_dataset: str):
    bq.sql("SELECT 1 as a").write.save(f"{random_test_dataset}.my_table", mode="OVERWRITE")
    with pytest.raises(google.api_core.exceptions.Conflict) as e:
        bq.sql("SELECT 2 as a").write.mode("error").save(f"{random_test_dataset}.my_table")
    assert "409 " in str(e.value) and "Already Exists" in str(e.value)


def test_write_with_mode_errorifexists(bq: BigQueryBuilder, random_test_dataset: str):
    bq.sql("SELECT 1 as a").write.save(f"{random_test_dataset}.my_table", mode="OVERWRITE")
    with pytest.raises(google.api_core.exceptions.Conflict) as e:
        bq.sql("SELECT 2 as a").write.mode("errorifexists").save(f"{random_test_dataset}.my_table")
    assert "409 " in str(e.value) and "Already Exists" in str(e.value)


def test_write_with_options(bq: BigQueryBuilder, random_test_dataset: str, client: Client):
    df = bq.sql("SELECT 1 as a")
    options = {"description": "this is a test table", "labels": {"org_unit": "development"}}
    df.write.save(f"{random_test_dataset}.my_table", mode="OVERWRITE", **options)
    table = client.get_table(f"{random_test_dataset}.my_table")
    assert table.description == "this is a test table"
    assert table.labels == {"org_unit": "development"}
