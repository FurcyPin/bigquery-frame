import pytest

from bigquery_frame import BigQueryBuilder, DataFrame
from bigquery_frame.cli import diff
from tests.utils import captured_output


@pytest.fixture(autouse=True)
def df(bq: BigQueryBuilder, random_test_dataset: str) -> DataFrame:
    df = bq.sql(
        """
            SELECT * FROM UNNEST ([
                STRUCT(1 as id, [STRUCT(1 as a, 2 as b, 3 as c)] as my_array),
                STRUCT(2 as id, [STRUCT(1 as a, 2 as b, 3 as c)] as my_array),
                STRUCT(3 as id, [STRUCT(1 as a, 2 as b, 3 as c)] as my_array)
            ])
        """
    )
    return df


@pytest.fixture(autouse=True)
def t1(bq: BigQueryBuilder, random_test_dataset: str, df: DataFrame) -> str:
    df.write.save(f"{random_test_dataset}.t1")
    yield f"{random_test_dataset}.t1"
    bq._execute_query(f"DROP TABLE IF EXISTS {random_test_dataset}.t1")


@pytest.fixture(autouse=True)
def t2(bq: BigQueryBuilder, random_test_dataset: str, df: DataFrame) -> str:
    df.write.save(f"{random_test_dataset}.t2")
    yield f"{random_test_dataset}.t2"
    bq._execute_query(f"DROP TABLE IF EXISTS {random_test_dataset}.t2")


def test_cli_diff(t1: str, t2: str):
    with captured_output() as (stdout, stderr):
        diff.main(["--tables", f"{t1}", f"{t2}", "--join-cols", "id"])
    assert "Report exported" in stdout.getvalue()
