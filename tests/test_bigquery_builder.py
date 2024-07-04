import pytest
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import Client

from bigquery_frame import BigQueryBuilder


def test_without_debug(client: Client):
    """GIVEN a BigQueryBuilder
    WHEN we run an incorrect query without the debug mode
    THEN it will fail only at the end
    """
    bq = BigQueryBuilder(client)
    df1 = bq.sql("""SELECT 1 as a""")
    df2 = df1.select("b")
    with pytest.raises(BadRequest):
        df2.show()


def test_with_debug(client: Client):
    """GIVEN a BigQueryBuilder
    WHEN we run an incorrect query without the debug mode
    THEN it will fail only at the end
    """
    bq = BigQueryBuilder(client, debug=True)
    df1 = bq.sql("""SELECT 1 as a""")
    with pytest.raises(BadRequest):
        df1.select("b")
