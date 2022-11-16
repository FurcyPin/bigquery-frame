from uuid import uuid4

import pytest
from google.cloud.bigquery import Client, Dataset

from bigquery_frame.auth import get_bq_client
from bigquery_frame.bigquery_builder import BigQueryBuilder


@pytest.fixture(scope="session")
def random_test_dataset() -> str:
    random_id = uuid4()
    return "test_dataset_" + str(random_id).replace("-", "_")


@pytest.fixture(scope="session")
def client(random_test_dataset: str) -> Client:
    client = get_bq_client()
    dataset = Dataset(f"{client.project}.{random_test_dataset}")
    dataset.location = "EU"
    client.create_dataset(dataset, exists_ok=True)
    yield client
    client.delete_dataset(dataset, delete_contents=True)
    client.close()


@pytest.fixture()
def bq(client: Client) -> BigQueryBuilder:
    bq = BigQueryBuilder(client)
    return bq
