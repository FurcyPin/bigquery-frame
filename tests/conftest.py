from uuid import uuid4

import pytest
from google.cloud.bigquery import Client, Dataset

from bigquery_frame.auth import get_bq_client
from bigquery_frame.bigquery_builder import BigQueryBuilder


@pytest.fixture(autouse=True, scope="session")
def random_test_dataset():
    id = uuid4()
    return "test_dataset_" + str(id).replace("-", "_")


@pytest.fixture(autouse=True, scope="session")
def client(random_test_dataset: str):
    client = get_bq_client()
    dataset = Dataset(f"{client.project}.{random_test_dataset}")
    dataset.location = "EU"
    client.create_dataset(dataset, exists_ok=True)
    yield client
    client.delete_dataset(dataset, delete_contents=True)
    client.close()


@pytest.fixture(autouse=True, scope="session")
def bq(client: Client):
    bq = BigQueryBuilder(client)
    yield bq
