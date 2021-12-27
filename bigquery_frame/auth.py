import os
from typing import Optional

import google
from google.cloud.bigquery import Client
from google.oauth2 import service_account

from bigquery_frame import conf


def _get_bq_client_from_credentials() -> Optional[Client]:
    gcp_credentials_path = os.getenv("GCP_CREDENTIALS_PATH") or conf.GCP_CREDENTIALS_PATH
    if gcp_credentials_path.endswith(".json"):
        credentials = service_account.Credentials.from_service_account_file(
            gcp_credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = google.cloud.bigquery.Client(credentials=credentials, project=credentials.project_id)
        return client


def _get_bq_client_from_default() -> Client:
    gcp_project = os.getenv("GCP_PROJECT") or conf.GCP_PROJECT
    client = google.cloud.bigquery.Client(gcp_project)
    return client


def get_bq_client():
    client = _get_bq_client_from_credentials()
    if client is None:
        client = _get_bq_client_from_default()
    return client

