import json
import os
from json import JSONDecodeError
from typing import Optional

import google
from google.cloud.bigquery import Client
from google.oauth2 import service_account

from bigquery_frame import conf


def _get_bq_client_from_credential_files() -> Optional[Client]:
    gcp_credentials_path = os.getenv("GCP_CREDENTIALS_PATH") or conf.GCP_CREDENTIALS_PATH
    if gcp_credentials_path.endswith(".json"):
        credentials = service_account.Credentials.from_service_account_file(
            filename=gcp_credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = google.cloud.bigquery.Client(credentials=credentials, project=credentials.project_id)
        return client


def _get_bq_client_from_credentials() -> Optional[Client]:
    gcp_credentials = os.getenv("GCP_CREDENTIALS") or conf.GCP_CREDENTIALS
    try:
        json_credentials = json.loads(gcp_credentials)
    except JSONDecodeError:
        return None
    credentials = service_account.Credentials.from_service_account_info(
        info=json_credentials, scopes=["https://www.googleapis.com/auth/cloud-platform"]
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
        client = _get_bq_client_from_credential_files()
    if client is None:
        client = _get_bq_client_from_default()
    return client
