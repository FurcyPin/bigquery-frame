#!/bin/bash
set -e

export GCP_CREDENTIALS_PATH="gcp-credentials.json"
poetry run coverage run --include "bigquery_frame/*" -m unittest -v

poetry run coverage html
