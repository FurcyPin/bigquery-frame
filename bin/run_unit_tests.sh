#!/bin/bash
set -e

source venv/bin/activate

export GCP_CREDENTIALS_PATH="gcp-credentials.json"
coverage run --include "bigquery_frame/*" -m unittest

coverage html
