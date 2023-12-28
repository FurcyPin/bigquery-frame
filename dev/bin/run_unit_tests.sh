#!/bin/bash
set -e

export GCP_CREDENTIALS_PATH="gcp-credentials.json"
poetry run pytest -n 32 "$@"

poetry run coverage xml
