#!/bin/bash
set -e

export GCP_CREDENTIALS_PATH="gcp-credentials.json"
poetry run pytest "$@"

poetry run coverage xml
