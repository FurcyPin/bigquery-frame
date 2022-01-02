#!/bin/bash
set -e

export GCP_CREDENTIALS_PATH="gcp-credentials.json"
poetry run nose2 -v

poetry run coverage html

