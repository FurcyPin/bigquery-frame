#!/bin/bash
set -e

poetry run black .
poetry run isort bigquery_frame tests
poetry run flake8 bigquery_frame tests
poetry run mypy bigquery_frame tests

