#!/bin/bash
set -e

poetry run black .
poetry run ruff check . bigquery_frame tests
poetry run mypy bigquery_frame

