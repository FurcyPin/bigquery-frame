#!/bin/bash
set -e

poetry run bandit .
poetry run safety check
