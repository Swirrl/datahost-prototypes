#!/usr/bin/env bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")

docker-compose -f "${SCRIPT_DIR}/docker-compose.yml" run --rm terraform "$@"