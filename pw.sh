#!/usr/bin/env bash
# Inner dev loop: jar + wheel, no distribution zip. Runs from any working directory.
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"
mvn clean package
python3 -m pip wheel --no-deps --wheel-dir=target .
