#!/usr/bin/env bash
set -e
mvn clean package
python3 -m pip wheel --no-deps --wheel-dir=target .
export GF_VER=$(grep -E '<version>' pom.xml | head -1 | gawk 'match($0,/<version>([^<]*)<\/version>/,a){print a[1]}')
zip -j geofunctions-${GF_VER}.zip LICENSE README.md target/geofunctions-${GF_VER}-py3-none-any.whl target/geofunctions-${GF_VER}.jar notebooks/*.ipynb
