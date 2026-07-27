#!/usr/bin/env bash
# Build the jar + wheel and zip the distribution bundle.
# Everything resolves relative to this script, so it runs from any working directory.
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"

mvn clean package
python3 -m pip wheel --no-deps --wheel-dir=target .

# sed rather than gawk: gawk's match(a,r,arr) is a GNU extension and is not always installed.
GF_VER=$(grep -E '<version>' pom.xml | head -1 | sed -E 's|.*<version>(.*)</version>.*|\1|')
echo "GF_VER=${GF_VER}"

rm -f "geofunctions-${GF_VER}.zip"
zip -j "geofunctions-${GF_VER}.zip"\
 LICENSE\
 README.md\
 data/world.zip\
 data/Miami.gdb.zip\
 "target/geofunctions-${GF_VER}-py3-none-any.whl"\
 "target/geofunctions-${GF_VER}.jar"\
 notebooks/*.ipynb\
 toolbox/ParquetToolbox.pyt\
 toolbox/ExtentToolbox.pyt\
 toolbox/Extent.lyrx
