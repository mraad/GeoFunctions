#!/usr/bin/env bash
# Build both Scala lines and stage everything a release needs under dist/.
# Everything resolves relative to this script, so it runs from any working directory.
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"

# sed rather than gawk: gawk's match(a,r,arr) is a GNU extension and is not always installed.
GF_VER=$(grep -E '<version>' pom.xml | head -1 | sed -E 's|.*<version>(.*)</version>.*|\1|')
echo "GF_VER=${GF_VER}"

# Checked before either build: two Maven runs is a long way to go to be told the notebooks
# are wrong. The notebooks ship inside the zip and name the jar, so a concrete version goes
# stale and ships a release whose own examples cannot find it. They carry the 0.XX
# placeholder for the reader to fill in; reject any real version that creeps back in.
PINNED=$(grep -l 'geofunctions-[0-9][0-9.]*\.jar' notebooks/*.ipynb | sed 's/^/  /')
if [ -n "$PINNED" ]; then
  echo "ERROR: these notebooks hardcode a jar version; use geofunctions-0.XX.jar:" >&2
  echo "$PINNED" >&2
  exit 1
fi

# Not "dist": maven-clean-plugin is configured to delete build/ and dist/ (the setuptools
# output dirs), so `mvn clean` in the builds below would take the staged jars with it.
DIST=release
rm -rf "$DIST"
mkdir -p "$DIST"

# Both profiles shade to the same target/geofunctions-<ver>.jar, so each build's jar has to
# be copied out before the next `mvn clean` deletes it - and the copy is easy to get wrong,
# because maven-jar-plugin also attaches an UNSHADED target/geofunctions-<ver>-<scala>.jar
# whose name collides with what we stage for the 2.13 line. Shipping that one produces a jar
# that resolves nothing at runtime, and it looks right in a directory listing. Hence the
# check: a shaded jar bundles the Esri geometry library, the unshaded one does not, and
# ColumnCompat is the single class that differs per Scala line - 2.13 routes through
# org.apache.spark.sql.classic.ExpressionUtils, 2.12 calls new Column(Expression).
verify_jar() { # <jar> <2.12|2.13>
  local jar="$1" line="$2" geometry compat
  geometry=$(unzip -l "$jar" | grep -c 'com/esri/core/geometry/' || true)
  if [ "$geometry" -eq 0 ]; then
    echo "ERROR: ${jar} is not the shaded jar (no com/esri/core/geometry entries)" >&2
    exit 1
  fi
  compat=$(unzip -p "$jar" 'org/apache/spark/sql/esri/ColumnCompat$.class' 2>/dev/null \
    | LC_ALL=C strings | grep -c 'classic/ExpressionUtils' || true)
  case "$line" in
    2.13) [ "$compat" -gt 0 ] || { echo "ERROR: ${jar} is not the Scala 2.13 build" >&2; exit 1; } ;;
    2.12) [ "$compat" -eq 0 ] || { echo "ERROR: ${jar} is not the Scala 2.12 build" >&2; exit 1; } ;;
  esac
  echo "  verified ${jar} (shaded, Scala ${line})"
}

# Scala 2.13 / Spark 4.0 first, so the default-profile build below is what target/ is left
# holding - that is the one people expect to find there.
mvn -P spark-4.0 clean package
cp "target/geofunctions-${GF_VER}.jar" "${DIST}/geofunctions-${GF_VER}-2.13.jar"
verify_jar "${DIST}/geofunctions-${GF_VER}-2.13.jar" 2.13

# Default profile: Spark 3.5 / Scala 2.12. This is what ArcGIS Pro runs and what the zip
# carries; the wheel is pure Python and serves both lines.
mvn clean package
python3 -m pip wheel --no-deps --wheel-dir=target .
cp "target/geofunctions-${GF_VER}.jar" "${DIST}/geofunctions-${GF_VER}.jar"
verify_jar "${DIST}/geofunctions-${GF_VER}.jar" 2.12

zip -j "${DIST}/geofunctions-${GF_VER}.zip"\
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

echo
echo "Staged in ${DIST}/:"
ls -1 "${DIST}"
echo
echo "Release with:"
echo "  gh release create v${GF_VER} ${DIST}/geofunctions-${GF_VER}.zip \\"
echo "    ${DIST}/geofunctions-${GF_VER}.jar ${DIST}/geofunctions-${GF_VER}-2.13.jar \\"
echo "    --title '${GF_VER}' --notes-file <notes.md>"
