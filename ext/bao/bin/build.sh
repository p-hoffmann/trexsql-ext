#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
JAVA_DIR="$PROJECT_DIR/java"
LIB_DIR="$PROJECT_DIR/lib"
JAR_FILE="$LIB_DIR/trexsql.jar"

# Version of trexsql-java to use (must have public signing key embedded)
TREXSQL_VERSION="v0.1.5"
DOWNLOAD_URL="https://github.com/p-hoffmann/trexsql-java/releases/download/${TREXSQL_VERSION}/trexsql-${TREXSQL_VERSION#v}.jar"

mkdir -p "$LIB_DIR"

# Check if JAR already exists
if [[ -f "$JAR_FILE" ]]; then
    echo "JAR already exists: $JAR_FILE"
    exit 0
fi

# Build from source
if [[ ! -d "$JAVA_DIR" ]]; then
    echo "Error: java/ directory not found at $JAVA_DIR"
    exit 1
fi

if ! command -v lein &> /dev/null; then
    echo "Error: Leiningen is required to build trexsql.jar"
    echo "Install from: https://leiningen.org/"
    exit 1
fi

# Download pre-built JDBC JAR from GitHub Releases (has signing key embedded)
# and install to local Maven repository so lein uses it instead of JitPack
M2_DIR="$HOME/.m2/repository/com/github/p-hoffmann/trexsql-java/${TREXSQL_VERSION}"
echo "Downloading JDBC driver from GitHub Releases (${TREXSQL_VERSION})..."
if curl -fsSL -o "/tmp/trexsql-jdbc.jar" "$DOWNLOAD_URL"; then
    echo "Downloaded JDBC driver, installing to local Maven repo..."
    mkdir -p "$M2_DIR"
    cp "/tmp/trexsql-jdbc.jar" "$M2_DIR/trexsql-java-${TREXSQL_VERSION}.jar"
    # Create minimal POM
    cat > "$M2_DIR/trexsql-java-${TREXSQL_VERSION}.pom" << 'POMEOF'
<?xml version="1.0" encoding="UTF-8"?>
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.github.p-hoffmann</groupId>
  <artifactId>trexsql-java</artifactId>
  <version>v0.1.5</version>
  <packaging>jar</packaging>
</project>
POMEOF
    rm -f "/tmp/trexsql-jdbc.jar"
    echo "Installed JDBC driver with signing key to local Maven repo"
else
    echo "Download failed, will use JitPack (extensions may fail signature verification)"
fi

echo "Building trexsql.jar from source..."
cd "$JAVA_DIR"
lein uberjar

# Find and copy the standalone jar
STANDALONE_JAR=$(ls target/trexsql-*-standalone.jar 2>/dev/null | head -1)
if [[ -n "$STANDALONE_JAR" ]]; then
    cp "$STANDALONE_JAR" "$JAR_FILE"
    echo "Built: $JAR_FILE"
    ls -lh "$JAR_FILE"
else
    echo "Error: Build succeeded but standalone JAR not found"
    exit 1
fi
