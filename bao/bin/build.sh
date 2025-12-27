#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
JAVA_DIR="$PROJECT_DIR/java"
LIB_DIR="$PROJECT_DIR/lib"
JAR_FILE="$LIB_DIR/trexsql.jar"

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
