#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LIB_DIR="$PROJECT_DIR/lib"

TREXSQL_VERSION="${TREXSQL_VERSION:-0.1.5}"
JAR_URL="https://github.com/p-hoffmann/trex-java/releases/download/v${TREXSQL_VERSION}/trexsql-${TREXSQL_VERSION}.jar"
JAR_FILE="$LIB_DIR/trexsql.jar"

mkdir -p "$LIB_DIR"

VERSION_FILE="$LIB_DIR/.version"

if [[ -f "$JAR_FILE" && -f "$VERSION_FILE" ]]; then
    CURRENT_VERSION=$(cat "$VERSION_FILE")
    if [[ "$CURRENT_VERSION" == "$TREXSQL_VERSION" ]]; then
        echo "JAR already exists at version $TREXSQL_VERSION: $JAR_FILE"
        exit 0
    fi
    echo "Upgrading from $CURRENT_VERSION to $TREXSQL_VERSION"
fi

echo "Downloading trexsql v${TREXSQL_VERSION}..."
curl -L -o "$JAR_FILE" "$JAR_URL"

echo "$TREXSQL_VERSION" > "$VERSION_FILE"

echo "Downloaded: $JAR_FILE"
ls -lh "$JAR_FILE"
