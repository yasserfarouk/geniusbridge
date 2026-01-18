#!/bin/bash
#
# Build script for GeniusBridge
# Compiles the Java source and creates a fat JAR with all dependencies
#

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
SRC_DIR="src"
OUT_DIR="out/production/geniusbridge"
JAR_DIR="out/artifacts/geniusbridge_jar"
JAR_NAME="geniusbridge.jar"
LIBS_DIR="libs"
LIBS="$LIBS_DIR/genius.jar:$LIBS_DIR/py4j0.10.8.1.jar"

# Use semicolon for Windows
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
    LIBS="$LIBS_DIR/genius.jar;$LIBS_DIR/py4j0.10.8.1.jar"
fi

echo "=== GeniusBridge Build Script ==="
echo ""

# Clean previous build
echo "Cleaning previous build..."
rm -rf "$OUT_DIR"
rm -f "$JAR_DIR/$JAR_NAME"

# Create output directories
echo "Creating output directories..."
mkdir -p "$OUT_DIR"
mkdir -p "$JAR_DIR"

# Compile
echo "Compiling Java sources..."
javac -cp "$LIBS" \
      -d "$OUT_DIR" \
      "$SRC_DIR/com/yasserm/negmasgeniusbridge/Main.java"

# Copy resources
echo "Copying resources..."
if [ -d "$SRC_DIR/com/yasserm/negmasgeniusbridge/resources" ]; then
    mkdir -p "$OUT_DIR/com/yasserm/negmasgeniusbridge"
    cp -r "$SRC_DIR/com/yasserm/negmasgeniusbridge/resources" "$OUT_DIR/com/yasserm/negmasgeniusbridge/"
fi
if [ -d "$SRC_DIR/resources" ]; then
    cp -r "$SRC_DIR/resources" "$OUT_DIR/"
fi

# Extract dependencies (create fat JAR)
echo "Extracting dependencies..."
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR"

# Extract genius.jar
unzip -q -o "$SCRIPT_DIR/$LIBS_DIR/genius.jar" -x "META-INF/MANIFEST.MF" "META-INF/*.SF" "META-INF/*.DSA" "META-INF/*.RSA" 2>/dev/null || true

# Extract py4j jar
unzip -q -o "$SCRIPT_DIR/$LIBS_DIR/py4j0.10.8.1.jar" -x "META-INF/MANIFEST.MF" "META-INF/*.SF" "META-INF/*.DSA" "META-INF/*.RSA" 2>/dev/null || true

# Copy compiled classes on top (overwrite if needed)
cp -r "$SCRIPT_DIR/$OUT_DIR/"* .

cd "$SCRIPT_DIR"

# Create fat JAR
echo "Creating fat JAR..."
jar cfm "$JAR_DIR/$JAR_NAME" \
    "$SRC_DIR/META-INF/MANIFEST.MF" \
    -C "$TEMP_DIR" .

# Cleanup
rm -rf "$TEMP_DIR"

echo ""
echo "=== Build complete ==="
echo "JAR created at: $JAR_DIR/$JAR_NAME"
echo ""

# Show JAR info
ls -lh "$JAR_DIR/$JAR_NAME"

# Copy to local NegMAS directory
NEGMAS_FILES_DIR="$HOME/negmas/files"
if [ -d "$NEGMAS_FILES_DIR" ]; then
    echo ""
    echo "Updating local NegMAS installation..."
    cp "$JAR_DIR/$JAR_NAME" "$NEGMAS_FILES_DIR/"
    echo "Copied to: $NEGMAS_FILES_DIR/$JAR_NAME"
else
    echo ""
    echo "Note: NegMAS files directory not found at $NEGMAS_FILES_DIR"
    echo "Run 'negmas genius-setup' first, or manually copy the JAR."
fi
