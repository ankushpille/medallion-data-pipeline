#!/usr/bin/env bash
set -euo pipefail

# Build a deployable AWS Lambda zip for this FastAPI app using Mangum.
# Usage: ./tools/build_lambda_zip.sh [output.zip]
#
# Notes:
# - This script will install Python dependencies from requirements.txt into a
#   temporary build folder, excluding 'uvicorn' (not needed for Lambda) and
#   ensuring 'mangum' is present so the FastAPI ASGI app can be invoked by
#   Lambda. It then copies project files (excluding large/dev-only items)
#   and creates a single zip file ready for Lambda upload.
# - Do NOT embed AWS credentials in the zip. Provide credentials via the
#   Lambda console, CLI, or use an IAM role for the function.

# Optional: pass custom requirements file as first arg, and output zip as second arg
REQ_FILE_ARG=""
if [ $# -ge 1 ] && [ -f "$1" ]; then
  REQ_FILE_ARG="$1"
  shift
fi
OUTPUT=${1:-lambda_deploy.zip}
HERE=$(pwd)
BUILD_DIR="$HERE/.lambda_build"

echo "Preparing lambda package -> $OUTPUT"

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Prepare a temporary requirements file excluding uvicorn and dev-only entries
REQ_SOURCE="requirements.txt"
if [ -n "$REQ_FILE_ARG" ]; then
  REQ_SOURCE="$REQ_FILE_ARG"
fi

if [ -f "$REQ_SOURCE" ]; then
  TMP_REQ="/tmp/req_lambda_$$.txt"
  # Exclude common non-lambda packages like uvicorn and any editable installs
  grep -v -Ei '^(uvicorn|\-e |#)' "$REQ_SOURCE" > "$TMP_REQ" || true
  # Ensure mangum is present
  if ! grep -qi '^mangum' "$TMP_REQ" 2>/dev/null; then
    echo "mangum" >> "$TMP_REQ"
  fi
  echo "Installing python packages into build dir (this may take a while)..."
  pip install --upgrade -r "$TMP_REQ" -t "$BUILD_DIR"
  rm -f "$TMP_REQ"
else
  echo "No requirements.txt found — installing mangum only"
  pip install --upgrade mangum -t "$BUILD_DIR"
fi

# Remove unnecessary files from installed packages to reduce zip size
find "$BUILD_DIR" -name '*.pyc' -delete || true
rm -rf $(find "$BUILD_DIR" -maxdepth 1 -type d -name 'tests' -print0 | xargs -0 2>/dev/null) || true

# Copy project files into the build dir, excluding large/dev items
rsync -av --copy-unsafe-links \
  --exclude '.git' \
  --exclude 'frontend' \
  --exclude 'build' \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  --exclude 'README.md' \
  --exclude '.venv' \
  --exclude 'venv' \
  --exclude '.lambda_build' \
  --exclude 'dea.log' \
  --exclude 'uv.lock' \
  --exclude "$OUTPUT" \
  ./ "$BUILD_DIR/"

# Ensure a Lambda handler exists that wraps the FastAPI app with Mangum
if [ ! -f "$BUILD_DIR/lambda_handler.py" ]; then
  cat > "$BUILD_DIR/lambda_handler.py" <<'PY'
from mangum import Mangum
from main import app

# AWS Lambda handler
handler = Mangum(app)
PY
fi

# Optionally remove uvicorn if it's present in installed packages
rm -rf "$BUILD_DIR/uvicorn" || true

# Create the zip
pushd "$BUILD_DIR" >/dev/null
zip -r9 "$HERE/$OUTPUT" . >/dev/null
popd >/dev/null

echo "Created $OUTPUT"
echo "Tip: do NOT include static frontend builds unless you want the function to serve them; consider hosting static files on S3 + CloudFront."

# Clean up
rm -rf "$BUILD_DIR"

echo "Done. Upload $OUTPUT to Lambda (use console, CLI, or Terraform)."

exit 0
