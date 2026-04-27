#!/usr/bin/env bash
set -euo pipefail

# Build a Lambda layer zip containing Python packages using a build container.
# Usage:
#   ./tools/build_lambda_layer.sh -r requirements-layer.txt -o layer.zip
# or
#   ./tools/build_lambda_layer.sh -p "numpy pyarrow pandas" -o layer.zip
#
OUT_ZIP=${OUT_ZIP:-layer.zip}
REQ_FILE=""
PKGS=""
DOCKER_FLAGS=${DOCKER_FLAGS:-}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -r|--requirements) REQ_FILE="$2"; shift 2;;
    -p|--packages) PKGS="$2"; shift 2;;
    -o|--output) OUT_ZIP="$2"; shift 2;;
    -h|--help) echo "Usage: $0 [-r requirements.txt] [-p 'pkg1 pkg2'] [-o layer.zip]"; exit 0;;
    *) echo "Unknown arg $1"; exit 1;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required to build manylinux-compatible wheels. Install Docker and try again." >&2
  exit 1
fi

BUILD_DIR=$(mktemp -d)
echo "Using build dir: $BUILD_DIR"

# Copy requirements or create temp requirements file
if [[ -n "$REQ_FILE" ]]; then
  cp "$REQ_FILE" "$BUILD_DIR/requirements-layer.txt"
else
  if [[ -z "$PKGS" ]]; then
    echo "Provide either --requirements or --packages" >&2
    exit 1
  fi
  # write each package on its own line (packages may be space-separated)
  printf "%s\n" $PKGS > "$BUILD_DIR/requirements-layer.txt"
fi

# Use a manylinux2014 container (quay) to build manylinux-compatible wheels.
# Default Python version for the layer build is 3.10. You can override by
# exporting PYTHON_VERSION (e.g., PYTHON_VERSION=3.10).
PYTHON_VERSION=${PYTHON_VERSION:-3.10}
MAJOR=${PYTHON_VERSION%%.*}
MINOR=${PYTHON_VERSION#*.}
PY_TAG="cp${MAJOR}${MINOR}-cp${MAJOR}${MINOR}"

echo "Building layer for Python $PYTHON_VERSION (manylinux) using /opt/python/$PY_TAG pip"
PIP_ONLY_BINARY=${PIP_ONLY_BINARY:-1}
if [ "$PIP_ONLY_BINARY" = "1" ]; then
  PIP_INSTALL_OPTS='--only-binary=:all:'
else
  PIP_INSTALL_OPTS=''
fi

echo "Running pip with options: $PIP_INSTALL_OPTS"
docker run $DOCKER_FLAGS --rm -v "$BUILD_DIR":/var_task quay.io/pypa/manylinux2014_x86_64 /bin/bash -lc \
  "/opt/python/${PY_TAG}/bin/pip install --upgrade pip && /opt/python/${PY_TAG}/bin/pip install $PIP_INSTALL_OPTS -r /var_task/requirements-layer.txt -t /var_task/python && chown -R $(id -u):$(id -g) /var_task/python" || {
    echo "\nLayer build failed. Common causes: a package (pyarrow) requires building from source and needs system C++ libs (Arrow)." >&2
    echo "Options to proceed:" >&2
    echo "  * Remove pyarrow from the layer and use a different deployment (container) or S3;" >&2
    echo "  * Try allowing source builds by exporting PIP_ONLY_BINARY=0 and re-running (may still fail and will be slow);" >&2
    echo "  * Build pyarrow using a full Arrow C++ toolchain (advanced) or use a prebuilt wheel for manylinux if available." >&2
    echo "Example: export PIP_ONLY_BINARY=0; export DOCKER_FLAGS='--platform linux/amd64'; ./tools/build_lambda_layer.sh -p \"numpy pyarrow pandas\" -o heavy-layer.zip" >&2
    exit 1
  }

pushd "$BUILD_DIR" >/dev/null
zip -r9 "$OUT_ZIP" python >/dev/null
popd >/dev/null

mv "$BUILD_DIR/$OUT_ZIP" .
rm -rf "$BUILD_DIR"

echo "Created layer zip: $OUT_ZIP"
echo "Next: publish this layer with the deploy script or via 'aws lambda publish-layer-version' and attach the LayerVersionArn to your function."

exit 0
