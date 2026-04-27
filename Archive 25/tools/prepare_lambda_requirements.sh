#!/usr/bin/env bash
set -euo pipefail

# Prepare a requirements file for the Lambda function by removing packages
# that were moved into a Lambda Layer. Usage:
#   ./tools/prepare_lambda_requirements.sh numpy pyarrow pandas -o requirements-lambda.txt
# If no packages are provided, it will default to excluding common heavy libs.

OUT_FILE=${OUT_FILE:-requirements-lambda.txt}
if [ "$#" -eq 0 ]; then
  EXCLUDE_PKGS=(numpy pyarrow pandas cryptography psycopg2-binary psycopg2)
else
  EXCLUDE_PKGS=("$@")
fi

while [ "$#" -gt 0 ]; do
  case "$1" in
    -o|--output)
      OUT_FILE="$2"
      shift 2
      ;;
    --) shift; break;;
    *) break;;
  esac
done

if [ ! -f requirements.txt ]; then
  echo "requirements.txt not found in repo root" >&2
  exit 1
fi

# Build grep pattern
PATTERN="^($(printf "%s|" "${EXCLUDE_PKGS[@]}" | sed 's/|$//'))"

echo "Excluding packages: ${EXCLUDE_PKGS[*]}"
echo "Writing to: $OUT_FILE"

grep -v -Ei "$PATTERN|^-e |^#" requirements.txt > "$OUT_FILE" || true

# Ensure mangum is present so the Lambda handler can run
if ! grep -qi '^mangum' "$OUT_FILE" 2>/dev/null; then
  echo "mangum" >> "$OUT_FILE"
fi

echo "Prepared $OUT_FILE"
exit 0
