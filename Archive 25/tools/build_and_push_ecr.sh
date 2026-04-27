#!/usr/bin/env bash
set -euo pipefail

# Build Docker image and push to ECR. Usage:
# ./tools/build_and_push_ecr.sh <ecr-repo-name> [tag] [region]
# Example: ./tools/build_and_push_ecr.sh dea-lambda-image latest us-west-2

REPO_NAME=${1:-dea-lambda-image}
TAG=${2:-latest}
REGION=${3:-us-west-2}
DOCKERFILE=${4:-Dockerfile.lambda}

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URI="$AWS_ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME"

# Create ECR repo if not exists
if ! aws ecr describe-repositories --repository-names "$REPO_NAME" --region "$REGION" >/dev/null 2>&1; then
  echo "Creating ECR repository $REPO_NAME"
  aws ecr create-repository --repository-name "$REPO_NAME" --region "$REGION" >/dev/null
fi

# Authenticate Docker to ECR
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$AWS_ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"

# Build image (allow overriding Dockerfile as 4th arg)
echo "Building Docker image using Dockerfile: $DOCKERFILE"
docker build -f "$DOCKERFILE" -t "$REPO_NAME:$TAG" .

# Tag and push
docker tag "$REPO_NAME:$TAG" "$ECR_URI:$TAG"
docker push "$ECR_URI:$TAG"

echo "Pushed image: $ECR_URI:$TAG"
echo "$ECR_URI:$TAG"

exit 0
