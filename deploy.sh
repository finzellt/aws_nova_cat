#!/usr/bin/env bash
# deploy.sh — Nova Cat deploy script
#
# Builds the nova_common_layer, then deploys the CDK stack.
#
# Usage:
#   ./deploy.sh               # dev deploy
#   ./deploy.sh -c env=prod   # prod deploy
#   ./deploy.sh --hotswap     # fast Lambda-only update (skip CloudFormation for code-only changes)
#
# Prerequisites:
#   - AWS credentials configured
#   - CDK bootstrapped: cd infra && cdk bootstrap
#   - Python 3.11+ and pip available
#   - Docker running (required for Docker-based Lambda image builds)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAYER_DIR="$REPO_ROOT/services/nova_common_layer"
LAYER_REQUIREMENTS="$REPO_ROOT/services/nova_common_layer_requirements.txt"

echo "==> Nova Cat deploy"
echo ""

# ---------------------------------------------------------------------------
# Step 1 — Build nova_common_layer
#
# The layer directory contains only python/ — no requirements.txt inside it,
# so CDK zips python/ contents at the root of the layer artifact as Lambda
# expects. Build instructions live in nova_common_layer_requirements.txt
# alongside the layer directory, not inside it.
# ---------------------------------------------------------------------------
echo "==> [1/3] Building nova_common_layer..."

# Install shared dependencies into python/
pip install \
  -r "$LAYER_REQUIREMENTS" \
  -t "$LAYER_DIR/python/" \
  --no-cache-dir \
  --quiet

echo "    Done."
echo ""

# ---------------------------------------------------------------------------
# Step 2 — Resolve AWS account ID
# ---------------------------------------------------------------------------
echo "==> [2/3] Resolving AWS account ID..."
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "    Account: $ACCOUNT_ID"
echo ""

# ---------------------------------------------------------------------------
# Step 3 — CDK deploy
# ---------------------------------------------------------------------------
echo "==> [3/3] Deploying CDK stack..."
cd "$REPO_ROOT/infra"
cdk deploy \
  -c account="$ACCOUNT_ID" \
  --require-approval never \
  "$@"

echo ""
echo "==> Deploy complete."
