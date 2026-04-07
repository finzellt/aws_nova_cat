#!/usr/bin/env bash
# deploy.sh — Nova Cat deploy script
#
# Builds the nova_common_layer, then deploys the CDK stack.
# After a successful deploy that includes the NovaCat stack, captures
# CloudFormation outputs and injects them as environment variables into
# ~/.zshrc so that all new terminals have access to resource names.
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
echo "==> [1/4] Building nova_common_layer..."

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
echo "==> [2/4] Resolving AWS account ID..."
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "    Account: $ACCOUNT_ID"
echo ""

# ---------------------------------------------------------------------------
# Step 3 — CDK deploy
#
# Deploys both the production stack (NovaCat) and the smoke test stack
# (NovaCatSmoke). Pass a stack name to target a specific stack, e.g.:
#   ./deploy.sh NovaCat
#   ./deploy.sh NovaCatSmoke
# ---------------------------------------------------------------------------
echo "==> [3/4] Deploying CDK stacks..."
cd "$REPO_ROOT/infra"
# Separate stack names from CDK flags.
# Stack names are positional args that don't start with -.
# All other args (--hotswap, -c env=prod, etc.) are passed through as flags.
STACKS=()
FLAGS=()
for arg in "$@"; do
  if [[ "$arg" == -* ]]; then
    FLAGS+=("$arg")
  else
    STACKS+=("$arg")
  fi
done
if [[ ${#STACKS[@]} -eq 0 ]]; then
  STACKS=(NovaCat NovaCatSmoke)
fi
cdk deploy \
  -c account="$ACCOUNT_ID" \
  --require-approval never \
  "${FLAGS[@]+"${FLAGS[@]}"}" \
  "${STACKS[@]}"

echo ""

# ---------------------------------------------------------------------------
# Step 4 — Capture NovaCat stack outputs as shell environment variables
#
# Only runs when the NovaCat (production) stack was among the deployed
# stacks. Queries CloudFormation, extracts resource names, and writes a
# fenced block into ~/.zshrc and .venv/bin/activate. The block is
# replaced on each deploy so values stay current without manual
# intervention.
#
# Variables exported:
#   NOVACAT_TABLE_NAME            — main DynamoDB table
#   NOVACAT_PHOTOMETRY_TABLE_NAME — dedicated photometry DynamoDB table
#   NOVACAT_PRIVATE_BUCKET        — private data S3 bucket
#   NOVACAT_PUBLIC_SITE_BUCKET    — public site S3 bucket (releases)
#   NOVACAT_QUARANTINE_TOPIC_ARN  — SNS quarantine/ops notifications topic
#   NOVACAT_CF_DOMAIN             — CloudFront distribution domain
#   NOVACAT_CF_DISTRIBUTION_ID    — CloudFront distribution ID
#   NOVACAT_INIT_ARN              — initialize_nova state machine ARN
#   NOVACAT_INGEST_TICKET_ARN     — ingest_ticket state machine ARN
# ---------------------------------------------------------------------------

# Check if NovaCat was among the deployed stacks.
_NOVACAT_DEPLOYED=false
for s in "${STACKS[@]}"; do
  [[ "$s" == "NovaCat" ]] && _NOVACAT_DEPLOYED=true
done

if $_NOVACAT_DEPLOYED; then
  echo "==> [4/4] Capturing NovaCat stack outputs..."

  _ENV_BLOCK=$(aws cloudformation describe-stacks \
    --stack-name NovaCat \
    --query 'Stacks[0].Outputs' \
    --output json 2>/dev/null | python3 -c "
import json, sys

outputs = json.load(sys.stdin)

# Build a key → value lookup from OutputKey substrings.
# OutputKeys include a CDK hash suffix (e.g. StorageTableNameB6E3E5D6)
# so we match on the human-readable prefix.
lookup = {}
for o in outputs:
    k = o['OutputKey']
    v = o['OutputValue']
    lookup[k] = v

def find(substring):
    \"\"\"Find the first output whose key contains the substring.\"\"\"
    for k, v in lookup.items():
        if substring in k:
            return v
    return None

pairs = [
    ('NOVACAT_TABLE_NAME',            find('StorageTableName')),
    ('NOVACAT_PHOTOMETRY_TABLE_NAME', find('StoragePhotometryTableName')),
    ('NOVACAT_PRIVATE_BUCKET',        find('StoragePrivateBucketName')),
    ('NOVACAT_PUBLIC_SITE_BUCKET',    find('StoragePublicSiteBucketName')),
    ('NOVACAT_QUARANTINE_TOPIC_ARN',  find('StorageQuarantineTopicArn')),
    ('NOVACAT_CF_DOMAIN',             find('DeliveryDistributionDomain')),
    ('NOVACAT_CF_DISTRIBUTION_ID',    find('DeliveryDistributionId')),
    ('NOVACAT_INIT_ARN',              find('InitializeNovaStateMachine')),
    ('NOVACAT_INGEST_TICKET_ARN',     find('IngestTicketStateMachine')),
]

for name, value in pairs:
    if value:
        print(f'export {name}=\"{value}\"')
    else:
        print(f'# {name} — not found in stack outputs')
")

  if [[ -n "$_ENV_BLOCK" ]]; then
    _MARKER_BEGIN="# >>> NovaCat env >>>"
    _MARKER_END="# <<< NovaCat env <<<"

    # Inject the fenced env block into a target file.
    # Replaces any existing block, then appends the new one.
    _inject_env_block() {
      local target="$1"
      if [[ -f "$target" ]]; then
        # Remove existing fenced block (if any).
        # Use a temp file to avoid sed -i portability issues (macOS vs GNU).
        local tmp
        tmp=$(mktemp)
        awk -v begin="$_MARKER_BEGIN" -v end="$_MARKER_END" '
          $0 == begin { skip=1; next }
          $0 == end   { skip=0; next }
          !skip
        ' "$target" > "$tmp"
        mv "$tmp" "$target"
      fi

      # Append the new block.
      {
        echo ""
        echo "$_MARKER_BEGIN"
        echo "# Auto-generated by deploy.sh — do not edit manually."
        echo "# Updated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "$_ENV_BLOCK"
        echo "$_MARKER_END"
      } >> "$target"
    }

    # Target 1: ~/.zshrc (new terminal sessions)
    _ZSHRC="${HOME}/.zshrc"
    _inject_env_block "$_ZSHRC"
    echo "    Wrote environment variables to $_ZSHRC"

    # Target 2: venv activate script (venv activations)
    _VENV_ACTIVATE="$REPO_ROOT/.venv/bin/activate"
    if [[ -f "$_VENV_ACTIVATE" ]]; then
      _inject_env_block "$_VENV_ACTIVATE"
      echo "    Wrote environment variables to $_VENV_ACTIVATE"
    else
      echo "    ⚠ Venv activate script not found at $_VENV_ACTIVATE — skipping."
    fi

    # Also export into the current shell so you don't need to open a new
    # terminal or re-activate the venv immediately after deploy.
    eval "$_ENV_BLOCK"

    echo ""
    echo "    Variables are available in this shell, new terminals, and venv activations."
    echo ""
    echo "    Current values:"
    echo "$_ENV_BLOCK" | sed 's/^/    /'
  else
    echo "    ⚠ Could not read NovaCat stack outputs — skipping env injection."
  fi
else
  echo "==> [4/4] Skipping env capture (NovaCat stack was not deployed)."
fi

echo ""
echo "==> Deploy complete."
