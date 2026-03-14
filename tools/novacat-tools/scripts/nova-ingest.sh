#!/usr/bin/env zsh
# nova-ingest.sh — Trigger the NovaCat initialize_nova workflow.
#
# Usage:
#   ./nova-ingest.sh "V1324 Sco"
#
# What it does:
#   1. Generates a correlation_id (UUID) for tracing the full execution chain.
#   2. Constructs the initialize_nova input payload.
#   3. Starts a Step Functions execution against the production state machine.
#   4. Prints the execution ARN and correlation_id.
#
# The correlation_id is threaded through every downstream workflow and Lambda
# log event. Use it with novacat_logs.py to trace the full execution chain.

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
STATE_MACHINE_ARN="arn:aws:states:us-east-1:433980486697:stateMachine:nova-cat-initialize-nova"
AWS_REGION="us-east-1"

# ── Validate input ────────────────────────────────────────────────────────────
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <nova_name>" >&2
  echo "  Example: $0 \"V1324 Sco\"" >&2
  exit 1
fi

CANDIDATE_NAME="$1"

# ── Generate correlation_id ───────────────────────────────────────────────────
CORRELATION_ID=$(python3 -c "import uuid; print(str(uuid.uuid4()))")

# ── Generate execution name ───────────────────────────────────────────────────
# SFN execution names must be unique, ≤80 chars, no spaces or special chars.
NORMALIZED=$(echo "$CANDIDATE_NAME" \
  | tr '[:upper:]' '[:lower:]' \
  | tr ' ' '-' \
  | tr -cd '[:alnum:]-' \
  | cut -c1-50)
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
EXECUTION_NAME="${NORMALIZED}-${TIMESTAMP}"

# ── Build input payload ───────────────────────────────────────────────────────
INPUT_JSON=$(python3 -c "
import json, sys
print(json.dumps({
    'candidate_name': sys.argv[1],
    'correlation_id': sys.argv[2],
    'source': 'operator',
    'attributes': {},
}))
" "$CANDIDATE_NAME" "$CORRELATION_ID")

# ── Fire ──────────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  NovaCat — Initialize Nova"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Nova:           $CANDIDATE_NAME"
echo "  Correlation ID: $CORRELATION_ID"
echo "  Execution Name: $EXECUTION_NAME"
echo ""

RESPONSE=$(aws stepfunctions start-execution \
  --region "$AWS_REGION" \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "$EXECUTION_NAME" \
  --input "$INPUT_JSON")

EXECUTION_ARN=$(echo "$RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['executionArn'])")
START_DATE=$(echo  "$RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['startDate'])")

echo "  ✓ Execution started"
echo "  Execution ARN:  $EXECUTION_ARN"
echo "  Started at:     $START_DATE"
echo ""
echo "  ── Trace this execution ──────────────────────────────────"
echo "  correlation_id  = $CORRELATION_ID"
echo ""
echo "  In novacat_logs.py (notebook or CLI):"
echo "    logs.trace(\"$CORRELATION_ID\")"
echo ""
echo "  In AWS Console:"
echo "  https://us-east-1.console.aws.amazon.com/states/home#/executions/details/$EXECUTION_ARN"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
