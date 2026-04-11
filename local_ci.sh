#!/usr/bin/env zsh
# Local CI — run from repo root.
# Usage:
#   ./ci.sh          Run all checks
#   ./ci.sh ruff     Ruff check + format only
#   ./ci.sh test     Pytest only
#   ./ci.sh mypy     Mypy only
#   ./ci.sh synth    CDK synth only
set -euo pipefail

run_ruff() {
  echo "── ruff check ──"
  python -m ruff check . --fix
  echo "── ruff format ──"
  python -m ruff format .
}

run_mypy() {
  echo "── mypy ──"
  python -m mypy .
}

run_test() {
  echo "── pytest ──"
  python -m pytest --ignore="tests/smoke" -v
}

run_synth() {
  echo "── cdk synth ──"
  cdk --app "python3 infra/app.py" synth
}

case "${1:-all}" in
  ruff)  run_ruff ;;
  mypy)  run_mypy ;;
  test)  run_test ;;
  synth) run_synth ;;
  all)
    run_ruff
    run_mypy
    run_test
    run_synth
    echo "✅ All checks passed."
    ;;
  *)
    echo "Unknown command: $1"
    echo "Usage: ./ci.sh [ruff|mypy|test|synth|all]"
    exit 1
    ;;
esac
