#!/usr/bin/env zsh
# sfn_debug.sh — Nova Cat Step Functions debugger
#
# Usage: ./sfn_debug.sh

set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
LOG_LOOKBACK_SECONDS=3600  # 1 hour

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

hr() { print -P "%F{cyan}────────────────────────────────────────────────────────%f"; }

pick_from_list() {
    local prompt="$1"
    shift
    local items=("$@")
    print -P "%B${prompt}%b" >&2
    local i=1
    for item in "${items[@]}"; do
        print -P "  %F{cyan}${i}%f) ${item}" >&2
        i=$(( i + 1 ))
    done
    local choice
    while true; do
        read -r "choice?Enter number: "
        if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#items} )); then
            echo "${items[$choice]}"
            return
        fi
        echo "Invalid choice — enter a number between 1 and ${#items}" >&2
    done
}

# ---------------------------------------------------------------------------
# Step 1 — Pick a state machine
# ---------------------------------------------------------------------------
hr
print -P "%BNova Cat Step Functions Debugger%b"
hr

print -P "\n%BFetching Nova Cat state machines...%b"

SM_ARNS=("${(@f)$(aws stepfunctions list-state-machines \
    --region "$REGION" \
    --query "stateMachines[?contains(name, 'nova-cat')].stateMachineArn" \
    --output text | tr '\t' '\n' | sort)}")

if [[ ${#SM_ARNS} -eq 0 ]]; then
    print -P "%F{red}No Nova Cat state machines found in $REGION.%f"
    exit 1
fi

SM_NAMES=("${SM_ARNS[@]##*:}")

CHOSEN_NAME=$(pick_from_list "Select a state machine:" "${SM_NAMES[@]}")
CHOSEN_ARN=""
for arn in "${SM_ARNS[@]}"; do
    if [[ "$arn" == *"$CHOSEN_NAME" ]]; then
        CHOSEN_ARN="$arn"
        break
    fi
done

print -P "\n%F{green}Selected:%f $CHOSEN_NAME"

# ---------------------------------------------------------------------------
# Step 2 — Pick an execution
# ---------------------------------------------------------------------------
print -P "\n%BFetching 10 most recent executions...%b"

EXEC_JSON=$(aws stepfunctions list-executions \
    --state-machine-arn "$CHOSEN_ARN" \
    --region "$REGION" \
    --max-results 10 \
    --query 'executions[*].{arn:executionArn,name:name,status:status,start:startDate}' \
    --output json)

EXEC_LABELS=("${(@f)$(echo "$EXEC_JSON" | python3 -c "
import json, sys
execs = json.load(sys.stdin)
for e in execs:
    status = e['status']
    icon = 'OK' if status == 'SUCCEEDED' else ('XX' if status == 'FAILED' else '..')
    print(f\"{icon} {status:<12} {e['start'][:19]}  {e['name']}\")
")}")

EXEC_ARNS=("${(@f)$(echo "$EXEC_JSON" | python3 -c "
import json, sys
for e in json.load(sys.stdin):
    print(e['arn'])
")}")

CHOSEN_LABEL=$(pick_from_list "Select an execution:" "${EXEC_LABELS[@]}")

CHOSEN_EXEC_ARN=""
for i in {1..${#EXEC_LABELS}}; do
    if [[ "${EXEC_LABELS[$i]}" == "$CHOSEN_LABEL" ]]; then
        CHOSEN_EXEC_ARN="${EXEC_ARNS[$i]}"
        break
    fi
done

print -P "\n%F{green}Selected execution:%f $CHOSEN_EXEC_ARN"

# ---------------------------------------------------------------------------
# Step 3 — Fetch execution details
# ---------------------------------------------------------------------------
EXEC_DETAIL=$(aws stepfunctions describe-execution \
    --execution-arn "$CHOSEN_EXEC_ARN" \
    --region "$REGION")

EXEC_STATUS=$(echo "$EXEC_DETAIL" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")
EXEC_START=$(echo "$EXEC_DETAIL" | python3 -c "import json,sys; print(json.load(sys.stdin)['startDate'])")
EXEC_OUTPUT=$(echo "$EXEC_DETAIL" | python3 -c "
import json,sys
d = json.load(sys.stdin)
out = d.get('output')
print(out if out else '(no output)')
")

EXEC_HISTORY=$(aws stepfunctions get-execution-history \
    --execution-arn "$CHOSEN_EXEC_ARN" \
    --region "$REGION" \
    --output json)

# ---------------------------------------------------------------------------
# Step 4 — Inspection menu
# ---------------------------------------------------------------------------
while true; do
    hr
    print -P "%BWhat would you like to inspect?%b"
    print -P "  %F{cyan}1%f) Summary"
    print -P "  %F{cyan}2%f) Execution output"
    print -P "  %F{cyan}3%f) Failed states only"
    print -P "  %F{cyan}4%f) Full event history"
    print -P "  %F{cyan}5%f) CloudWatch Lambda logs (1h lookback)"
    print -P "  %F{cyan}6%f) Exit"
    read -r "ACTION?Enter number: "

    case "$ACTION" in
    1)
        hr
        print -P "%BSummary%b"
        hr
        local status_color="%F{green}"
        [[ "$EXEC_STATUS" == "FAILED" || "$EXEC_STATUS" == "TIMED_OUT" || "$EXEC_STATUS" == "ABORTED" ]] && status_color="%F{red}"
        [[ "$EXEC_STATUS" == "RUNNING" ]] && status_color="%F{yellow}"
        print -P "  State Machine : $CHOSEN_NAME"
        print -P "  Execution     : ${CHOSEN_EXEC_ARN##*:}"
        print -P "  Status        : ${status_color}%B${EXEC_STATUS}%b%f"
        print -P "  Started       : $EXEC_START"

        if [[ "$EXEC_STATUS" == "FAILED" ]]; then
            ERROR_SUMMARY=$(echo "$EXEC_HISTORY" | python3 -c "
import json, sys
history = json.load(sys.stdin)['events']
failed = [e for e in history if e['type'] in ('ExecutionFailed', 'TaskFailed')]
if not failed:
    print('  No TaskFailed/ExecutionFailed events found.')
else:
    for e in failed:
        details = e.get('taskFailedEventDetails') or e.get('executionFailedEventDetails') or {}
        error = details.get('error', 'unknown')
        cause = details.get('cause', '')
        try:
            cause_obj = json.loads(cause)
            msg = cause_obj.get('errorMessage', cause)
        except Exception:
            msg = cause[:200]
        state = ''
        idx = history.index(e)
        for prev in reversed(history[:idx]):
            if prev['type'] == 'TaskStateEntered':
                state = prev.get('stateEnteredEventDetails', {}).get('name', '')
                break
        print(f'  State  : {state}')
        print(f'  Error  : {error}')
        print(f'  Cause  : {msg}')
        print()
" 2>/dev/null || echo "  (could not parse failure details)")
            print -P "\n%F{red}%BFailure Summary:%b%f"
            echo "$ERROR_SUMMARY"
        fi
        ;;

    2)
        hr
        print -P "%BExecution Output%b"
        hr
        echo "$EXEC_OUTPUT" | python3 -m json.tool 2>/dev/null || echo "$EXEC_OUTPUT"
        ;;

    3)
        hr
        print -P "%BFailed States%b"
        hr
        echo "$EXEC_HISTORY" | python3 -c "
import json, sys
history = json.load(sys.stdin)['events']
failed = [e for e in history if e['type'] in ('ExecutionFailed', 'TaskFailed', 'TaskStateAborted')]
if not failed:
    print('  No failed states found.')
else:
    for e in failed:
        ts = e['timestamp']
        details = e.get('taskFailedEventDetails') or e.get('executionFailedEventDetails') or {}
        error = details.get('error', 'unknown')
        cause = details.get('cause', '')
        try:
            cause_obj = json.loads(cause)
            msg = cause_obj.get('errorMessage', cause)
        except Exception:
            msg = cause
        print(f'[{ts}]')
        print(f'  Type  : {e[\"type\"]}')
        print(f'  Error : {error}')
        print(f'  Cause : {msg}')
        print()
"
        ;;

    4)
        hr
        print -P "%BFull Event History%b"
        hr
        echo "$EXEC_HISTORY" | python3 -c "
import json, sys
history = json.load(sys.stdin)['events']
for e in history:
    ts = e['timestamp']
    etype = e['type']
    detail = ''
    for key in ('stateEnteredEventDetails', 'stateExitedEventDetails',
                'taskScheduledEventDetails', 'taskSucceededEventDetails',
                'taskFailedEventDetails', 'executionFailedEventDetails',
                'executionSucceededEventDetails', 'lambdaFunctionFailedEventDetails'):
        if key in e:
            d = e[key]
            if 'name' in d:
                detail = d['name']
            elif 'error' in d:
                detail = f\"{d.get('error')}: {d.get('cause', '')[:80]}\"
            elif 'output' in d:
                detail = '(output present)'
            break
    print(f'  [{ts}] {etype:<45} {detail}')
"
        ;;

    5)
        hr
        print -P "%BCloudWatch Lambda Logs (last 1h)%b"
        hr
        print -P "%F{yellow}Fetching log groups for nova-cat Lambdas...%f"

        LOG_GROUPS=("${(@f)$(aws logs describe-log-groups \
            --log-group-name-prefix "/aws/lambda/nova-cat" \
            --region "$REGION" \
            --query 'logGroups[*].logGroupName' \
            --output text | tr '\t' '\n' | sort)}")

        if [[ ${#LOG_GROUPS} -eq 0 ]]; then
            echo "No Nova Cat Lambda log groups found."
        else
            CHOSEN_LOG_GROUP=$(pick_from_list "Select a log group:" "${LOG_GROUPS[@]}")
            NOW_MS=$(python3 -c "import time; print(int(time.time() * 1000))")
            START_MS=$(python3 -c "import time; print(int((time.time() - $LOG_LOOKBACK_SECONDS) * 1000))")

            print -P "\n%BLogs from $CHOSEN_LOG_GROUP (last 1h):%b"
            hr
            aws logs filter-log-events \
                --log-group-name "$CHOSEN_LOG_GROUP" \
                --start-time "$START_MS" \
                --end-time "$NOW_MS" \
                --region "$REGION" \
                --query 'events[*].message' \
                --output text | tr '\t' '\n' | while IFS= read -r line; do
                    if echo "$line" | grep -q '"level":"ERROR"'; then
                        print -P "%F{red}${line}%f"
                    elif echo "$line" | grep -q '"level":"WARNING"'; then
                        print -P "%F{yellow}${line}%f"
                    else
                        echo "$line"
                    fi
                done
        fi
        ;;

    6)
        print -P "\n%F{green}Goodbye!%f"
        exit 0
        ;;

    *)
        echo "Invalid choice."
        ;;
    esac
done
