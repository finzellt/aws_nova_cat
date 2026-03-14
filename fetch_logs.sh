#!/usr/bin/env zsh
# sfn_debug.sh — Nova Cat Step Functions debugger
#
# Usage: ./sfn_debug.sh

set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
LOG_LOOKBACK_SECONDS=3600  # 1 hour
MAX_EXPRESS_EVENTS=200

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

json_get() {
    local expr="$1"
    python3 -c "import json,sys; data=json.load(sys.stdin); print($expr)"
}

now_ms() {
    python3 -c "import time; print(int(time.time() * 1000))"
}

lookback_ms() {
    local seconds="$1"
    python3 -c "import time; print(int((time.time() - ${seconds}) * 1000))"
}

extract_log_group_name_from_arn() {
    python3 -c '
import sys
arn = sys.stdin.read().strip()
# arn:aws:logs:region:acct:log-group:GROUPNAME:*
parts = arn.split(":log-group:", 1)
if len(parts) == 2:
    group = parts[1]
    group = group.split(":*", 1)[0]
    print(group)
'
}

# For EXPRESS state machines, we try to discover the configured log group.
get_sfn_log_group_for_state_machine() {
    local describe_json="$1"
    local log_group=""

    log_group=$(echo "$describe_json" | python3 -c '
import json, sys
d = json.load(sys.stdin)
cfg = d.get("loggingConfiguration") or {}
for dest in cfg.get("destinations", []):
    cw = dest.get("cloudWatchLogsLogGroup") or {}
    arn = cw.get("logGroupArn")
    if arn:
        parts = arn.split(":log-group:", 1)
        if len(parts) == 2:
            group = parts[1].split(":*", 1)[0]
            print(group)
            raise SystemExit
' 2>/dev/null || true)

    if [[ -n "$log_group" ]]; then
        echo "$log_group"
        return
    fi

    # Fallback guess
    echo "/aws/vendedlogs/states/${CHOSEN_NAME}"
}

start_logs_query() {
    local log_group="$1"
    local start_s="$2"
    local end_s="$3"
    local query_string="$4"

    aws logs start-query \
        --region "$REGION" \
        --log-group-name "$log_group" \
        --start-time "$start_s" \
        --end-time "$end_s" \
        --query-string "$query_string" \
        --output json
}

wait_for_query_results() {
    local query_id="$1"
    while true; do
        local out
        out=$(aws logs get-query-results --region "$REGION" --query-id "$query_id" --output json)
        local status
        status=$(echo "$out" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
        case "$status" in
            Complete)
                echo "$out"
                return
                ;;
            Failed|Cancelled|Timeout|Unknown)
                echo "$out"
                return 1
                ;;
            Scheduled|Running)
                sleep 1
                ;;
            *)
                sleep 1
                ;;
        esac
    done
}

# Build a recent execution list heuristically from CloudWatch logs for EXPRESS.
# We search raw messages for execution ARN + status-ish content.
build_express_execution_index() {
    local log_group="$1"
    local output_file="$2"

    local start_s end_s
    start_s=$(python3 -c "import time; print(int(time.time()) - ${LOG_LOOKBACK_SECONDS})")
    end_s=$(python3 -c "import time; print(int(time.time()))")

    local query_json query_id results_json
    query_json=$(start_logs_query "$log_group" "$start_s" "$end_s" \
'fields @timestamp, @message
| sort @timestamp desc
| limit 1000') || return 1

    query_id=$(echo "$query_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["queryId"])')
    results_json=$(wait_for_query_results "$query_id") || true

    echo "$results_json" | python3 - <<'PY' > "$output_file"
import json, sys, re
from collections import OrderedDict

data = json.load(sys.stdin)
rows = data.get("results", [])

def row_to_dict(row):
    d = {}
    for item in row:
        d[item["field"]] = item["value"]
    return d

arn_pat = re.compile(r'arn:aws:states:[^:\s]+:\d+:execution:[^"\s,}]+')
status_words = ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED", "RUNNING")

seen = OrderedDict()

for row in rows:
    r = row_to_dict(row)
    msg = r.get("@message", "")
    ts = r.get("@timestamp", "")

    arn_match = arn_pat.search(msg)
    if not arn_match:
        continue

    arn = arn_match.group(0)
    name = arn.rsplit(":", 1)[-1]

    status = "UNKNOWN"
    for s in status_words:
        if s in msg:
            status = s
            break

    label = f"{status:<12} {ts[:19]}  {name}"
    if arn not in seen:
        seen[arn] = {
            "arn": arn,
            "name": name,
            "status": status,
            "start": ts,
            "label": label,
        }

items = list(seen.values())[:10]
print(json.dumps(items))
PY
}

fetch_express_execution_events() {
    local log_group="$1"
    local exec_arn="$2"
    local output_file="$3"

    local start_ms end_ms
    start_ms=$(lookback_ms "$LOG_LOOKBACK_SECONDS")
    end_ms=$(now_ms)

    aws logs filter-log-events \
        --region "$REGION" \
        --log-group-name "$log_group" \
        --start-time "$start_ms" \
        --end-time "$end_ms" \
        --filter-pattern "\"$exec_arn\"" \
        --output json > "$output_file"
}

show_express_raw_events() {
    local events_file="$1"
    python3 - <<'PY' < "$events_file"
import json, sys

data = json.load(sys.stdin)
events = data.get("events", [])
if not events:
    print("  No matching log events found.")
    raise SystemExit

for e in events:
    ts = e.get("timestamp")
    msg = e.get("message", "").rstrip()
    print(f"[{ts}] {msg}")
    print()
PY
}

show_express_failure_summary() {
    local events_file="$1"
    python3 - <<'PY' < "$events_file"
import json, sys

data = json.load(sys.stdin)
events = data.get("events", [])
hits = []

for e in events:
    msg = e.get("message", "")
    if any(word in msg for word in ("FAILED", "TIMED_OUT", "ABORTED", '"error"', '"Error"', '"cause"', '"Cause"')):
        hits.append(e)

if not hits:
    print("  No obvious failure-related log lines found.")
    raise SystemExit

for e in hits:
    print(f"[{e.get('timestamp')}]")
    print(e.get("message", "").rstrip())
    print()
PY
}

show_express_summary() {
    local exec_arn="$1"
    local events_file="$2"

    python3 - "$exec_arn" <<'PY' < "$events_file"
import json, sys

exec_arn = sys.argv[1]
data = json.load(sys.stdin)
events = data.get("events", [])

status = "UNKNOWN"
started = ""
last_ts = ""

for e in events:
    msg = e.get("message", "")
    ts = str(e.get("timestamp", ""))
    if not started:
        started = ts
    last_ts = ts
    if "SUCCEEDED" in msg:
        status = "SUCCEEDED"
    elif "FAILED" in msg:
        status = "FAILED"
    elif "TIMED_OUT" in msg:
        status = "TIMED_OUT"
    elif "ABORTED" in msg:
        status = "ABORTED"
    elif "RUNNING" in msg and status == "UNKNOWN":
        status = "RUNNING"

print(f"  Execution     : {exec_arn.rsplit(':',1)[-1]}")
print(f"  Status        : {status}")
print(f"  First log ts  : {started or '(unknown)'}")
print(f"  Last log ts   : {last_ts or '(unknown)'}")
PY
}

# ---------------------------------------------------------------------------
# Outer loop — restart here when user selects "Start over"
# ---------------------------------------------------------------------------
while true; do

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

SM_DESCRIBE=$(aws stepfunctions describe-state-machine \
    --state-machine-arn "$CHOSEN_ARN" \
    --region "$REGION" \
    --output json)

SM_TYPE=$(echo "$SM_DESCRIBE" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("type","UNKNOWN"))')
print -P "%F{green}Type:%f $SM_TYPE"

CHOSEN_EXEC_ARN=""
EXEC_STATUS=""
EXEC_START=""
EXEC_OUTPUT=""
EXEC_HISTORY_FILE=""
EXPRESS_LOG_GROUP=""

# ---------------------------------------------------------------------------
# Step 2 — Pick an execution
# ---------------------------------------------------------------------------
if [[ "$SM_TYPE" == "STANDARD" ]]; then
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

    for i in {1..${#EXEC_LABELS}}; do
        if [[ "${EXEC_LABELS[$i]}" == "$CHOSEN_LABEL" ]]; then
            CHOSEN_EXEC_ARN="${EXEC_ARNS[$i]}"
            break
        fi
    done

    print -P "\n%F{green}Selected execution:%f $CHOSEN_EXEC_ARN"

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

    EXEC_HISTORY_FILE=$(mktemp /tmp/sfn_history_XXXXXX.json)
    trap "rm -f $EXEC_HISTORY_FILE" EXIT

    aws stepfunctions get-execution-history \
        --execution-arn "$CHOSEN_EXEC_ARN" \
        --region "$REGION" \
        --output json > "$EXEC_HISTORY_FILE"

elif [[ "$SM_TYPE" == "EXPRESS" ]]; then
    EXPRESS_LOG_GROUP=$(get_sfn_log_group_for_state_machine "$SM_DESCRIBE")
    print -P "\n%F{yellow}Using CloudWatch log group:%f $EXPRESS_LOG_GROUP"

    EXEC_INDEX_FILE=$(mktemp /tmp/sfn_express_exec_index_XXXXXX.json)
    trap "rm -f $EXEC_INDEX_FILE" EXIT

    build_express_execution_index "$EXPRESS_LOG_GROUP" "$EXEC_INDEX_FILE" || true

    EXEC_COUNT=$(python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' < "$EXEC_INDEX_FILE" 2>/dev/null || echo 0)

    if [[ "$EXEC_COUNT" -gt 0 ]]; then
        EXEC_LABELS=("${(@f)$(python3 -c '
import json, sys
for e in json.load(sys.stdin):
    print(e["label"])
' < "$EXEC_INDEX_FILE")}")

        EXEC_ARNS=("${(@f)$(python3 -c '
import json, sys
for e in json.load(sys.stdin):
    print(e["arn"])
' < "$EXEC_INDEX_FILE")}")

        CHOSEN_LABEL=$(pick_from_list "Select a recent execution (from logs):" "${EXEC_LABELS[@]}")

        for i in {1..${#EXEC_LABELS}}; do
            if [[ "${EXEC_LABELS[$i]}" == "$CHOSEN_LABEL" ]]; then
                CHOSEN_EXEC_ARN="${EXEC_ARNS[$i]}"
                break
            fi
        done
    else
        print -P "%F{yellow}Could not auto-discover recent executions from logs.%f"
        print -P "%F{yellow}You can still inspect raw Step Functions logs from the menu.%f"
        read -r "CHOSEN_EXEC_ARN?Enter an execution ARN to filter on (or leave blank): "
    fi

    EXEC_HISTORY_FILE=$(mktemp /tmp/sfn_express_events_XXXXXX.json)
    trap "rm -f $EXEC_HISTORY_FILE" EXIT

    if [[ -n "$CHOSEN_EXEC_ARN" ]]; then
        print -P "\n%F{green}Selected execution:%f $CHOSEN_EXEC_ARN"
        fetch_express_execution_events "$EXPRESS_LOG_GROUP" "$CHOSEN_EXEC_ARN" "$EXEC_HISTORY_FILE"
    else
        aws logs filter-log-events \
            --region "$REGION" \
            --log-group-name "$EXPRESS_LOG_GROUP" \
            --start-time "$(lookback_ms "$LOG_LOOKBACK_SECONDS")" \
            --end-time "$(now_ms)" \
            --limit "$MAX_EXPRESS_EVENTS" \
            --output json > "$EXEC_HISTORY_FILE"
    fi
else
    print -P "%F{red}Unsupported state machine type: $SM_TYPE%f"
    exit 1
fi

# ---------------------------------------------------------------------------
# Step 4 — Inspection menu
# ---------------------------------------------------------------------------
while true; do
    hr
    print -P "%BWhat would you like to inspect?%b"
    print -P "  %F{cyan}1%f) Summary"
    print -P "  %F{cyan}2%f) Execution output"
    print -P "  %F{cyan}3%f) Failed states / failure-related events"
    print -P "  %F{cyan}4%f) Full event history / raw Step Functions log events"
    print -P "  %F{cyan}5%f) CloudWatch Lambda logs (1h lookback)"
    print -P "  %F{cyan}6%f) Exit"
    print -P "  %F{cyan}7%f) Start over (pick a different state machine / execution)"
    read -r "ACTION?Enter number: "

    case "$ACTION" in
    1)
        hr
        print -P "%BSummary%b"
        hr
        print -P "  State Machine : $CHOSEN_NAME"
        print -P "  Type          : $SM_TYPE"

        if [[ "$SM_TYPE" == "STANDARD" ]]; then
            local status_color="%F{green}"
            [[ "$EXEC_STATUS" == "FAILED" || "$EXEC_STATUS" == "TIMED_OUT" || "$EXEC_STATUS" == "ABORTED" ]] && status_color="%F{red}"
            [[ "$EXEC_STATUS" == "RUNNING" ]] && status_color="%F{yellow}"
            print -P "  Execution     : ${CHOSEN_EXEC_ARN##*:}"
            print -P "  Status        : ${status_color}%B${EXEC_STATUS}%b%f"
            print -P "  Started       : $EXEC_START"

            if [[ "$EXEC_STATUS" == "FAILED" ]]; then
                ERROR_SUMMARY=$(python3 -c "
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
" 2>/dev/null < "$EXEC_HISTORY_FILE" || echo "  (could not parse failure details)")
                print -P "\n%F{red}%BFailure Summary:%b%f"
                echo "$ERROR_SUMMARY"
            fi
        else
            if [[ -n "$CHOSEN_EXEC_ARN" ]]; then
                show_express_summary "$CHOSEN_EXEC_ARN" "$EXEC_HISTORY_FILE"
            else
                print -P "  Execution     : (not selected)"
                print -P "  Log group     : $EXPRESS_LOG_GROUP"
                print -P "  Note          : EXPRESS uses CloudWatch logs instead of list-executions/get-execution-history"
            fi
        fi
        ;;

    2)
        hr
        print -P "%BExecution Output%b"
        hr
        if [[ "$SM_TYPE" == "STANDARD" ]]; then
            echo "$EXEC_OUTPUT" | python3 -m json.tool 2>/dev/null || echo "$EXEC_OUTPUT"
        else
            print -P "%F{yellow}EXPRESS output is not fetched via describe-execution in this script.%f"
            print -P "%F{yellow}Showing raw Step Functions log lines for the selected execution instead.%f"
            show_express_raw_events "$EXEC_HISTORY_FILE"
        fi
        ;;

    3)
        hr
        print -P "%BFailed States / Failure-Related Events%b"
        hr
        if [[ "$SM_TYPE" == "STANDARD" ]]; then
            python3 -c "
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
" < "$EXEC_HISTORY_FILE"
        else
            show_express_failure_summary "$EXEC_HISTORY_FILE"
        fi
        ;;

    4)
        hr
        if [[ "$SM_TYPE" == "STANDARD" ]]; then
            print -P "%BFull Event History%b"
            hr
            python3 -c "
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
" < "$EXEC_HISTORY_FILE"
        else
            print -P "%BRaw Step Functions Log Events%b"
            hr
            show_express_raw_events "$EXEC_HISTORY_FILE"
        fi
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
            NOW_MS=$(now_ms)
            START_MS=$(lookback_ms "$LOG_LOOKBACK_SECONDS")

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

    7)
        print -P "\n%F{yellow}Restarting...%f"
        [[ -n "${EXEC_HISTORY_FILE:-}" ]] && rm -f "$EXEC_HISTORY_FILE"
        break
        ;;

    *)
        echo "Invalid choice."
        ;;
    esac
done

done
