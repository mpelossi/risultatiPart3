#!/usr/bin/env bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
AUTOMATION_DIR="${CCA_AUTOMATION_DIR:-$REPO_ROOT/Matte/automation}"
CLI_MODULE="${CCA_CLI_MODULE:-Matte.automation.cli}"
CLI_PATH="$AUTOMATION_DIR/cli.py"
DEFAULT_CONFIG_PATH="$AUTOMATION_DIR/experiment.yaml"
DEFAULT_POLICY_PATH="$AUTOMATION_DIR/schedule.yaml"

BLOCKERS=()
WARNINGS=()
NEXT_STEPS=()

CAPTURED_STDOUT=""
CAPTURED_STDERR=""

GCLOUD_OK=0
KOPS_OK=0
KUBECTL_OK=0
PYTHON_OK=0
GCLOUD_AUTH_OK=0
ADC_OK=0
RESOURCE_WARNINGS=0

section() {
  echo -e "\n${CYAN}== $1 ==${NC}"
}

ok() {
  echo -e "${GREEN}[OK]${NC} $1"
}

warn() {
  echo -e "${YELLOW}[WARN]${NC} $1"
}

fail() {
  echo -e "${RED}[FAIL]${NC} $1"
}

info() {
  echo -e "${CYAN}[INFO]${NC} $1"
}

add_blocker() {
  BLOCKERS+=("$1")
  fail "$1"
}

add_warning() {
  WARNINGS+=("$1")
  warn "$1"
}

add_next_step() {
  local step="$1"
  local existing
  for existing in "${NEXT_STEPS[@]}"; do
    [[ "$existing" == "$step" ]] && return
  done
  NEXT_STEPS+=("$step")
}

capture_command() {
  local stdout_file
  local stderr_file
  stdout_file="$(mktemp)"
  stderr_file="$(mktemp)"
  "$@" >"$stdout_file" 2>"$stderr_file"
  local status=$?
  CAPTURED_STDOUT="$(cat "$stdout_file")"
  CAPTURED_STDERR="$(cat "$stderr_file")"
  rm -f "$stdout_file" "$stderr_file"
  return "$status"
}

require_command() {
  local cmd="$1"
  if command -v "$cmd" >/dev/null 2>&1; then
    ok "Found command: $cmd"
    return 0
  fi
  add_blocker "Missing required command: $cmd"
  return 1
}

check_local_tools() {
  section "Local Tools"
  require_command python3 && PYTHON_OK=1
  require_command gcloud && GCLOUD_OK=1
  require_command kops && KOPS_OK=1
  require_command kubectl && KUBECTL_OK=1
}

check_automation_files() {
  section "Automation Files"

  if [[ ! -d "$AUTOMATION_DIR" ]]; then
    add_blocker "Automation directory not found: $AUTOMATION_DIR"
    return
  fi
  ok "Automation directory present: $AUTOMATION_DIR"

  if [[ ! -f "$CLI_PATH" ]]; then
    add_blocker "CLI entrypoint not found: $CLI_PATH"
  else
    ok "CLI entrypoint present: $CLI_PATH"
  fi

  if [[ ! -f "$DEFAULT_CONFIG_PATH" ]]; then
    add_warning "Default config file missing: $DEFAULT_CONFIG_PATH"
  else
    ok "Default config file present: $DEFAULT_CONFIG_PATH"
  fi

  if [[ ! -f "$DEFAULT_POLICY_PATH" ]]; then
    add_warning "Default policy file missing: $DEFAULT_POLICY_PATH"
  else
    ok "Default policy file present: $DEFAULT_POLICY_PATH"
  fi
}

check_cli_help() {
  section "CLI Module"
  if [[ "$PYTHON_OK" -ne 1 ]]; then
    return
  fi

  if capture_command bash -lc "cd \"$REPO_ROOT\" && python3 -m $CLI_MODULE --help"; then
    ok "python3 -m $CLI_MODULE --help works"
  else
    add_blocker "The automation CLI module is not runnable"
    if [[ -n "$CAPTURED_STDERR" ]]; then
      while IFS= read -r line; do
        [[ -n "$line" ]] && echo "  $line"
      done <<< "$CAPTURED_STDERR"
    fi
  fi
}

check_default_config_parse() {
  section "Config Parse"
  if [[ "$PYTHON_OK" -ne 1 || ! -f "$DEFAULT_CONFIG_PATH" ]]; then
    return
  fi

  if capture_command bash -lc "cd \"$REPO_ROOT\" && python3 - <<'PY'
from Matte.automation.config import load_experiment_config
load_experiment_config('Matte/automation/experiment.yaml')
print('ok')
PY"; then
    ok "Default experiment config parses successfully"
  else
    add_blocker "Default experiment config is not parseable by the automation"
    if [[ -n "$CAPTURED_STDERR" ]]; then
      while IFS= read -r line; do
        [[ -n "$line" ]] && echo "  $line"
      done <<< "$CAPTURED_STDERR"
    fi
  fi

  if [[ ! -f "$DEFAULT_POLICY_PATH" ]]; then
    return
  fi

  if capture_command bash -lc "cd \"$REPO_ROOT\" && python3 - <<'PY'
from Matte.automation.config import load_policy_config
load_policy_config('Matte/automation/schedule.yaml')
print('ok')
PY"; then
    ok "Default policy parses successfully"
  else
    add_blocker "Default schedule/policy is not parseable by the automation"
    if [[ -n "$CAPTURED_STDERR" ]]; then
      while IFS= read -r line; do
        [[ -n "$line" ]] && echo "  $line"
      done <<< "$CAPTURED_STDERR"
    fi
  fi
}

check_gcloud_auth() {
  section "Credentials"
  if [[ "$GCLOUD_OK" -ne 1 ]]; then
    return
  fi

  local active_account
  active_account="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null | head -n 1)"
  if [[ -z "$active_account" ]]; then
    add_blocker "No active gcloud account configured"
    add_next_step "gcloud auth login"
  else
    ok "Active gcloud account: $active_account"
  fi

  if capture_command gcloud auth print-access-token --quiet; then
    GCLOUD_AUTH_OK=1
    ok "gcloud user credentials are usable"
  else
    add_blocker "gcloud user credentials are not usable for live cluster commands"
    add_next_step "gcloud auth login"
    if [[ -n "$CAPTURED_STDERR" ]]; then
      while IFS= read -r line; do
        [[ -n "$line" ]] && echo "  $line"
      done <<< "$CAPTURED_STDERR"
    fi
  fi

  if capture_command gcloud auth application-default print-access-token; then
    ADC_OK=1
    ok "Application default credentials are usable"
  else
    add_blocker "Application default credentials are not usable for kops/gcloud automation"
    add_next_step "gcloud auth application-default login"
    if [[ -n "$CAPTURED_STDERR" ]]; then
      while IFS= read -r line; do
        [[ -n "$line" ]] && echo "  $line"
      done <<< "$CAPTURED_STDERR"
    fi
  fi
}

check_kubectl_access() {
  section "kubectl Access"
  if [[ "$KUBECTL_OK" -ne 1 ]]; then
    return
  fi

  local current_context
  current_context="$(kubectl config current-context 2>/dev/null | tr -d '\r')"
  if [[ -n "$current_context" ]]; then
    ok "kubectl current context: $current_context"
  else
    add_warning "kubectl has no current context configured"
  fi

  if capture_command kubectl get nodes -o wide --request-timeout=10s; then
    ok "kubectl can reach a cluster API"
  else
    local reason="$CAPTURED_STDERR"
    if [[ "$reason" == *"127.0.0.1:8080"* || "$reason" == *"localhost:8080"* ]]; then
      add_warning "kubectl is currently pointing at localhost:8080, so live cluster commands will fail until kubeconfig is exported"
      add_next_step "kops export kubecfg --admin --name <cluster-name>"
    elif [[ "$reason" == *"Unauthorized"* || "$reason" == *"You must be logged in to the server"* ]]; then
      add_warning "kubectl has a context, but its credentials are not accepted by the cluster"
      add_next_step "kops export kubecfg --admin --name <cluster-name>"
    else
      add_warning "kubectl could not reach the cluster API cleanly"
    fi

    if [[ -n "$reason" ]]; then
      while IFS= read -r line; do
        [[ -n "$line" ]] && echo "  $line"
      done <<< "$reason"
    fi
  fi
}

check_resource() {
  local resource_name="$1"
  local delete_hint="$2"
  shift 2

  if capture_command "$@" --quiet --format='value(name)'; then
    local result="$CAPTURED_STDOUT"
    local count
    count=$(printf '%s\n' "$result" | grep -c . || true)
    if [[ "$count" -eq 0 ]]; then
      ok "$resource_name clear (0 found)"
    else
      RESOURCE_WARNINGS=1
      warn "$resource_name has $count active resource(s)"
      while IFS= read -r line; do
        [[ -n "$line" ]] && echo "  - $line"
      done <<< "$result"
      echo "  Delete with: $delete_hint"
    fi
    return
  fi

  add_warning "Could not verify $resource_name"
  if [[ -n "$CAPTURED_STDERR" ]]; then
    while IFS= read -r line; do
      [[ -n "$line" ]] && echo "  $line"
    done <<< "$CAPTURED_STDERR"
  fi
}

check_billable_resources() {
  section "Billable Resources"
  if [[ "$GCLOUD_OK" -ne 1 || "$GCLOUD_AUTH_OK" -ne 1 ]]; then
    return
  fi

  check_resource "VM instances" \
    "gcloud compute instances delete <name> --zone <zone>" \
    gcloud compute instances list

  check_resource "Disks" \
    "gcloud compute disks delete <name> --zone <zone>" \
    gcloud compute disks list

  check_resource "Forwarding rules" \
    "gcloud compute forwarding-rules delete <name> --region <region>" \
    gcloud compute forwarding-rules list

  check_resource "Target pools" \
    "gcloud compute target-pools delete <name> --region <region>" \
    gcloud compute target-pools list

  check_resource "Static IP addresses" \
    "gcloud compute addresses delete <name> --region <region>" \
    gcloud compute addresses list

  check_resource "Instance groups" \
    "gcloud compute instance-groups managed delete <name> --zone <zone>" \
    gcloud compute instance-groups list

  add_warning "The CLI can tell you what is still running, but not the exact remaining education coupon balance. Check the GCP Billing report for that."
}

print_summary() {
  section "Summary"
  if [[ "${#BLOCKERS[@]}" -eq 0 ]]; then
    ok "No blocking preflight issues detected"
  else
    fail "Blocking issues detected:"
    local item
    for item in "${BLOCKERS[@]}"; do
      echo "  - $item"
    done
  fi

  if [[ "${#WARNINGS[@]}" -gt 0 ]]; then
    warn "Warnings:"
    local item
    for item in "${WARNINGS[@]}"; do
      echo "  - $item"
    done
  fi

  if [[ "${#NEXT_STEPS[@]}" -gt 0 ]]; then
    info "Suggested next commands:"
    local step
    for step in "${NEXT_STEPS[@]}"; do
      echo "  $step"
    done
  fi

  if [[ "$RESOURCE_WARNINGS" -eq 1 ]]; then
    warn "Active billable resources were found. That is normal if your cluster is intentionally up, but they are still costing credits."
  fi
}

run_doctor() {
  echo -e "${CYAN}======================================================${NC}"
  echo -e "${CYAN}   risultatiPart3 Automation Preflight                ${NC}"
  echo -e "${CYAN}======================================================${NC}"

  check_local_tools
  check_automation_files
  check_cli_help
  check_default_config_parse
  check_gcloud_auth
  check_kubectl_access
  check_billable_resources
  print_summary

  if [[ "${#BLOCKERS[@]}" -gt 0 ]]; then
    return 1
  fi
  return 0
}

run_resources_only() {
  echo -e "${CYAN}======================================================${NC}"
  echo -e "${CYAN}   risultatiPart3 Billable Resource Check             ${NC}"
  echo -e "${CYAN}======================================================${NC}"

  check_local_tools
  check_gcloud_auth
  check_billable_resources
  print_summary

  if [[ "${#BLOCKERS[@]}" -gt 0 ]]; then
    return 1
  fi
  return 0
}

print_help() {
  cat <<EOF
Usage:
  ./checkCredits.sh doctor
  ./checkCredits.sh resources
  ./checkCredits.sh help

What this script does:
  - checks that the risultatiPart3 automation files exist
  - checks that "python3 -m $CLI_MODULE --help" works
  - checks that the default config and schedule parse cleanly
  - checks gcloud user auth and application-default auth
  - checks whether kubectl currently has usable cluster access
  - lists active billable GCP resources still consuming credits

What this script does not do:
  - it does not create, update, replace, validate, or delete any cluster
  - it does not call "cluster up"
  - it does not export kubeconfig for you
EOF
}

COMMAND="${1:-doctor}"

case "$COMMAND" in
  doctor|setup)
    run_doctor
    exit $?
    ;;
  resources)
    run_resources_only
    exit $?
    ;;
  help|-h|--help)
    print_help
    exit 0
    ;;
  *)
    fail "Unknown command: $COMMAND"
    print_help
    exit 2
    ;;
esac
