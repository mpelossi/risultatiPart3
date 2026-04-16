#!/usr/bin/env bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_PATH="${BASH_SOURCE[0]}"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
EXPERIMENT_CONFIG="${CCA_EXPERIMENT_CONFIG:-$REPO_ROOT/part3/automation/experiment.yaml}"

IS_SOURCED=0
if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  IS_SOURCED=1
fi

BLOCKERS=()
WARNINGS=()
NEXT_STEPS=()

CAPTURED_STDOUT=""
CAPTURED_STDERR=""

CLUSTER_NAME=""
ZONE=""
KOPS_STATE_STORE=""
SSH_KEY_PATH=""
SSH_PUB_KEY_PATH=""
CLUSTER_CONFIG_PATH=""
EXPERIMENT_ID=""
SUBMISSION_GROUP=""
CONFIGURED_PROJECT=""

CURRENT_PROJECT=""
ACTIVE_ACCOUNT=""

GCLOUD_OK=0
KOPS_OK=0
KUBECTL_OK=0
PYTHON_OK=0

GCLOUD_AUTH_OK=0
ADC_OK=0
CLUSTER_VISIBLE=0
KUBECTL_ACCESS_OK=0
RESOURCE_WARNINGS=0

finish() {
  local code="$1"
  if [[ "$IS_SOURCED" -eq 1 ]]; then
    return "$code"
  fi
  exit "$code"
}

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
    if [[ "$existing" == "$step" ]]; then
      return
    fi
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

load_config() {
  section "Config"
  if [[ ! -f "$EXPERIMENT_CONFIG" ]]; then
    add_blocker "Experiment config not found: $EXPERIMENT_CONFIG"
    return 1
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    add_blocker "python3 is required to parse $EXPERIMENT_CONFIG"
    return 1
  fi
  PYTHON_OK=1

  local cfg
  if ! mapfile -t cfg < <(python3 - "$EXPERIMENT_CONFIG" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1]).resolve()
base = path.parent
text = path.read_text(encoding="utf-8")

try:
    import yaml  # type: ignore
    data = yaml.safe_load(text)
except ModuleNotFoundError:
    data = json.loads(text)

def resolve_path(raw: str) -> str:
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = base / p
    return str(p.resolve())

print(data["cluster_name"])
print(data["zone"])
print(data["kops_state_store"])
print(resolve_path(data["ssh_key_path"]))
print(resolve_path(data["cluster_config_path"]))
print(data["experiment_id"])
print(str(data.get("submission_group", "000")).zfill(3))
PY
  ); then
    add_blocker "Could not parse $EXPERIMENT_CONFIG"
    return 1
  fi

  CLUSTER_NAME="${cfg[0]}"
  ZONE="${cfg[1]}"
  KOPS_STATE_STORE="${cfg[2]}"
  SSH_KEY_PATH="${cfg[3]}"
  CLUSTER_CONFIG_PATH="${cfg[4]}"
  EXPERIMENT_ID="${cfg[5]}"
  SUBMISSION_GROUP="${cfg[6]}"

  if [[ ! -f "$CLUSTER_CONFIG_PATH" ]]; then
    add_blocker "Cluster config not found: $CLUSTER_CONFIG_PATH"
    return 1
  fi

  CONFIGURED_PROJECT="$(awk '/^[[:space:]]+project:[[:space:]]*/ { print $2; exit }' "$CLUSTER_CONFIG_PATH")"
  if [[ -z "$CONFIGURED_PROJECT" ]]; then
    add_blocker "Could not read spec.project from $CLUSTER_CONFIG_PATH"
    return 1
  fi

  if [[ "$SSH_KEY_PATH" == *.pub ]]; then
    SSH_PUB_KEY_PATH="$SSH_KEY_PATH"
    SSH_KEY_PATH="${SSH_KEY_PATH%.pub}"
  else
    SSH_PUB_KEY_PATH="${SSH_KEY_PATH}.pub"
  fi

  ok "Loaded experiment config: $EXPERIMENT_CONFIG"
  info "Experiment ID: $EXPERIMENT_ID"
  info "Cluster name: $CLUSTER_NAME"
  info "Project: $CONFIGURED_PROJECT"
  info "Zone: $ZONE"
  info "KOPS state store: $KOPS_STATE_STORE"
}

apply_session_env() {
  section "Session Environment"
  export KOPS_STATE_STORE
  export PROJECT="$CONFIGURED_PROJECT"
  ok "Prepared KOPS_STATE_STORE and PROJECT for this shell session"
  if [[ "$IS_SOURCED" -eq 1 ]]; then
    ok "Because the script was sourced, the exports persist in your current shell"
  else
    add_warning "Exports only live inside this process. Use 'source ./checkCredits.sh' at the start of a session."
  fi
}

check_local_tools() {
  section "Local Tools"
  require_command gcloud && GCLOUD_OK=1
  require_command kops && KOPS_OK=1
  require_command kubectl && KUBECTL_OK=1
  require_command python3 && PYTHON_OK=1
}

check_ssh_keys() {
  section "SSH Keys"
  if [[ -f "$SSH_KEY_PATH" ]]; then
    ok "SSH private key present: $SSH_KEY_PATH"
  else
    add_blocker "SSH private key missing: $SSH_KEY_PATH"
    add_next_step "ssh-keygen -t rsa -b 4096 -f \"$SSH_KEY_PATH\""
  fi

  if [[ -f "$SSH_PUB_KEY_PATH" ]]; then
    ok "SSH public key present: $SSH_PUB_KEY_PATH"
  else
    add_blocker "SSH public key missing: $SSH_PUB_KEY_PATH"
    add_next_step "ssh-keygen -y -f \"$SSH_KEY_PATH\" > \"$SSH_PUB_KEY_PATH\""
  fi
}

sync_gcloud_project() {
  section "gcloud Project"
  if [[ "$GCLOUD_OK" -ne 1 ]]; then
    return
  fi

  CURRENT_PROJECT="$(gcloud config get-value project 2>/dev/null | tr -d '\r')"
  if [[ -z "$CURRENT_PROJECT" || "$CURRENT_PROJECT" == "(unset)" ]]; then
    if capture_command gcloud config set project "$CONFIGURED_PROJECT"; then
      ok "Set gcloud active project to $CONFIGURED_PROJECT"
      CURRENT_PROJECT="$CONFIGURED_PROJECT"
    else
      add_blocker "Could not set gcloud project to $CONFIGURED_PROJECT"
      add_next_step "gcloud config set project $CONFIGURED_PROJECT"
      return
    fi
  elif [[ "$CURRENT_PROJECT" != "$CONFIGURED_PROJECT" ]]; then
    warn "gcloud project was $CURRENT_PROJECT; switching to $CONFIGURED_PROJECT"
    if capture_command gcloud config set project "$CONFIGURED_PROJECT"; then
      ok "gcloud active project now matches the Part 3 config"
      CURRENT_PROJECT="$CONFIGURED_PROJECT"
    else
      add_blocker "Could not switch gcloud project to $CONFIGURED_PROJECT"
      add_next_step "gcloud config set project $CONFIGURED_PROJECT"
      return
    fi
  else
    ok "gcloud active project already matches the Part 3 config"
  fi
}

check_gcloud_auth() {
  section "Credentials"
  if [[ "$GCLOUD_OK" -ne 1 ]]; then
    return
  fi

  ACTIVE_ACCOUNT="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null | head -n 1)"
  if [[ -z "$ACTIVE_ACCOUNT" ]]; then
    add_blocker "No active gcloud account configured"
    add_next_step "gcloud auth login"
  else
    ok "Active gcloud account: $ACTIVE_ACCOUNT"
  fi

  if capture_command gcloud auth print-access-token --quiet; then
    GCLOUD_AUTH_OK=1
    ok "gcloud user credentials are usable"
  else
    local reason="$CAPTURED_STDERR"
    if [[ "$reason" == *"invalid_rapt"* || "$reason" == *"invalid_grant"* || "$reason" == *"reauth related error"* ]]; then
      add_blocker "gcloud login looks expired or needs reauthentication"
    else
      add_blocker "gcloud user credentials are not usable"
    fi
    add_next_step "gcloud auth login"
    if [[ -n "$reason" ]]; then
      info "gcloud said:"
      while IFS= read -r line; do
        [[ -n "$line" ]] && echo "  $line"
      done <<< "$reason"
    fi
  fi

  if capture_command gcloud auth application-default print-access-token; then
    ADC_OK=1
    ok "Application default credentials are usable"
  else
    local reason="$CAPTURED_STDERR"
    if [[ "$reason" == *"invalid_rapt"* || "$reason" == *"invalid_grant"* || "$reason" == *"reauth related error"* ]]; then
      add_blocker "Application default credentials look expired or need reauthentication"
    else
      add_blocker "Application default credentials are not usable"
    fi
    add_next_step "gcloud auth application-default login"
    if [[ -n "$reason" ]]; then
      info "ADC said:"
      while IFS= read -r line; do
        [[ -n "$line" ]] && echo "  $line"
      done <<< "$reason"
    fi
  fi
}

check_billing_link() {
  section "Billing"
  if [[ "$GCLOUD_OK" -ne 1 || "$GCLOUD_AUTH_OK" -ne 1 ]]; then
    return
  fi

  if capture_command gcloud billing projects describe "$CONFIGURED_PROJECT" --format='value(billingEnabled)'; then
    local enabled
    enabled="$(echo "$CAPTURED_STDOUT" | tr -d '\r')"
    if [[ "$enabled" == "True" || "$enabled" == "true" ]]; then
      ok "Billing is enabled on $CONFIGURED_PROJECT"
    else
      add_warning "Billing is not enabled on $CONFIGURED_PROJECT"
    fi
    return
  fi

  if capture_command gcloud beta billing projects describe "$CONFIGURED_PROJECT" --format='value(billingEnabled)'; then
    local enabled_beta
    enabled_beta="$(echo "$CAPTURED_STDOUT" | tr -d '\r')"
    if [[ "$enabled_beta" == "True" || "$enabled_beta" == "true" ]]; then
      ok "Billing is enabled on $CONFIGURED_PROJECT"
    else
      add_warning "Billing is not enabled on $CONFIGURED_PROJECT"
    fi
  else
    add_warning "Could not verify billing linkage from the CLI. Remaining education credits still need to be checked in the GCP Billing report."
  fi
}

check_cluster_visibility() {
  section "kOps Cluster"
  if [[ "$KOPS_OK" -ne 1 || "$ADC_OK" -ne 1 ]]; then
    return
  fi

  if capture_command env KOPS_STATE_STORE="$KOPS_STATE_STORE" kops get cluster --name "$CLUSTER_NAME"; then
    CLUSTER_VISIBLE=1
    ok "Cluster config is visible in the kOps state store"
  else
    local reason="$CAPTURED_STDERR"
    if [[ "$reason" == *"not found"* || "$reason" == *"does not exist"* ]]; then
      add_warning "Cluster $CLUSTER_NAME is not in the state store yet"
      add_next_step "./checkCredits.sh cluster-up"
    else
      add_blocker "Could not query kOps for $CLUSTER_NAME"
      if [[ -n "$reason" ]]; then
        info "kOps said:"
        while IFS= read -r line; do
          [[ -n "$line" ]] && echo "  $line"
        done <<< "$reason"
      fi
    fi
  fi
}

refresh_kubeconfig() {
  section "Kubeconfig"
  if [[ "$KOPS_OK" -ne 1 || "$ADC_OK" -ne 1 || "$CLUSTER_VISIBLE" -ne 1 ]]; then
    return 1
  fi

  if capture_command env KOPS_STATE_STORE="$KOPS_STATE_STORE" kops export kubecfg --admin --name "$CLUSTER_NAME"; then
    ok "Refreshed local kubeconfig for $CLUSTER_NAME"
    return 0
  fi

  add_blocker "Could not export kubeconfig for $CLUSTER_NAME"
  add_next_step "kops export kubecfg --admin --name $CLUSTER_NAME"
  if [[ -n "$CAPTURED_STDERR" ]]; then
    info "kOps said:"
    while IFS= read -r line; do
      [[ -n "$line" ]] && echo "  $line"
    done <<< "$CAPTURED_STDERR"
  fi
  return 1
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
    KUBECTL_ACCESS_OK=1
    ok "kubectl can reach the cluster API"
  else
    local reason="$CAPTURED_STDERR"
    if [[ "$reason" == *"127.0.0.1:8080"* || "$reason" == *"localhost:8080"* ]]; then
      add_blocker "kubectl is pointing at localhost:8080, which usually means kubeconfig is missing or stale"
      add_next_step "./checkCredits.sh kubeconfig"
    elif [[ "$reason" == *"Unauthorized"* || "$reason" == *"You must be logged in to the server"* ]]; then
      add_blocker "kubectl credentials are not accepted by the cluster"
      add_next_step "./checkCredits.sh kubeconfig"
    else
      add_warning "kubectl could not reach the cluster API cleanly"
    fi

    if [[ -n "$reason" ]]; then
      info "kubectl said:"
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

  add_warning "The CLI can tell you what is still running, but not your exact remaining education credit balance. Check the GCP Billing report for that."
}

print_summary() {
  section "Summary"
  if [[ "${#BLOCKERS[@]}" -eq 0 ]]; then
    ok "No blocking setup issues detected"
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
  echo -e "${CYAN}   Part 3 Session Bootstrap and Credit Checker        ${NC}"
  echo -e "${CYAN}======================================================${NC}"

  load_config || return 1
  apply_session_env
  check_local_tools
  check_ssh_keys
  sync_gcloud_project
  check_gcloud_auth
  check_billing_link
  check_cluster_visibility
  if [[ "$CLUSTER_VISIBLE" -eq 1 ]]; then
    refresh_kubeconfig || true
  fi
  check_kubectl_access
  check_billable_resources
  print_summary

  if [[ "${#BLOCKERS[@]}" -gt 0 ]]; then
    return 1
  fi
  return 0
}

run_kubeconfig_refresh() {
  load_config || return 1
  apply_session_env
  check_local_tools
  sync_gcloud_project
  check_gcloud_auth
  check_cluster_visibility
  refresh_kubeconfig || return 1
  check_kubectl_access
  print_summary
  if [[ "${#BLOCKERS[@]}" -gt 0 ]]; then
    return 1
  fi
  return 0
}

print_help() {
  cat <<'EOF'
Usage:
  source ./checkCredits.sh
  ./checkCredits.sh doctor
  ./checkCredits.sh kubeconfig
  ./checkCredits.sh resources
  ./checkCredits.sh help

Recommended:
  1. Start every shell session with:
       source ./checkCredits.sh
  2. If kubeconfig is stale:
       ./checkCredits.sh kubeconfig

What the default doctor does:
  - loads part3/automation/experiment.yaml
  - exports KOPS_STATE_STORE and PROJECT
  - checks gcloud login and application-default login
  - refreshes kubeconfig when the cluster exists
  - checks kubectl access
  - lists active billable resources still consuming credits

Note:
  The GCP CLI does not expose your exact remaining education coupon balance cleanly.
  This script tells you what is still running and what credentials look expired.
EOF
}

run_resources_only() {
  echo -e "${CYAN}======================================================${NC}"
  echo -e "${CYAN}   Part 3 Billable Resource Check                     ${NC}"
  echo -e "${CYAN}======================================================${NC}"

  load_config || return 1
  apply_session_env
  check_local_tools
  sync_gcloud_project
  check_gcloud_auth
  check_billing_link
  check_billable_resources
  print_summary
  if [[ "${#BLOCKERS[@]}" -gt 0 ]]; then
    return 1
  fi
  return 0
}

COMMAND="${1:-doctor}"

case "$COMMAND" in
  doctor|setup)
    run_doctor
    finish $?
    ;;
  kubeconfig)
    run_kubeconfig_refresh
    finish $?
    ;;
  resources)
    run_resources_only
    finish $?
    ;;
  help|-h|--help)
    print_help
    finish 0
    ;;
  *)
    fail "Unknown command: $COMMAND"
    print_help
    finish 2
    ;;
esac
