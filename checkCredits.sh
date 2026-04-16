#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}======================================================${NC}"
echo -e "${CYAN}   GCP Resource & Credit Drain Checker (Read-Only)    ${NC}"
echo -e "${CYAN}======================================================${NC}"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}Error: 'gcloud' command not found. Please install the Google Cloud SDK.${NC}"
    exit 1
fi

# Get current project
PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT" ] || [ "$PROJECT" = "(unset)" ]; then
    echo -e "${RED}Error: No GCP project set. Run 'gcloud config set project <PROJECT_ID>'${NC}"
    exit 1
fi

ACTIVE_ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null)
if [ -z "$ACTIVE_ACCOUNT" ]; then
    echo -e "${RED}Error: No active gcloud account configured. Run 'gcloud auth login'.${NC}"
    exit 1
fi

AUTH_ERROR_FILE=$(mktemp)
if ! gcloud auth print-access-token --quiet >/dev/null 2>"$AUTH_ERROR_FILE"; then
    echo -e "${RED}Error: gcloud authentication/network check failed. Resource checks were not run.${NC}"
    echo -e "${CYAN}Active account:${NC} ${YELLOW}$ACTIVE_ACCOUNT${NC}"
    while IFS= read -r line; do
        [ -n "$line" ] && echo "   $line"
    done < "$AUTH_ERROR_FILE"
    rm -f "$AUTH_ERROR_FILE"
    exit 1
fi
rm -f "$AUTH_ERROR_FILE"

echo -e "Currently checking project: ${YELLOW}$PROJECT${NC}"
echo -e "Using gcloud account: ${YELLOW}$ACTIVE_ACCOUNT${NC}\n"

# Variables to track if we found any leaks or could not complete checks
LEAKS_FOUND=0
CHECK_ERRORS=0

# Function to check resources
check_resource() {
    local resource_name="$1"
    local gcloud_cmd="$2"
    local delete_hint="$3"
    local error_file
    local result
    local error_output
    local status

    error_file=$(mktemp)
    # We use format="value(name)" so it only prints names. If empty, it returns nothing.
    result=$(eval "$gcloud_cmd --quiet --format='value(name)'" 2>"$error_file")
    status=$?
    error_output=$(<"$error_file")
    rm -f "$error_file"
    
    printf "%-30s" "Checking $resource_name..."

    if [ $status -ne 0 ]; then
        echo -e "${RED}ERROR${NC}"
        echo -e "   ${RED}Could not verify $resource_name. Do not assume this project is clear.${NC}"
        while IFS= read -r line; do
            [ -n "$line" ] && echo "   $line"
        done <<< "$error_output"
        CHECK_ERRORS=1
        return
    fi

    if [ -z "$result" ]; then
        echo -e "${GREEN}CLEAR (0 found)${NC}"
    else
        local count
        count=$(printf '%s\n' "$result" | grep -c .)
        echo -e "${RED}ALERT! ($count found)${NC}"
        
        # Print the names of the offending resources
        while IFS= read -r line; do
            echo -e "   ↳ ${YELLOW}$line${NC}"
        done <<< "$result"
        
        echo -e "   ${CYAN}To delete run:${NC} $delete_hint <name>"
        echo ""
        LEAKS_FOUND=1
    fi
}

# 1. Check for Virtual Machines
check_resource "VM Instances" "gcloud compute instances list" "gcloud compute instances delete"

# 2. Check for Disks (Orphaned hard drives cost money!)
check_resource "Disks (Storage)" "gcloud compute disks list" "gcloud compute disks delete"

# 3. Check for Load Balancers (Forwarding Rules)
check_resource "Load Balancers" "gcloud compute forwarding-rules list" "gcloud compute forwarding-rules delete"

# 4. Check for Target Pools (Often tied to Load Balancers)
check_resource "Target Pools" "gcloud compute target-pools list" "gcloud compute target-pools delete"

# 5. Check for Reserved IP Addresses
check_resource "Static IP Addresses" "gcloud compute addresses list" "gcloud compute addresses delete"

# 6. Check for Instance Groups (Kops uses these)
check_resource "Instance Groups" "gcloud compute instance-groups list" "gcloud compute instance-groups managed delete"

echo -e "\n${CYAN}======================================================${NC}"
FINAL_EXIT_CODE=0
if [ $CHECK_ERRORS -ne 0 ]; then
    echo -e "${RED}❌ INCOMPLETE! One or more gcloud checks failed.${NC}"
    echo -e "${RED}   This script cannot confirm that your project is safe. Fix the errors above and re-run it.${NC}"
    FINAL_EXIT_CODE=1
elif [ $LEAKS_FOUND -eq 0 ]; then
    echo -e "${GREEN}✅ ALL CLEAR! No expensive compute or network resources are running.${NC}"
    echo -e "${GREEN}   You are safe to close your laptop. Sleep well! 😴${NC}"
else
    echo -e "${RED}❌ WARNING! Active resources found!${NC}"
    echo -e "${RED}   These are currently eating your $50 budget. Please delete them using the commands provided above or via the GCP Console!${NC}"
    FINAL_EXIT_CODE=2
fi
echo -e "${CYAN}======================================================${NC}"
exit "$FINAL_EXIT_CODE"
