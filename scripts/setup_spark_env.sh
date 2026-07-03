#!/usr/bin/env bash

# ==============================================================================
# UNI_CREDIT COMPLIANCE TRAINING: SERVERLESS SPARK ENVIRONMENT SETUP (STEP 2)
# This script creates a BigQuery Spark Connection and configures its IAM roles.
# ==============================================================================

set -euo pipefail

# ANSI Color Codes for output formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}====================================================================${NC}"
echo -e "${BLUE}       UniCredit CZ Compliance Training: Spark Environment Setup     ${NC}"
echo -e "${BLUE}====================================================================${NC}\n"

# Check if bq CLI is installed
if ! command -v bq &> /dev/null; then
    echo -e "${RED}Error: 'bq' command-line tool is not installed.${NC}"
    exit 1
fi

# 1. GET GCP PROJECT ID
PROJECT_ID=""
if [ $# -ge 1 ]; then
    PROJECT_ID="$1"
else
    CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
    if [ -n "$CURRENT_PROJECT" ]; then
        read -rp "Enter GCP Project ID [Default: $CURRENT_PROJECT]: " PROJECT_ID
        PROJECT_ID="${PROJECT_ID:-$CURRENT_PROJECT}"
    else
        read -rp "Enter GCP Project ID: " PROJECT_ID
    fi
fi

if [ -z "$PROJECT_ID" ]; then
    echo -e "${RED}Error: GCP Project ID is required.${NC}"
    exit 1
fi

# 2. GET REGION AND CONNECTION ID
CONNECTION_ID="spark-connection"
REGION="us-central1"
if [ $# -ge 2 ]; then
    REGION="$2"
else
    read -rp "Enter GCP Region/Location for Connection [Default: $REGION]: " USER_REGION
    REGION="${USER_REGION:-$REGION}"
fi

echo -e "\n${YELLOW}=== Configuration ===${NC}"
echo -e "GCP Project:   ${GREEN}$PROJECT_ID${NC}"
echo -e "Location:      ${GREEN}$REGION${NC}"
echo -e "Connection ID: ${GREEN}$CONNECTION_ID${NC}\n"

if [ -t 0 ]; then
    read -rp "Proceed with environment setup? (y/N): " CONFIRM
    if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Setup cancelled by user.${NC}"
        exit 0
    fi
else
    echo -e "${YELLOW}Non-interactive shell detected. Proceeding automatically...${NC}"
fi

# 3. ENABLE REQUIRED GCP APIS
echo -e "\n${YELLOW}[1/5] Enabling required APIs (BigQuery Connection & Dataproc)...${NC}"
gcloud services enable bigqueryconnection.googleapis.com dataproc.googleapis.com --project="$PROJECT_ID"
echo -e "${GREEN}✓ Required APIs successfully enabled.${NC}"

# 4. CREATE SPARK CONNECTION
echo -e "\n${YELLOW}[2/5] Creating BigQuery Spark Connection...${NC}"

# Check if connection already exists
if bq --project_id="$PROJECT_ID" show --connection --location="$REGION" "$CONNECTION_ID" &>/dev/null; then
    echo -e "Spark Connection '${GREEN}$CONNECTION_ID${NC}' already exists. Skipping creation."
else
    bq --project_id="$PROJECT_ID" --location="$REGION" mk \
       --connection \
       --connection_type="SPARK" \
       "$CONNECTION_ID"
    echo -e "${GREEN}✓ Spark Connection '$CONNECTION_ID' successfully created.${NC}"
fi

# 5. RETRIEVE CONNECTION SERVICE ACCOUNT
echo -e "\n${YELLOW}[3/5] Retrieving Connection Service Account...${NC}"

# Safely query connection JSON and parse with python3 (portable and robust)
CONNECTION_JSON=$(bq --project_id="$PROJECT_ID" show --connection --location="$REGION" --format=json "$CONNECTION_ID")

SA_EMAIL=$(python3 -c "
import sys, json
try:
    data = json.loads(sys.argv[1])
    sa = data.get('spark', {}).get('serviceAccountId')
    if not sa:
        # Fallback to general serviceAccountId if key structure differs
        sa = data.get('serviceAccountId')
    print(sa if sa else '')
except Exception as e:
    print('', file=sys.stderr)
" "$CONNECTION_JSON")

if [ -z "$SA_EMAIL" ]; then
    echo -e "${RED}Error: Failed to retrieve Service Account for Connection '$CONNECTION_ID'.${NC}"
    echo "Please check connection details manually."
    exit 1
fi

echo -e "Retrieved Connection Service Account: ${GREEN}$SA_EMAIL${NC}"

# 6. CREATE STAGING GCS BUCKET
echo -e "\n${YELLOW}[4/5] Creating Cloud Storage staging bucket...${NC}"
STAGING_BUCKET="compliance-training-staging-$PROJECT_ID"
if gcloud storage buckets describe "gs://$STAGING_BUCKET" --project="$PROJECT_ID" &>/dev/null; then
    echo -e "Storage bucket '${GREEN}gs://$STAGING_BUCKET${NC}' already exists. Skipping."
else
    gcloud storage buckets create "gs://$STAGING_BUCKET" --location="$REGION" --project="$PROJECT_ID"
    echo -e "${GREEN}✓ Staging bucket 'gs://$STAGING_BUCKET' successfully created in location '$REGION'.${NC}"
fi

# 7. GRANT IAM ROLES
echo -e "\n${YELLOW}[5/5] Granting required IAM permissions to Service Account...${NC}"

# 7.1 Dataproc Worker
echo -e "Granting ${BLUE}roles/dataproc.worker${NC} (Dataproc Serverless Execution)..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/dataproc.worker" \
    --condition=None \
    --no-user-output-enabled

# 7.2 BigQuery Admin
echo -e "Granting ${BLUE}roles/bigquery.admin${NC} (BigQuery Reading and Writing)..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.admin" \
    --condition=None \
    --no-user-output-enabled

# 7.3 Storage Object Admin on the new staging bucket
echo -e "Granting ${BLUE}roles/storage.objectAdmin${NC} on GCS staging bucket..."
gcloud storage buckets add-iam-policy-binding "gs://$STAGING_BUCKET" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.objectAdmin" \
    --no-user-output-enabled

echo -e "${GREEN}✓ All IAM roles successfully granted to the connection service account.${NC}"

# 8. SUMMARY
echo -e "\n${BLUE}====================================================================${NC}"
echo -e "🎉 ${GREEN}Dataproc Serverless / Spark Environment Setup Completed!${NC}"
echo -e "${BLUE}====================================================================${NC}"
echo -e "Please share these details with trainees for validation:"
echo -e "  Service Account Email:  ${GREEN}$SA_EMAIL${NC}"
echo -e "  BigQuery Connection URI: ${GREEN}$PROJECT_ID.$REGION.$CONNECTION_ID${NC}"
echo -e "  Dataset Location:       ${GREEN}$REGION${NC}"
echo -e "  Staging GCS Bucket:     ${GREEN}gs://$STAGING_BUCKET${NC}"
echo -e "${BLUE}====================================================================${NC}"
