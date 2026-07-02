#!/usr/bin/env bash

# ==============================================================================
# UNI_CREDIT COMPLIANCE TRAINING: DATA INGESTION SCRIPT
# This script creates a BigQuery dataset and loads the dummy CSV datasets.
# ==============================================================================

set -euo pipefail

# ANSI Color Codes for output formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}====================================================================${NC}"
echo -e "${BLUE}       UniCredit CZ Compliance Training: BigQuery Data Loader        ${NC}"
echo -e "${BLUE}====================================================================${NC}\n"

# Check if bq CLI is installed
if ! command -v bq &> /dev/null; then
    echo -e "${RED}Error: 'bq' command-line tool is not installed.${NC}"
    echo "Please install the Google Cloud SDK first: https://cloud.google.com/sdk"
    exit 1
fi

# 1. GET GCP PROJECT ID
PROJECT_ID=""
if [ $# -ge 1 ]; then
    PROJECT_ID="$1"
else
    # Try to fetch current active gcloud project
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

# 2. GET DATASET NAME AND REGION
DATASET_NAME="compliance_training"
read -rp "Enter Target BigQuery Dataset Name [Default: $DATASET_NAME]: " USER_DATASET
DATASET_NAME="${USER_DATASET:-$DATASET_NAME}"

REGION="europe-west8"
read -rp "Enter GCP Region for the Dataset [Default: $REGION]: " USER_REGION
REGION="${USER_REGION:-$REGION}"

# 3. VERIFY DUMMY DATA FOLDER
DUMMY_DATA_DIR="./dummy_data"
if [ ! -d "$DUMMY_DATA_DIR" ]; then
    # Try to check in parent/child directories
    if [ -d "../dummy_data" ]; then
        DUMMY_DATA_DIR="../dummy_data"
    else
        echo -e "${RED}Error: 'dummy_data' directory not found.${NC}"
        echo "Please ensure you run this script from the project root directory."
        exit 1
    fi
fi

echo -e "\n${YELLOW}=== Configuration ===${NC}"
echo -e "GCP Project: ${GREEN}$PROJECT_ID${NC}"
echo -e "Dataset:     ${GREEN}$PROJECT_ID.$DATASET_NAME${NC}"
echo -e "Location:    ${GREEN}$REGION${NC}"
echo -e "Data Source: ${GREEN}$DUMMY_DATA_DIR${NC}\n"

read -rp "Proceed with loading data? (y/N): " CONFIRM
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Ingestion cancelled by user.${NC}"
    exit 0
fi

# 4. CREATE BIGQUERY DATASET
echo -e "\n${YELLOW}[1/3] Creating BigQuery dataset if it doesn't exist...${NC}"
if bq --project_id="$PROJECT_ID" show "$DATASET_NAME" &>/dev/null; then
    echo -e "Dataset '${GREEN}$DATASET_NAME${NC}' already exists. Skipping creation."
else
    bq --project_id="$PROJECT_ID" --location="$REGION" mk \
       --dataset \
       --description "CZ Compliance Training Source Dataset" \
       "$DATASET_NAME"
    echo -e "${GREEN}✓ Dataset '$DATASET_NAME' successfully created in region '$REGION'.${NC}"
fi

# 5. INGEST THE CSV TABLES
echo -e "\n${YELLOW}[2/3] Ingesting CSV files into BigQuery...${NC}"

# Dictionary-like mappings for files and target tables
declare -A TABLES
TABLES=(
    ["CZ_segm_country_risk_table.csv"]="CZ_segm_country_risk_table"
    ["CZ_segm_perimeter.csv"]="CZ_segm_perimeter"
    ["bank_transfer_after_gap.csv"]="bank_transfer_after_gap"
    ["card_after_gap.csv"]="card_after_gap"
    ["sdd_after_gap.csv"]="sdd_after_gap"
    ["cash.csv"]="cash"
    ["deal_dossier_after_gap.csv"]="deal_dossier_after_gap"
)

for csv_file in "${!TABLES[@]}"; do
    table_name="${TABLES[$csv_file]}"
    csv_path="$DUMMY_DATA_DIR/$csv_file"
    
    if [ -f "$csv_path" ]; then
        echo -e "Loading ${BLUE}$csv_file${NC} into table ${GREEN}$table_name${NC}..."
        
        # Run BQ load with autodetect and skip headers
        bq --project_id="$PROJECT_ID" --location="$REGION" load \
           --source_format=CSV \
           --skip_leading_rows=1 \
           --autodetect \
           --replace \
           "$DATASET_NAME.$table_name" \
           "$csv_path"
           
        echo -e "  └─ ${GREEN}✓ Loaded '$table_name' successfully.${NC}"
    else
        echo -e "${RED}✕ Error: File '$csv_path' not found. Skipping.${NC}"
    fi
done

# 6. VERIFICATION SUMMARY
echo -e "\n${YELLOW}[3/3] Verifying ingestion...${NC}"
echo -e "${BLUE}------------------------------------------------------------${NC}"
echo -e "Table Name | Row Count"
echo -e "${BLUE}------------------------------------------------------------${NC}"

for table_name in "${TABLES[@]}"; do
    if bq --project_id="$PROJECT_ID" show "$DATASET_NAME.$table_name" &>/dev/null; then
        ROW_COUNT=$(bq --project_id="$PROJECT_ID" query --nouse_legacy_sql --format=csv \
            "SELECT COUNT(1) FROM \`$PROJECT_ID.$DATASET_NAME.$table_name\`" | tail -n 1)
        printf "%-30s | %s\n" "$table_name" "$ROW_COUNT"
    else
        printf "%-30s | %s\n" "$table_name" "${RED}NOT FOUND${NC}"
    fi
done
echo -e "${BLUE}------------------------------------------------------------${NC}"

echo -e "\n${GREEN}🎉 Ingestion completed successfully! All tables are ready to be used in BigQuery Studio.${NC}"
