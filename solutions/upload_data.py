import os
from google.cloud import bigquery

# 1. Initialize BigQuery Client
client = bigquery.Client()

# 2. Define Configurations
# Replace with your active Google Cloud Project ID
PROJECT_ID = "qwiklabs-asl-03-46a033ad38bd" 
DATASET_ID = f"{PROJECT_ID}.compliance_training"
REGION = "europe-west8"  # europe-west8 matches the training region

# 3. Create Dataset if it does not exist
dataset = bigquery.Dataset(DATASET_ID)
dataset.location = REGION
dataset.description = "CZ Compliance Training Source Dataset"

try:
    client.get_dataset(DATASET_ID)
    print(f"✓ Dataset '{DATASET_ID}' already exists.")
except Exception:
    client.create_dataset(dataset, timeout=30)
    print(f"✓ Created dataset '{DATASET_ID}' in location '{REGION}'.")

# 4. Define Ingestion Mapping (CSV Filename -> BigQuery Table Name)
tables_to_load = {
    "CZ_segm_country_risk_table.csv": "CZ_segm_country_risk_table",
    "CZ_segm_perimeter.csv": "CZ_segm_perimeter",
    "bank_transfer_after_gap.csv": "bank_transfer_after_gap",
    "card_after_gap.csv": "card_after_gap",
    "sdd_after_gap.csv": "sdd_after_gap",
    "cash.csv": "cash",
    "deal_dossier_after_gap.csv": "deal_dossier_after_gap",
}

# Source directory containing raw CSVs
DUMMY_DATA_DIR = "./dummy_data"

# 5. Ingest each CSV
for csv_file, table_name in tables_to_load.items():
    csv_path = os.path.join(DUMMY_DATA_DIR, csv_file)
    table_ref = f"{DATASET_ID}.{table_name}"
    
    if not os.path.exists(csv_path):
        print(f"✕ Skipping {csv_file}: path not found.")
        continue

    print(f"Loading {csv_file} into {table_ref}...")
    
    # Configure Load Job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,      # Skip header row
        autodetect=True,          # Automatically infer column data types
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # Overwrite existing
    )
    
    with open(csv_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )
        
    load_job.result()  # Wait for the job to complete
    print(f"  └─ ✓ Successfully loaded {table_name}.")

# 6. Verify row counts
print("\n--- Ingestion Verification Summary ---")
for table_name in tables_to_load.values():
    table_ref = f"{DATASET_ID}.{table_name}"
    table = client.get_table(table_ref)
    print(f"Table: {table_name:<30} | Row Count: {table.num_rows}")
