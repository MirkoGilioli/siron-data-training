# Training Lab Guide: End-to-End Serverless Spark & BigQuery Integration

## Lab Overview
In this hands-on lab, you will transition a legacy, cluster-based PySpark pipeline into a modern, cloud-native serverless architecture. You will guide your data through the entire operational lifecycle: programmatically ingesting raw CSV files, provisioning a secure serverless environment, running interactive Spark transformations using a pre-built notebook inside the BigQuery console, and scheduling a standalone Serverless Dataproc batch job.

### Objectives
1. **Upload Tables to BigQuery** using the BigQuery Python SDK.
2. **Setup the Spark Environment** using the Google Cloud SDK (`gcloud`).
3. **Upload and Run the PySpark Notebook** directly in the BigQuery Console UI (BigQuery Studio).
4. **Deploy & Run PySpark Batch Workloads** natively on Serverless Dataproc.

---

## Task 1: Upload Tables to BigQuery using the BigQuery Python SDK

In this task, you will write and execute a Python ingestion script using the official Google Cloud BigQuery client library. This script will programmatically provision a BigQuery dataset and load raw CSV files under auto-detected schemas.

### 1.1 Ingestion Script Setup
Create a file named `upload_data.py` on your workstation or Cloud Shell:

```python
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
```

### 1.2 Execution
Run your script in the terminal:
```bash
python upload_data.py
```
*Verify that all 7 tables show loaded status and row counts match your expectations.*

---

## Task 2: Setup Spark Environment in BigQuery using Google Cloud SDK

Serverless Spark Connect inside the BigQuery console works on a client-server architecture. To execute Spark code, BigQuery requires a managed **Spark Connection** and an isolated Cloud Storage staging bucket to act as an intermediate workspace.

In this task, you will configure these secure runtime assets using the `gcloud` CLI.

### 2.1 Enable APIs & Create Connection
Run these commands in your shell terminal:

1. **Enable the required BigQuery Connection and Dataproc APIs:**
   ```bash
   gcloud services enable bigqueryconnection.googleapis.com dataproc.googleapis.com
   ```

2. **Create the BigQuery Spark Connection resource:**
   ```bash
   bq mk --connection \
      --location="europe-west8" \
      --connection_type="SPARK" \
      "spark-connection"
   ```

3. **Retrieve the auto-generated Spark Connection Service Account Email:**
   ```bash
   bq show --connection --location="europe-west8" --format=json "spark-connection" | grep "serviceAccountId"
   ```
   *Take note of this Service Account email address. It will be formatted like `bq-cx-xxxxxxxxxx-xxxx@gcp-sa-bigquery-condel.iam.gserviceaccount.com`.*

### 2.2 Provision GCS Staging Bucket
Create a storage bucket to host the temporary work files for Spark reads and writes:
```bash
# Bucket name must be globally unique. Let's suffix it with your Project ID:
STAGING_BUCKET="compliance-training-staging-qwiklabs-asl-03-46a033ad38bd"

gcloud storage buckets create "gs://$STAGING_BUCKET" --location="europe-west8"
```

### 2.3 Grant IAM Roles to the Connection Service Account
Configure least-privilege security roles on the Spark Connection service account. Run the following, replacing `[SA_EMAIL]` with the email retrieved in step 2.1:

```bash
SA_EMAIL="[YOUR_CONNECTION_SERVICE_ACCOUNT_EMAIL]"

# 1. Grant Dataproc Worker role (Required to spin up Serverless Spark executors)
gcloud projects add-iam-policy-binding "qwiklabs-asl-03-46a033ad38bd" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/dataproc.worker"

# 2. Grant BigQuery Admin role (Required to read source and write final schemas)
gcloud projects add-iam-policy-binding "qwiklabs-asl-03-46a033ad38bd" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.admin"

# 3. Grant GCS Object Admin role on the staging bucket
gcloud storage buckets add-iam-policy-binding "gs://compliance-training-staging-qwiklabs-asl-03-46a033ad38bd" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.objectAdmin"
```

---

## Task 3: Import & Run the PySpark Notebook in BigQuery Studio (UI)

With your datasets and environment provisioned, you will import and execute the full compliance processing pipeline. This phase is handled directly within **BigQuery Studio** in the Google Cloud Console.

### 3.1 Upload the Training Notebook
Your instructor will provide you with the ready-to-run Jupyter notebook file:  
`02_Data_Prep_transaction_BigQuery.ipynb`

1. Open the **BigQuery** console in the Google Cloud Console.
2. In the left-hand **Explorer** panel, find the **Notebooks** section.
3. Click the **three-dot kebab menu** next to "Notebooks" (or "Explorer" ADD menu) and select **Upload notebook**.
4. Browse and select the `02_Data_Prep_transaction_BigQuery.ipynb` file from your local machine.
5. Click **Upload** to import the notebook directly into your BigQuery workspace.

### 3.2 Establish the Serverless Connection
1. Double-click the newly uploaded `02_Data_Prep_transaction_BigQuery.ipynb` notebook to open it.
2. In the top-right toolbar of the notebook editor, click the **Connect** dropdown menu.
3. Select **Connect to a runtime** (or configure a session) and select your pre-configured **Serverless Spark Connect runtime** in region `europe-west8`.

### 3.3 Validate and Run the Code Cells
Once connected, step through the notebook cells sequentially:

1. **Cell 1 (Spark Session):** Execute the cell to automatically hook into the pre-configured GCP Spark Connect runtime.
2. **Cell 2 (Configurations):** Review the target configuration parameters. Verify that `gcp_project` matches your active Project ID and `temporaryGcsBucket` points to your staging bucket.
3. **Cells 3 - 5 (Data Transformation):** Run these cells. You will see Spark natively streaming BigQuery tables directly, processing transactions, and generating compliance aggregates.
4. **Final Cell (Schema-Safe Save):** Run the final cell. It contains a self-healing schema-alignment block that automatically retrieves the schema of your target table (`CZ_segm_trx_final_01`) and casts the DataFrame's numeric columns down to the exact scale expected by BigQuery, avoiding any `ProvisionException` errors.

---

## Task 4: Submit PySpark Batch Job to Serverless Dataproc

To operationalize your data pipeline for scheduled production schedules, you must convert the interactive notebook cells into a standalone Python file and run it natively on Google Cloud Serverless Dataproc as a batch job.

### 4.1 Standalone Script Modification
Convert your notebook cells into a single file named `test_data_prep.py`. Ensure that you add the standard Scala-independent BigQuery Maven package (`spark-3.5-bigquery:0.44.2`) inside the standalone fallback block:

```python
# test_data_prep.py
import sys
from pyspark.sql import SparkSession

# Initialize clean session with modern Scala-independent BigQuery support
spark = (SparkSession.builder
    .appName('UniCredit CZ Compliance Training Standalone')
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-3.5-bigquery:0.44.2')
    .getOrCreate())

gcp_project = 'qwiklabs-asl-03-46a033ad38bd'
dataset_name = 'compliance_training'

spark.conf.set('temporaryGcsBucket', f'compliance-training-staging-{gcp_project}')
spark.conf.set('parentProject', gcp_project)

# ... [Include Task 3 logic here: Load, Filter, Align Schema, Save] ...
```

Upload your `test_data_prep.py` script into your Cloud Storage bucket so that Dataproc can access it:
```bash
gcloud storage cp test_data_prep.py gs://compliance-training-staging-qwiklabs-asl-03-46a033ad38bd/scripts/test_data_prep.py
```

### 4.2 Submit Serverless Dataproc Batch Job
Now, submit the standalone PySpark job to Serverless Dataproc using the Google Cloud SDK CLI. Specify your staging bucket and regional network configurations:

```bash
gcloud dataproc batches submit pyspark \
    gs://compliance-training-staging-qwiklabs-asl-03-46a033ad38bd/scripts/test_data_prep.py \
    --project="qwiklabs-asl-03-46a033ad38bd" \
    --region="us-central1" \
    --deps-bucket="gs://compliance-training-staging-qwiklabs-asl-03-46a033ad38bd"
```

### 4.3 Verify Executed Batch Jobs
1. Go to the GCP Console and navigate to **Dataproc > Serverless > Batches**.
2. Select the `us-central1` region.
3. Locate your batch ID (e.g. `compliance-batch-xxxxxxxxxx`).
4. Click on **Logs** to inspect active job execution. You should see a successful run printout:
   `✓ Spark Session successfully initialized with standard Spark 3.5 BigQuery connector.`
   `✓ Automatically aligned DataFrame decimal precision and scale with BigQuery target table.`
   `🎉 Success! CZ final compliance dataset written...`

---

## 🏆 Summary Checklist for Instructors/Students
*   [ ] Raw bank transaction CSVs ingested into BigQuery via SDK with correct schemas.
*   [ ] GCS staging bucket created & BigQuery Spark connection configured in `us` / `us-central1`.
*   [ ] Connection Service Account possesses Dataproc Worker, BigQuery Admin, and GCS Object Admin privileges.
*   [ ] Notebook file (`02_Data_Prep_transaction_BigQuery.ipynb`) uploaded directly via the BigQuery console Explorer.
*   [ ] Connected notebook successfully to the regional Serverless Spark Connect runtime.
*   [ ] Notebook executes without `ServiceConfigurationError` and handles schema-matching decimals seamlessly.
*   [ ] Standalone PySpark script submitted, monitored, and successfully completed as a Serverless Batch.
