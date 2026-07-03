# Training Lab Guide: End-to-End Serverless Spark & BigQuery Integration

Welcome to the **End-to-End Serverless Spark & BigQuery Integration** training lab. In this hands-on lab, you will transition a legacy, cluster-based PySpark pipeline into a modern, cloud-native serverless architecture. 

You will guide your compliance data through its entire lifecycle: programmatically ingesting raw CSV logs, provisioning a secure serverless environment using automated scripts, and executing interactive PySpark transformations directly inside the BigQuery console (BigQuery Studio).

---

## 🎯 Lab Objectives

1. **Clone & Authenticate:** Clone the code repository and authorize your Cloud Shell session.
2. **Automated Data Ingestion:** Execute the automated ingestion script to upload mock CSV records under auto-detected schemas into BigQuery.
3. **Automated Spark Environment Provisioning:** Set up your Serverless Spark Connections, GCS staging bucket, and IAM security profiles using automated utilities.
4. **Interactive Notebook Execution:** Upload, connect, and run your compliance transformations natively in BigQuery Studio.

---

## 🛠️ Task 1: Initialize Your Workspace in Google Cloud Shell

In this task, you will set up your command-line environment inside Google Cloud Shell, clone the official training codebase, and authorize your active sessions.

### 1.1 Open Cloud Shell & Clone the Repository
1. Navigate to the [Google Cloud Console](https://console.cloud.google.com).
2. Click the **Activate Cloud Shell** icon (>_) in the top-right toolbar.
3. Once the shell finishes provisioning, clone the training repository by running:
   ```bash
   git clone https://github.com/MirkoGilioli/siron-data-training.git
   ```

### 1.2 Enter the Project Folder
Enter the newly cloned directory:
```bash
cd siron-data-training
```

### 1.3 Authenticate Google Cloud CLI & Credentials
To allow scripting tools and programmatic clients to run on your behalf, authorize your Cloud Shell session and establish **Application Default Credentials (ADC)**:

1. **Authorize the standard gcloud session:**
   ```bash
   gcloud auth login
   ```
   *Follow the on-screen prompt, click the provided verification URL, authenticate with your student training account, and paste the authorization code back into the terminal.*

2. **Establish Application Default Credentials (ADC):**
   ```bash
   gcloud auth application-default login
   ```
   *Follow the same verification process to authorize programmatic APIs and SDK clients operating from your terminal.*

---

## 📥 Task 2: Ingest Compliance Data into BigQuery

Rather than writing an ingestion loader from scratch, you will run an automated script that creates a BigQuery dataset and programmatically uploads all mock compliance CSV files.

### 2.1 Run the Automated Loader
Execute the interactive ingestion script from the project root directory:
```bash
bash scripts/upload_data.sh
```

### 2.2 Configure and Confirm Ingestion
During execution, the script will prompt you for configuration values:
1. **GCP Project ID:** Hit **Enter** to accept your active training project, or type it in manually.
2. **Dataset Name:** Hit **Enter** to accept the default name: `compliance_training`.
3. **GCP Region:** Hit **Enter** to accept the default training region: `us-central1`.
4. **Confirmation:** Type `y` and hit **Enter** to proceed.

*The script will automatically create the dataset and load 7 mock compliance tables (`CZ_segm_country_risk_table`, `CZ_segm_perimeter`, `bank_transfer_after_gap`, `card_after_gap`, `sdd_after_gap`, `cash`, and `deal_dossier_after_gap`). At completion, it will display a row-count summary verifying successful load.*

---

## ⚙️ Task 3: Provision Serverless Spark Resources and IAM Security

To run PySpark notebooks directly inside the BigQuery console, BigQuery requires a managed **Spark Connection** and an isolated **Cloud Storage staging bucket** to act as a workspace. 

You will use an automated setup utility that provisions these assets and configures the necessary least-privilege security roles.

### 3.1 Run the Environment Setup Utility
Run the Spark environment script from your terminal:
```bash
bash scripts/setup_spark_env.sh
```

### 3.2 Provide Setup Parameters
1. **GCP Project ID:** Hit **Enter** to accept the active project.
2. **gcloud Authentication Check:** The script will verify your logins from Task 1. Since you already authenticated, it will quickly proceed.
3. **GCP Region:** Hit **Enter** to accept the default region: `us-central1`.
4. **Confirmation:** Type `y` and hit **Enter** to proceed.

### 3.3 What the Script Automates Behind the Scenes:
> [!NOTE]
> The setup script executes the following key configurations on your behalf:
> 1. **Enables APIs:** Activates BigQuery Connection (`bigqueryconnection.googleapis.com`) and Dataproc (`dataproc.googleapis.com`) APIs.
> 2. **Creates Spark Connection:** Provisions a BigQuery connection named `spark-connection`.
> 3. **Retrieves Connection Service Account:** Extracts the auto-generated service account linked to the connection.
> 4. **Creates GCS Staging Bucket:** Creates a globally unique bucket named `gs://compliance-training-staging-[PROJECT_ID]`.
> 5. **Grants IAM Roles:** Binds the Connection Service Account to essential security roles:
>    *   `roles/dataproc.worker` (Necessary to spin up Serverless executors)
>    *   `roles/bigquery.admin` (Allows reading source and writing final compliance datasets)
>    *   `roles/storage.objectAdmin` (Allows reading/writing scratch files to the staging bucket)

*Keep the terminal output summary handy. You will need the staging GCS bucket name and the active location for the next steps!*

---

## 🖥️ Task 4: Run the Compliance Pipeline in BigQuery Studio (UI)

Now that your data is ingested and your serverless environment is provisioned, you will upload and execute the full compliance processing pipeline directly inside **BigQuery Studio**.

### 4.1 Download the Notebook to Your Local Machine
You need to transfer the development notebook file to your local computer so that you can upload it into the Cloud Console UI.

*   Locate the file `02_Data_Prep_transaction_BigQuery.ipynb` inside the `notebooks/` directory of your cloned repo.
*   **If working in Cloud Shell:** You can download it directly from the Cloud Shell editor, or download it using the terminal:
    ```bash
    # Run this command in Cloud Shell to trigger an browser download prompt:
    cloudshell download notebooks/02_Data_Prep_transaction_BigQuery.ipynb
    ```

### 4.2 Upload the Notebook to the Google Cloud Console
1. Open the **BigQuery** console in the Google Cloud Console UI.
2. In the left-hand **Explorer** panel, locate the **Notebooks** section.
3. Click the **three-dot kebab menu** next to "Notebooks" (or click **+ ADD** at the top of the panel) and select **Upload notebook**.
4. Browse your local files, select the downloaded `02_Data_Prep_transaction_BigQuery.ipynb` file, and upload it.

### 4.3 Connect the Serverless Spark Connect Runtime
1. Double-click your newly uploaded `02_Data_Prep_transaction_BigQuery.ipynb` notebook inside BigQuery to open it.
2. In the top-right toolbar of the notebook, click the **Connect** dropdown menu.
3. Select **Connect to a runtime** (or configure session settings).
4. Select your pre-configured **Serverless Spark Connect runtime** operating in your selected region (e.g., `us-central1`).
5. **Enable APIs (if prompted):** If the UI indicates any missing APIs, confirm and click **Enable** to let the BigQuery Studio console initialize your notebook runtime session.

### 4.4 Step Through and Validate the Pipeline Cells
Once connected, step through the cells sequentially and observe the compliance data transformation steps:

1. **Chapter 1 (Spark Session):** Run the cell to establish a clean Spark Connect session.
2. **Chapter 2 (Configurations):** 
   > [!WARNING]
   > Update the variables inside this code block before executing it:
   > *   Change `gcp_project` to match your active **GCP Project ID**.
   > *   Ensure `temporaryGcsBucket` points to the staging bucket created in Task 3: `'compliance-training-staging-[YOUR_PROJECT_ID]'`.
3. **Chapter 3 (Unifying Channels):** Run the cells. Watch how standard Bank Transfers (BT), Direct Debits (SDD), Cards, and Cash are split into credit/debit rows, unioned, and consolidated.
4. **Chapter 4 & 5 (Feature Engineering & Products):** Step through the aggregations. You will see transactions left-joined with Country Risk tables and joined with Product dossiers, generating flattened customer profiles.
5. **Chapter 6 (Schema Safeguard & Save):** Run the final cell. This block programmatically loads the target table metadata schema, down-casts Spark decimals to match BigQuery scale definitions, and saves the final consolidated compliance ledger (`CZ_segm_trx_final_01`) successfully with zero write mismatch errors!

---

## 🏆 Success Checklist
Before completing your lab, verify that you have successfully met all checkpoints:
- [ ] Code repository cloned and session authenticated with ADC credentials in Cloud Shell.
- [ ] Mock CSV records programmatically loaded into BigQuery via `upload_data.sh`.
- [ ] Spark Connection and GCS staging bucket securely provisioned via `setup_spark_env.sh`.
- [ ] Training notebook downloaded to your local machine and imported into BigQuery Studio.
- [ ] Variables (`gcp_project` and `temporaryGcsBucket`) updated with your active training configurations.
- [ ] All notebook chapters executed successfully, writing the final `CZ_segm_trx_final_01` dataset to BigQuery.
