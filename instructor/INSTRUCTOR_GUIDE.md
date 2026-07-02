# Instructor Guide: End-to-End Serverless Spark & BigQuery Integration

## Module Overview
This guide provides the necessary architectural context, classroom setup procedures, technical deep-dives, and troubleshooting protocols for instructors delivering the **End-to-End Serverless Spark and BigQuery Integration** hands-on training. 

This lab is designed to teach data engineers how to migrate and optimize legacy, cluster-based PySpark jobs into Google Cloud's managed Serverless Spark and BigQuery Studio environment.

### Primary Learning Objectives
1. **Programmatic Ingestion:** Learn to use the BigQuery Python SDK to load and validate structured source data.
2. **Secure Workspace Provisioning:** Configure connection resources and IAM bindings using `gcloud`.
3. **Interactive Prototyping:** Master the Spark Connect lifecycle within BigQuery Studio.
4. **Dynamic Schema-Scale Alignment:** Implement self-healing numeric casting in PySpark to resolve write-time precision conflicts.
5. **Operationalizing Batches:** Package and submit standalone Spark batch workloads to Serverless Dataproc.

---

## 1. Classroom Setup & Prerequisites

Before class begins, ensure that the active Google Cloud project (such as a Qwiklabs sandbox project) is fully provisioned.

### 1.1 Dataset and Raw Assets
*   **Active Project ID:** Ensure students retrieve and replace their dynamic GCP Project ID (represented as `qwiklabs-asl-03-46a033ad38bd` in scripts) in all configurations.
*   **Ingestion Files:** The mock bank compliance CSV dataset must be present inside the workstation/Cloud Shell directory `./dummy_data`.
*   **Jupyter Notebook:** Ensure the pre-configured notebook file `02_Data_Prep_transaction_BigQuery.ipynb` is uploaded to an easily accessible sharing folder or repository for students to download.

### 1.2 Verification Commands
Instructors can run these commands from Cloud Shell to verify a student's workspace state:

```bash
# Verify BigQuery tables are successfully ingested:
bq ls --project_id=[PROJECT_ID] compliance_training

# Verify Spark connection in US (or US-Central1):
bq show --connection --project_id=[PROJECT_ID] --location=us spark-connection

# Verify the GCS Staging bucket is created:
gsutil ls | grep compliance-training-staging
```

---

## 2. Key Architectural Concepts to Teach

Use the whiteboard or slides to highlight these core architectural shifts:

### Concept A: Separation of Compute and Storage
*   **Legacy Model:** Traditional Hadoop/Spark clusters (like Dataproc on GCE or on-prem HDFS) co-locate storage and compute on long-running VM nodes. This is expensive and scales poorly.
*   **Modern Cloud-Native Model:** BigQuery manages raw columnar storage, while Serverless Dataproc runs compute elastically on-demand. Data is streamed in-memory via the **BigQuery Storage Read API**, completely avoiding local storage bottlenecks.

### Concept B: Spark Connect Architecture
Explain that **BigQuery Studio Notebooks** do not run local PySpark engines. They use **Spark Connect** (introduced in Spark 3.4):
1. The Notebook acts as a lightweight client.
2. It sends execution graphs (logical plans) via gRPC to a managed Google serverless Spark Connect backend.
3. This decouples the notebook execution state from the heavy cluster JVM, making session startup nearly instant.

### Concept C: Shaded "Fat" JARs vs. "Thin" JARs
*   **Legacy "With-Dependencies" JARs:** Standard tutorials recommend `spark-bigquery-with-dependencies_2.12`. Explain that this "fat" JAR shades and repackages Google Cloud client libraries.
*   **The Conflict:** Dataproc Serverless has the BigQuery connector preloaded. Adding a "fat" jar introduces duplicate, conflicting classes on the JVM classloader, causing instant boot failures.
*   **The Solution:** Use Scala-independent "thin" jars built specifically for major Spark versions, such as:
    `com.google.cloud.spark:spark-3.5-bigquery:0.44.2`

### Concept D: The Precision/Scale Write Bottleneck
Explain why PySpark writes can trigger a Guice `ProvisionException`/`InvalidSchemaException` on decimals:
*   In PySpark, standard divisions (like calculating percentage contributions) return a high-precision scale of 18 (`DecimalType(38,18)`).
*   However, BigQuery tables enforce a strict `BIGNUMERIC(38, 6)` or scale 9 schema. 
*   Because decimal down-casting without explicit rules can lead to silent rounding/precision losses, BigQuery's connector rejects the write.
*   **The Solution:** Teach students to write metadata-driven loops that dynamically inspect the target table schema and cast fields before saving, ensuring their pipelines are fully self-healing.

---

## 3. Classroom Troubleshooting & Common Errors

Students will run into these issues. Guide them through diagnostics using this runbook:

### Error 1: SparkClassNotFoundException: DATA_SOURCE_NOT_FOUND: Failed to find the data source: bigquery
*   **Diagnostic:** The Spark Session has been initialized without the BigQuery connector jar, or the student is running on a standard Python kernel instead of a Google-managed Serverless Spark runtime.
*   **Instructor Action:** Have the student check Cell 1. Ensure they are using the standard Spark 3.5 package dependency in their builder:
    ```python
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-3.5-bigquery:0.44.2')
    ```

### Error 2: java.util.ServiceConfigurationError: com.google.cloud.spark.bigquery.BigQueryRelationProvider could not be instantiated
*   **Diagnostic:** The student is running on a Dataproc Connect runtime that has the connector preloaded, but they manually loaded the shaded `spark-bigquery-with-dependencies_2.12` JAR. This creates a duplicate classpath conflict.
*   **Instructor Action:** Instruct the student to replace the `spark-bigquery-with-dependencies_2.12` package string with the standard, Scala-independent `spark-3.5-bigquery:0.44.2` package.

### Error 3: Guice/ErrorInCustomProvider: IllegalArgumentException: Destination table's schema is not compatible with dataframe's schema. Incompatible precision, scale: cannot write source field (38, 18) to destination field (38, 6)
*   **Diagnostic:** The Spark DataFrame has decimal fields with a default scale of 18 (usually from aggregations/divisions) trying to overwrite a pre-existing BigQuery table field with scale 6.
*   **Instructor Action:** Have the student check **Cell 5**. Ensure their Schema-Alignment try/except block is not commented out. Explain that the block queries the destination table metadata schema and casts decimal fields in the DataFrame down to match the target table before writing.

### Error 4: Batch submission fails with VPC network errors
*   **Diagnostic:** Dataproc Serverless requires a VPC subnet with **Private Google Access** enabled in order to securely talk to internal Google APIs (like BigQuery and Cloud Storage).
*   **Instructor Action:** Run this command to verify or enable Private Google Access on the default subnet of your project region:
    ```bash
    gcloud compute networks subnets update default \
        --region="us-central1" \
        --enable-private-ip-google-access
    ```

---

## 4. Discussion Prompts (To Gauge Student Understanding)

Ask the class these questions during transition phases:

1.  *“If we run our Notebook and the Spark session is already injected under the `spark` global variable, why do we still include `SparkSession.builder.getOrCreate()` inside our script?”*
    *   **Answer:** Portability and robustness. It allows the exact same code to run interactively inside the BigQuery console (retrieving the pre-loaded global session) and as an automated standalone batch job on Serverless Dataproc (creating a fresh session with dependency packages on boot).
2.  *“Why is loading tables with `spark.read.format('bigquery').option('table', ...)` more optimized than executing a raw SQL query string inside Spark?”*
    *   **Answer:** Direct table loading streams the data directly from BigQuery storage using the **BigQuery Storage Read API**, bypassing the SQL query compilation and materialization stages. Spark can push down standard filters (`.filter()`) and column projections (`.select()`) natively to BigQuery storage.
3.  *“What is the advantage of using our try/except block to inspect the BigQuery target schema before saving, rather than simply writing `.cast(DecimalType(38,6))` on every column manually?”*
    *   **Answer:** It is metadata-driven and self-healing. If a table's schema changes (e.g., scale increases to 9) or is newly created, the script dynamically adapts or falls back cleanly, completely avoiding brittle, hardcoded column names and type changes.

---

## 🏆 Assessment Rubric for Grading Labs
Score students based on the execution state of their environment:

*   **Grade A (Production Ready):** Ingestion script completes via BigQuery SDK. Spark Connection successfully created and authenticated. Notebook runs through cell 5 without schema or classpath errors. Serverless Dataproc batch job successfully completes execution and terminates cleanly.
*   **Grade B (Notebook Only):** Ingestion and environment are fully set up. Notebook executes successfully in the BigQuery Studio UI, but the student fails to convert, upload, and submit the standalone script to Serverless Dataproc.
*   **Grade C (Partially Configured):** Raw data is ingested and the notebook runs, but they experience write-time schema precision conflicts due to missing or misconfigured schema-alignment code.
*   **Grade F (Incomplete):** The student cannot connect their notebook to Serverless Spark or loads conflicting packages causing runtime initialization errors.
