# UniCredit CZ Compliance Training: Serverless Spark & BigQuery Integration

Welcome to the **End-to-End Serverless Spark & BigQuery Integration** training lab. This hands-on course is designed to guide data engineers through migrating a legacy, cluster-based PySpark pipeline into a modern, cloud-native serverless architecture using Google Cloud Platform (GCP).

By the end of this lab, you will have programmatically ingested raw compliance data, provisioned a secure serverless Spark environment, executed interactive transformations within BigQuery Studio, and scheduled a production-grade Dataproc Serverless batch job.

---

## 🎯 Lab Objectives

1. **Programmatic Ingestion:** Learn to use the BigQuery Python SDK to load and validate mock compliance data.
2. **Secure Workspace Provisioning:** Set up GCP resources, Spark connections, and IAM role bindings using the `gcloud` CLI.
3. **Interactive Prototyping:** Connect notebooks to Serverless Spark runtimes inside BigQuery Studio.
4. **Dynamic Schema-Scale Alignment:** Implement metadata-driven PySpark decimal down-casting to avoid write conflicts.
5. **Operationalizing Batches:** Package and submit standalone PySpark workloads to Serverless Dataproc.

---

## 📂 Repository Directory Structure

To help you easily navigate this lab, the repository is organized as follows:

| Directory / File | Description | Role / Target Audience |
| :--- | :--- | :--- |
| 📖 **[STUDENT_LAB_GUIDE.md](STUDENT_LAB_GUIDE.md)** | Step-by-step training instructions for students. | **Student Main Guide** |
| 📁 **[dummy_data/](dummy_data/)** | Contains 7 raw mock compliance CSV datasets. | Data Sources (Task 1) |
| 📁 **[notebooks/](notebooks/)** | Notebook files for interactive development. | Hands-On Workspace (Task 3) |
| ├── `02_Data_Prep_transaction_BigQuery.ipynb` | **Primary Notebook** for Spark Connect inside BigQuery. | Active Workspace |
| └── `02_Data_Prep_transaction.ipynb` | Legacy, cluster-based notebook for reference. | Legacy Codebase Reference |
| 📁 **[solutions/](solutions/)** | Complete working scripts for verification and reference. | Verification / Solution Key |
| ├── `upload_data.py` | Completed BigQuery ingestion python script. | Task 1 Solution |
| └── `test_data_prep.py` | Completed standalone PySpark batch script. | Task 4 Solution |
| 📁 **[scripts/](scripts/)** | Helper bash scripts for quick execution/shortcuts. | Optional Automation Helpers |
| ├── `upload_data.sh` | Shell-based alternative to ingest mock CSV files. | Task 1 Bash Helper |
| └── `setup_spark_env.sh` | Shell script to automate connection and IAM creation. | Task 2 Bash Helper |
| 📁 **[instructor/](instructor/)** | Dedicated resources, slides reference, and prep tools. | **Instructors Only** |
| ├── `INSTRUCTOR_GUIDE.md` | Instructional tips, troubleshooting runbooks, and rubrics. | Instructor Guide |
| ├── `generate_dummy_data.py` | Script used to generate the mock dataset. | Development Tool |
| └── `adapt_notebook.py` | Script used to adapt standard notebooks to BigQuery. | Development Tool |

---

## 🚀 How to Get Started

### Prerequisites
Before starting, ensure that you have access to a GCP Project (e.g., a Qwiklabs sandbox project) with owner-level or editor-level permissions.

### Quick Start Workflow
1. **Read the Lab Guide:** Open the **[STUDENT_LAB_GUIDE.md](STUDENT_LAB_GUIDE.md)** and follow the tasks sequentially.
2. **Ingest Mock Data (Task 1):** You can write your script from scratch as guided, or check the reference script in `solutions/upload_data.py`.
3. **Configure Your Environment (Task 2):** Use the terminal commands in the guide, or run the `scripts/setup_spark_env.sh` helper to establish the connections and IAM permissions.
4. **Run Notebook (Task 3):** Upload `notebooks/02_Data_Prep_transaction_BigQuery.ipynb` to the BigQuery console and run your transformations.
5. **Operationalize Batch (Task 4):** Convert your code into a standalone batch job and submit it to Serverless Dataproc.

> [!TIP]
> If you get stuck at any step, refer to the **Troubleshooting & Common Errors** section in the **[instructor/INSTRUCTOR_GUIDE.md](instructor/INSTRUCTOR_GUIDE.md)** for quick diagnostics and resolution steps.

---

## 🎓 Summary Checklist for Success
- [ ] Raw bank transaction CSVs ingested into BigQuery.
- [ ] Spark Connection configured and IAM role permissions granted.
- [ ] Interactive notebook connected to Serverless Spark Connect.
- [ ] Standalone batch script successfully submitted and verified on Serverless Dataproc.

---
*UniCredit Compliance Training Program © 2026. Designed for Data Engineering Labs.*
