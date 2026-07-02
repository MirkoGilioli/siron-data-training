import json
import os

input_notebook = "notebooks/02_Data_Prep_transaction.ipynb"
output_notebook = "notebooks/02_Data_Prep_transaction_BigQuery.ipynb"

print(f"Reading original notebook: {input_notebook}")
with open(input_notebook, "r", encoding="utf-8") as f:
    notebook_data = json.load(f)

# Define configurations
gcp_project = "qwiklabs-asl-03-46a033ad38bd"
dataset_name = "compliance_training"

# 1. Modify the first cell (Spark Session setup)
# Find the first cell of type 'code'
for cell in notebook_data.get("cells", []):
    if cell.get("cell_type") == "code":
        source_text = "".join(cell.get("source", []))
        if "DataprocSparkSession" in source_text:
            cell["source"] = [
                "import sys\n",
                "\n",
                "# 1. Environment-Aware Spark Session Setup\n",
                "# In BigQuery Studio notebooks (running Spark 3.5 / Python 3.12), standard Spark Connect is used.\n",
                "# To prevent classloader conflicts while ensuring the 'bigquery' datasource is registered on local Python kernels,\n",
                "# we configure the Spark 3.5-specific Scala-independent Maven package.\n",
                "# This avoids duplicate shaded dependency loading errors from 'with-dependencies' jars.\n",
                "from pyspark.sql import SparkSession\n",
                "spark = (SparkSession.builder\n",
                "    .appName('UniCredit CZ Compliance Training')\n",
                "    .config('spark.jars.packages', 'com.google.cloud.spark:spark-3.5-bigquery:0.44.2')\n",
                "    .getOrCreate())\n",
                "print('✓ Spark Session successfully initialized with standard Spark 3.5 BigQuery connector.')\n"
            ]
            print("✓ Adapted Cell 1 to use clean Spark Session builder with Spark 3.5 package.")
            break

# 2. Insert configuration variables in the second cell (right after Spark Session)
cell_idx = 0
for i, cell in enumerate(notebook_data.get("cells", [])):
    if cell.get("cell_type") == "code":
        source_text = "".join(cell.get("source", []))
        if "start_year" in source_text:
            cell_idx = i
            break

notebook_data["cells"][cell_idx]["source"] = [
    f"gcp_project = '{gcp_project}'\n",
    f"dataset_name = '{dataset_name}'\n",
    "\n",
    "spark.conf.set('materializationDataset', dataset_name)\n",
    "spark.conf.set('parentProject', gcp_project)\n",
    f"spark.conf.set('temporaryGcsBucket', 'compliance-training-staging-{gcp_project}')\n",
    "print('✓ Injected dynamic materialization configs and GCS temporary bucket.')\n",
    "\n"
] + notebook_data["cells"][cell_idx]["source"]
print("✓ Injected gcp_project and dataset_name configurations with materialization and GCS staging parameters.")

# 3. Modernize table loading to use .option('table', ...) as per BigQuery Studio Spark Codelab
# This eliminates view-materialization dependencies and query bottlenecks.
modified_cells_count = 0
for cell in notebook_data.get("cells", []):
    if cell.get("cell_type") == "code":
        source_text = "".join(cell.get("source", []))
        modified = False

        # First, match load operations which contain both the variable assignment and spark.read.format
        if "country_risk_table =" in source_text and "spark.read.format" in source_text:
            cell["source"] = [
                "country_risk_table = (\n",
                "    spark.read.format('bigquery')\n",
                "    .option('table', f'{gcp_project}.{dataset_name}.CZ_segm_country_risk_table')\n",
                "    .load()\n",
                ")\n"
            ]
            modified = True
        elif "perimeter =" in source_text and "spark.read.format" in source_text:
            cell["source"] = [
                "perimeter = (\n",
                "    spark.read.format('bigquery')\n",
                "    .option('table', f'{gcp_project}.{dataset_name}.CZ_segm_perimeter')\n",
                "    .load()\n",
                ")\n"
            ]
            modified = True
        elif "tmp_bt =" in source_text and "spark.read.format" in source_text:
            cell["source"] = [
                "tmp_bt = (\n",
                "    spark.read.format('bigquery')\n",
                "    .option('table', f'{gcp_project}.{dataset_name}.bank_transfer_after_gap')\n",
                "    .load()\n",
                "    .filter(\n",
                "        F.col('trans_dt').between(\n",
                "            f'{start_year}-{start_month}-{start_day}',\n",
                "            f'{end_year}-{end_month}-{end_day}'\n",
                "        ) &\n",
                "        (F.col('trans_status_de') == 'PAID') &\n",
                "        (F.col('paym_sett_tp') != 'OTH pay OTH')\n",
                "    )\n",
                "    .select(\n",
                "        'trans_id',\n",
                "        'trans_agg_01_tp',\n",
                "        'trans_agg_02_tp',\n",
                "        F.col('eur_trans_am').cast('numeric').alias('eur_trans_am'),\n",
                "        'trans_dt',\n",
                "        'trans_status_de',\n",
                "        'paym_sett_tp',\n",
                "        'debtor_cntp_id',\n",
                "        'debtor_country_cd',\n",
                "        'debtor_deal_id',\n",
                "        'debtor_deal_branch_cd',\n",
                "        'debtor_deal_reg_tp',\n",
                "        'creditor_cntp_id',\n",
                "        'creditor_country_cd',\n",
                "        'creditor_deal_id',\n",
                "        'creditor_deal_branch_cd',\n",
                "        'creditor_deal_reg_tp',\n",
                "        F.lit('BT').alias('trx_type')\n",
                "    )\n",
                ")\n"
            ]
            modified = True
        elif "tmp_sdd =" in source_text and "spark.read.format" in source_text:
            cell["source"] = [
                "tmp_sdd = (\n",
                "    spark.read.format('bigquery')\n",
                "    .option('table', f'{gcp_project}.{dataset_name}.sdd_after_gap')\n",
                "    .load()\n",
                "    .filter(\n",
                "        F.col('trans_dt').between(\n",
                "            f'{start_year}-{start_month}-{start_day}',\n",
                "            f'{end_year}-{end_month}-{end_day}'\n",
                "        ) &\n",
                "        (F.col('trans_status_de') == 'PAID') &\n",
                "        (F.col('paym_sett_tp') != 'OTH pay OTH')\n",
                "    )\n",
                "    .select(\n",
                "        'trans_id',\n",
                "        'trans_agg_01_tp',\n",
                "        'trans_agg_02_tp',\n",
                "        F.col('eur_trans_am').cast('numeric').alias('eur_trans_am'),\n",
                "        'trans_dt',\n",
                "        'trans_status_de',\n",
                "        'paym_sett_tp',\n",
                "        'debtor_cntp_id',\n",
                "        'debtor_country_cd',\n",
                "        'debtor_deal_id',\n",
                "        'debtor_deal_branch_cd',\n",
                "        'debtor_deal_reg_tp',\n",
                "        'creditor_cntp_id',\n",
                "        'creditor_country_cd',\n",
                "        'creditor_deal_id',\n",
                "        'creditor_deal_branch_cd',\n",
                "        'creditor_deal_reg_tp',\n",
                "        'deal_id',\n",
                "        F.lit('SDD').alias('trx_type')\n",
                "    )\n",
                ")\n"
            ]
            modified = True
        elif "tmp_card =" in source_text and "spark.read.format" in source_text:
            cell["source"] = [
                "tmp_card = (\n",
                "    spark.read.format('bigquery')\n",
                "    .option('table', f'{gcp_project}.{dataset_name}.card_after_gap')\n",
                "    .load()\n",
                "    .filter(\n",
                "        F.col('trans_dt').between(\n",
                "            f'{start_year}-{start_month}-{start_day}',\n",
                "            f'{end_year}-{end_month}-{end_day}'\n",
                "        ) &\n",
                "        (F.col('trans_status_de') == 'PAID') &\n",
                "        (F.col('paym_sett_tp') != 'OTH pay OTH')\n",
                "    )\n",
                "    .select(\n",
                "        'trans_id',\n",
                "        'trans_agg_01_tp',\n",
                "        'trans_agg_02_tp',\n",
                "        F.col('eur_trans_am').cast('numeric').alias('eur_trans_am'),\n",
                "        'trans_dt',\n",
                "        'trans_status_de',\n",
                "        'paym_sett_tp',\n",
                "        'debtor_cntp_id',\n",
                "        'debtor_country_cd',\n",
                "        'debtor_deal_id',\n",
                "        'debtor_deal_branch_cd',\n",
                "        'debtor_deal_reg_tp',\n",
                "        'creditor_cntp_id',\n",
                "        'creditor_country_cd',\n",
                "        'creditor_deal_id',\n",
                "        'creditor_deal_branch_cd',\n",
                "        'creditor_deal_reg_tp',\n",
                "        F.lit('CARD').alias('trx_type')\n",
                "    )\n",
                ")\n"
            ]
            modified = True
        elif "tmp_cash =" in source_text and "spark.read.format" in source_text:
            cell["source"] = [
                "tmp_cash = (\n",
                "    spark.read.format('bigquery')\n",
                "    .option('table', f'{gcp_project}.{dataset_name}.cash')\n",
                "    .load()\n",
                "    .filter(\n",
                "        F.col('trans_dt').between(\n",
                "            f'{start_year}-{start_month}-{start_day}',\n",
                "            f'{end_year}-{end_month}-{end_day}'\n",
                "        ) &\n",
                "        (F.col('trans_status_de') == 'PAID')\n",
                "    )\n",
                "    .select(\n",
                "        'trans_id',\n",
                "        'trans_agg_01_tp',\n",
                "        'trans_agg_02_tp',\n",
                "        F.col('eur_trans_am').cast('numeric').alias('eur_trans_am'),\n",
                "        'trans_dt',\n",
                "        'trans_status_de',\n",
                "        'paym_sett_tp',\n",
                "        'transaction_tp',\n",
                "        'debtor_cntp_id',\n",
                "        'debtor_deal_id',\n",
                "        'debtor_deal_branch_cd',\n",
                "        'debtor_deal_reg_tp',\n",
                "        'creditor_cntp_id',\n",
                "        'creditor_deal_id',\n",
                "        'creditor_deal_branch_cd',\n",
                "        'creditor_deal_reg_tp',\n",
                "        'transaction_country_nm',\n",
                "        F.lit('CASH').alias('trx_type')\n",
                "    )\n",
                ")\n"
            ]
            modified = True
        elif "tmp_products =" in source_text and "spark.read.format" in source_text:
            cell["source"] = [
                "tmp_products = (\n",
                "    spark.read.format('bigquery')\n",
                "    .option('table', f'{gcp_project}.{dataset_name}.deal_dossier_after_gap')\n",
                "    .load()\n",
                "    .filter(\n",
                "        (F.col('deal_open_dt') <= f'{end_year}-{end_month}-{end_day}') &\n",
                "        (F.col('deal_close_dt').isNull() | (F.col('deal_close_dt') > f'{end_year}-{end_month}-{end_day}'))\n",
                "    )\n",
                "    .select(\n",
                "        'cntp_id',\n",
                "        'deal_surrogate_id',\n",
                "        'deal_id',\n",
                "        'deal_branch_cd',\n",
                "        F.col('deal_reg_tp_cd').alias('deal_reg_tp'),\n",
                "        'deal_reg_tp_de',\n",
                "        'cur_iso_cd',\n",
                "        'business_tp',\n",
                "        'overdue_repay_in'\n",
                "    )\n",
                ")\n"
            ]
            modified = True

        # Dynamic Schema Alignment before save
        elif "trx_final.write" in source_text and "save(" in source_text:
            cell["source"] = [
                "# Align decimal column precisions and scales with the destination BigQuery table to prevent write schema mismatch errors\n",
                "from pyspark.sql.types import DecimalType\n",
                "\n",
                "try:\n",
                "    dest_table = f'{gcp_project}.{dataset_name}.CZ_segm_trx_final_01'\n",
                "    target_schema = spark.read.format('bigquery').option('table', dest_table).load().schema\n",
                "    for field in target_schema:\n",
                "        if field.name in trx_final.columns and isinstance(field.dataType, DecimalType):\n",
                "            trx_final = trx_final.withColumn(field.name, F.col(field.name).cast(field.dataType))\n",
                "    print('✓ Automatically aligned DataFrame decimal precision and scale with BigQuery target table.')\n",
                "except Exception as e:\n",
                "    print('⚠ Target table schema alignment skipped or failed (table may not exist yet):', e)\n",
                "\n",
                "# save dataset\n",
                "trx_final.write.format('bigquery') \\\n",
                "    .option('writeMethod', 'direct') \\\n",
                "    .mode('overwrite') \\\n",
                "    .save(f'{gcp_project}.{dataset_name}.CZ_segm_trx_final_01')\n"
            ]
            modified = True

        # Second, deprecate raw query variables (if they were not already replaced as part of a single cell load)
        elif "query_crt =" in source_text:
            cell["source"] = ["# query_crt is deprecated in favor of direct table load\n"]
            modified = True
        elif "query_perimeter =" in source_text:
            cell["source"] = ["# query_perimeter is deprecated in favor of direct table load\n"]
            modified = True
        elif "query_bt =" in source_text:
            cell["source"] = ["# query_bt is deprecated in favor of direct table load\n"]
            modified = True
        elif "query_sdd =" in source_text:
            cell["source"] = ["# query_sdd is deprecated in favor of direct table load\n"]
            modified = True
        elif "query_card =" in source_text:
            cell["source"] = ["# query_card is deprecated in favor of direct table load\n"]
            modified = True
        elif "query_cash =" in source_text:
            cell["source"] = ["# query_cash is deprecated in favor of direct table load\n"]
            modified = True
        elif "query_products =" in source_text:
            cell["source"] = ["# query_products is deprecated in favor of direct table load\n"]
            modified = True

        # Perform generic replacements for other elements like final save paths
        else:
            lines = cell.get("source", [])
            new_lines = []
            replacements = {
                "prj-cdl-prd-edlsironconsww-001.bqd_cdl_prd_ew8_results_baz_compliance_001.CZ_segm_country_risk_table": "{gcp_project}.{dataset_name}.CZ_segm_country_risk_table",
                "prj-cdl-prd-edlsironconsww-001.bqd_cdl_prd_ew8_results_baz_compliance_001.CZ_segm_perimeter": "{gcp_project}.{dataset_name}.CZ_segm_perimeter",
                "prj-dpu-prd-dpcc-001.bqd_dpu_prd_ew8_edl_fdm_baz_compliance_payments_0.bank_transfer_after_gap": "{gcp_project}.{dataset_name}.bank_transfer_after_gap",
                "prj-dpu-prd-dpcc-001.bqd_dpu_prd_ew8_edl_fdm_baz_compliance_payments_0.sdd_after_gap": "{gcp_project}.{dataset_name}.sdd_after_gap",
                "prj-dpu-prd-dpcc-001.bqd_dpu_prd_ew8_edl_fdm_baz_compliance_payments_0.card_after_gap": "{gcp_project}.{dataset_name}.card_after_gap",
                "prj-dpu-prd-dpcc-001.bqd_dpu_prd_ew8_edl_fdm_baz_compliance_payments_0.cash": "{gcp_project}.{dataset_name}.cash",
                "prj-dpu-prd-dpcc-001.bqd_dpu_prd_ew8_edl_fdm_baz_compliance_deals_0.deal_dossier_after_gap": "{gcp_project}.{dataset_name}.deal_dossier_after_gap",
                "prj-cdl-prd-edlsironconsww-001.bqd_cdl_prd_ew8_results_baz_compliance_001.CZ_segm_trx_final_01": "{gcp_project}.{dataset_name}.CZ_segm_trx_final_01"
            }
            for line in lines:
                new_line = line
                for old_path, new_path in replacements.items():
                    if old_path in new_line:
                        new_line = new_line.replace(old_path, new_path)
                        modified = True
                
                if ".save('{gcp_project}.{dataset_name}.CZ_segm_trx_final_01')" in new_line:
                    new_line = new_line.replace(
                        ".save('{gcp_project}.{dataset_name}.CZ_segm_trx_final_01')",
                        ".save(f'{gcp_project}.{dataset_name}.CZ_segm_trx_final_01')"
                    )
                    modified = True
                new_lines.append(new_line)
            if modified:
                cell["source"] = new_lines

        if modified:
            modified_cells_count += 1

print(f"✓ Modernized BigQuery table loading in {modified_cells_count} cells.")

# Save modified notebook
with open(output_notebook, "w", encoding="utf-8") as f:
    json.dump(notebook_data, f, indent=2)
print(f"✓ Wrote BigQuery Studio optimized notebook to: {output_notebook}")

# 4. Extract Python script
print("Extracting clean python script scenario/test_data_prep.py...")
code_lines = []
for cell in notebook_data.get("cells", []):
    if cell.get("cell_type") == "code":
        code_lines.extend(cell.get("source", []))
        code_lines.append("\n\n")

with open("scenario/test_data_prep.py", "w", encoding="utf-8") as f:
    f.writelines(code_lines)

print("🎉 Success! Re-generation and script extraction complete.")
