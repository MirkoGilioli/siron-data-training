import sys

# 1. Environment-Aware Spark Session Setup
# In BigQuery Studio notebooks (running Spark 3.5 / Python 3.12), standard Spark Connect is used.
# To prevent classloader conflicts while ensuring the 'bigquery' datasource is registered on local Python kernels,
# we configure the Spark 3.5-specific Scala-independent Maven package.
# This avoids duplicate shaded dependency loading errors from 'with-dependencies' jars.
from pyspark.sql import SparkSession
spark = (SparkSession.builder
    .appName('UniCredit CZ Compliance Training')
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-3.5-bigquery:0.44.2')
    .getOrCreate())
print('✓ Spark Session successfully initialized with standard Spark 3.5 BigQuery connector.')


gcp_project = 'qwiklabs-asl-03-46a033ad38bd'
dataset_name = 'compliance_training'

spark.conf.set('materializationDataset', dataset_name)
spark.conf.set('parentProject', gcp_project)
spark.conf.set('temporaryGcsBucket', 'compliance-training-staging-qwiklabs-asl-03-46a033ad38bd')
print('✓ Injected dynamic materialization configs and GCS temporary bucket.')

from pyspark.sql import functions as F, Row
from pyspark.sql.window import Window as W
from pyspark.sql.types import NumericType

start_year, start_month, start_day = '2025', '01', '01'
end_year, end_month, end_day = '2025', '12', '31'

country = 'CZ'
drop_col_list = (
    'manorg_id',
    'master_kyc',
    'ref_dt',
    'last_update_dt',
    'snapshot_dt'
)

jurisdictions_list = (
    'Z110',
    'Z11A',
    'Z11B',
    'Z11C',
    'Z120',
    'Z12A',
    'Z140',
    'Z14A',
    'Z14B',
    'Z14C',
    'Z14D',
    'Z14E',
    'Z150',
    'Z15A',
    'Z160',
    'Z16A',
    'Z16B',
)

country_risk_table = (
    spark.read.format('bigquery')
    .option('table', f'{gcp_project}.{dataset_name}.CZ_segm_country_risk_table')
    .load()
)


country_risk_table.show(5, truncate=False)

perimeter = (
    spark.read.format('bigquery')
    .option('table', f'{gcp_project}.{dataset_name}.CZ_segm_perimeter')
    .load()
)


perimeter.show(5, truncate= False)

# query_bt is deprecated in favor of direct table load


tmp_bt = (
    spark.read.format('bigquery')
    .option('table', f'{gcp_project}.{dataset_name}.bank_transfer_after_gap')
    .load()
    .filter(
        F.col('trans_dt').between(
            f'{start_year}-{start_month}-{start_day}',
            f'{end_year}-{end_month}-{end_day}'
        ) &
        (F.col('trans_status_de') == 'PAID') &
        (F.col('paym_sett_tp') != 'OTH pay OTH')
    )
    .select(
        'trans_id',
        'trans_agg_01_tp',
        'trans_agg_02_tp',
        F.col('eur_trans_am').cast('numeric').alias('eur_trans_am'),
        'trans_dt',
        'trans_status_de',
        'paym_sett_tp',
        'debtor_cntp_id',
        'debtor_country_cd',
        'debtor_deal_id',
        'debtor_deal_branch_cd',
        'debtor_deal_reg_tp',
        'creditor_cntp_id',
        'creditor_country_cd',
        'creditor_deal_id',
        'creditor_deal_branch_cd',
        'creditor_deal_reg_tp',
        F.lit('BT').alias('trx_type')
    )
)


# tmp_bt.show(5, truncate=False)

# creditor (incoming)
bt_crd = (
    tmp_bt
    .filter(F.col('paym_sett_tp').isin('OTH pay BAZ', 'BAZ pay BAZ'))
    .withColumn('cntp_id', F.col('creditor_cntp_id'))
    .withColumn('deal_id', F.lpad(F.col('creditor_deal_id'), 16, '0'))
    .withColumn('deal_reg_tp', F.lpad(F.col('creditor_deal_reg_tp'), 5, '0'))
    .withColumn('deal_branch_cd', F.lpad(F.col('creditor_deal_branch_cd'), 5, '0'))
    .withColumn('country_cd', F.col('debtor_country_cd'))
    .withColumn('direction', F.lit('inc'))
)

# debtor (outgoing)
bt_deb = (
    tmp_bt
    .filter(F.col('paym_sett_tp').isin('BAZ pay OTH', 'BAZ pay BAZ'))
    .withColumn('cntp_id', F.col('debtor_cntp_id'))
    .withColumn('deal_id', F.lpad(F.col('debtor_deal_id'), 16, '0'))
    .withColumn('deal_reg_tp', F.lpad(F.col('debtor_deal_reg_tp'), 5, '0'))
    .withColumn('deal_branch_cd', F.lpad(F.col('debtor_deal_branch_cd'), 5, '0'))
    .withColumn('country_cd', F.col('creditor_country_cd'))
    .withColumn('direction', F.lit('out'))
)

# union
bt = (
    bt_crd
    .unionByName(bt_deb)
    .filter(F.col('cntp_id').isNotNull())
    .withColumn('pk', F.concat_ws("_", 'trans_id', 'direction'))
    .withColumn('deal_surrogate_id', F.concat_ws("_", 'deal_id', 'deal_branch_cd', 'deal_reg_tp'))
    .select(
       'trans_id',
       'trx_type',
       'trans_agg_01_tp',
       'trans_agg_02_tp',
       'eur_trans_am',
       'trans_dt',
       'trans_status_de',
       'paym_sett_tp',
       'cntp_id',
       'deal_surrogate_id',
       'country_cd',
       'direction',
       'pk'
    )
)

bt.show(5, truncate=False)

# print('Row count:', bt.count())

# query_sdd is deprecated in favor of direct table load


tmp_sdd = (
    spark.read.format('bigquery')
    .option('table', f'{gcp_project}.{dataset_name}.sdd_after_gap')
    .load()
    .filter(
        F.col('trans_dt').between(
            f'{start_year}-{start_month}-{start_day}',
            f'{end_year}-{end_month}-{end_day}'
        ) &
        (F.col('trans_status_de') == 'PAID') &
        (F.col('paym_sett_tp') != 'OTH pay OTH')
    )
    .select(
        'trans_id',
        'trans_agg_01_tp',
        'trans_agg_02_tp',
        F.col('eur_trans_am').cast('numeric').alias('eur_trans_am'),
        'trans_dt',
        'trans_status_de',
        'paym_sett_tp',
        'debtor_cntp_id',
        'debtor_country_cd',
        'debtor_deal_id',
        'debtor_deal_branch_cd',
        'debtor_deal_reg_tp',
        'creditor_cntp_id',
        'creditor_country_cd',
        'creditor_deal_id',
        'creditor_deal_branch_cd',
        'creditor_deal_reg_tp',
        'deal_id',
        F.lit('SDD').alias('trx_type')
    )
)


# Create one row per ndg and associate the payment's direction
sdd_crd = (
    tmp_sdd
    .filter(F.col('paym_sett_tp').isin('OTH pay BAZ', 'BAZ pay BAZ'))
    .withColumn('cntp_id', F.col('creditor_cntp_id'))
    .withColumn('deal_id', F.lpad(F.col('creditor_deal_id'), 16, '0'))
    .withColumn('deal_reg_tp', F.lpad(F.col('creditor_deal_reg_tp'), 5, '0'))
    .withColumn('deal_branch_cd', F.lpad(F.col('creditor_deal_branch_cd'), 5, '0'))
    .withColumn('country_cd', F.col('debtor_country_cd'))
    .withColumn('direction', F.lit('inc'))
)

sdd_deb = (
    tmp_sdd
    .filter(F.col('paym_sett_tp').isin('BAZ pay OTH', 'BAZ pay BAZ'))
    .withColumn('cntp_id', F.col('debtor_cntp_id'))
    .withColumn('deal_id', F.lpad(F.col('debtor_deal_id'), 16, '0'))
    .withColumn('deal_reg_tp', F.lpad(F.col('debtor_deal_reg_tp'), 5, '0'))
    .withColumn('deal_branch_cd', F.lpad(F.col('debtor_deal_branch_cd'), 5, '0'))
    .withColumn('country_cd', F.col('creditor_country_cd'))
    .withColumn('direction', F.lit('out'))
)

sdd = (
    sdd_crd
    .unionByName(sdd_deb)
    .filter(F.col('cntp_id').isNotNull())
    .withColumn('pk', F.concat_ws("_", 'trans_id', 'direction'))
    .withColumn('deal_surrogate_id', F.concat_ws("_", 'deal_id', 'deal_branch_cd', 'deal_reg_tp'))
    .select(
        'trans_id',
        'trx_type',
        'trans_agg_01_tp',
        'trans_agg_02_tp',
        'eur_trans_am',
        'trans_dt',
        'trans_status_de',
        'paym_sett_tp',
        'cntp_id',
        'deal_surrogate_id',
        'country_cd',
        'direction',
        'pk'
    )
)

sdd.show(5, truncate=False)

# print('Row count:', sdd.count())

# query_card is deprecated in favor of direct table load


tmp_card = (
    spark.read.format('bigquery')
    .option('table', f'{gcp_project}.{dataset_name}.card_after_gap')
    .load()
    .filter(
        F.col('trans_dt').between(
            f'{start_year}-{start_month}-{start_day}',
            f'{end_year}-{end_month}-{end_day}'
        ) &
        (F.col('trans_status_de') == 'PAID') &
        (F.col('paym_sett_tp') != 'OTH pay OTH')
    )
    .select(
        'trans_id',
        'trans_agg_01_tp',
        'trans_agg_02_tp',
        F.col('eur_trans_am').cast('numeric').alias('eur_trans_am'),
        'trans_dt',
        'trans_status_de',
        'paym_sett_tp',
        'debtor_cntp_id',
        'debtor_country_cd',
        'debtor_deal_id',
        'debtor_deal_branch_cd',
        'debtor_deal_reg_tp',
        'creditor_cntp_id',
        'creditor_country_cd',
        'creditor_deal_id',
        'creditor_deal_branch_cd',
        'creditor_deal_reg_tp',
        F.lit('CARD').alias('trx_type')
    )
)


# Create one row per ndg and associate the payment's direction
card_crd = (
    tmp_card
    .filter(F.col('paym_sett_tp').isin('OTH pay BAZ', 'BAZ pay BAZ'))
    .withColumn('cntp_id', F.col('creditor_cntp_id'))
    .withColumn('deal_id', F.lpad(F.col('creditor_deal_id'), 16, '0'))
    .withColumn('deal_reg_tp', F.lpad(F.col('creditor_deal_reg_tp'), 5, '0'))
    .withColumn('deal_branch_cd', F.lpad(F.col('creditor_deal_branch_cd'), 5, '0'))
    .withColumn('country_cd', F.col('debtor_country_cd'))
    .withColumn('direction', F.lit('inc'))
)

card_deb = (
    tmp_card
    .filter(F.col('paym_sett_tp').isin('BAZ pay OTH', 'BAZ pay BAZ'))
    .withColumn('cntp_id', F.col('debtor_cntp_id'))
    .withColumn('deal_id', F.lpad(F.col('debtor_deal_id'), 16, '0'))
    .withColumn('deal_reg_tp', F.lpad(F.col('debtor_deal_reg_tp'), 5, '0'))
    .withColumn('deal_branch_cd', F.lpad(F.col('debtor_deal_branch_cd'), 5, '0'))
    .withColumn('country_cd', F.col('creditor_country_cd'))
    .withColumn('direction', F.lit('out'))
)

card = (
    card_crd
    .unionByName(card_deb)
    .filter(F.col('cntp_id').isNotNull())
    .withColumn('pk', F.concat_ws("_", 'trans_id', 'direction'))
    .withColumn('deal_surrogate_id', F.concat_ws("_", 'deal_id', 'deal_branch_cd', 'deal_reg_tp'))
    .select(
        'trans_id',
        'trx_type',
        'trans_agg_01_tp',
        'trans_agg_02_tp',
        'eur_trans_am',
        'trans_dt',
        'trans_status_de',
        'paym_sett_tp',
        'cntp_id',
        'deal_surrogate_id',
        'country_cd',
        'direction',
        'pk'
    )
)


card.show(5, truncate=False)

# print('Row count:', card.count())

# query_cash is deprecated in favor of direct table load


tmp_cash = (
    spark.read.format('bigquery')
    .option('table', f'{gcp_project}.{dataset_name}.cash')
    .load()
    .filter(
        F.col('trans_dt').between(
            f'{start_year}-{start_month}-{start_day}',
            f'{end_year}-{end_month}-{end_day}'
        ) &
        (F.col('trans_status_de') == 'PAID')
    )
    .select(
        'trans_id',
        'trans_agg_01_tp',
        'trans_agg_02_tp',
        F.col('eur_trans_am').cast('numeric').alias('eur_trans_am'),
        'trans_dt',
        'trans_status_de',
        'paym_sett_tp',
        'transaction_tp',
        'debtor_cntp_id',
        'debtor_deal_id',
        'debtor_deal_branch_cd',
        'debtor_deal_reg_tp',
        'creditor_cntp_id',
        'creditor_deal_id',
        'creditor_deal_branch_cd',
        'creditor_deal_reg_tp',
        'transaction_country_nm',
        F.lit('CASH').alias('trx_type')
    )
)


# Create one row per ndg and associate the payment's direction
cash_crd = (
    tmp_cash
    .filter(F.col('transaction_tp').isin('DEPOSIT'))
    .withColumn('cntp_id', F.col('creditor_cntp_id'))
    .withColumn('deal_id', F.lpad(F.col('creditor_deal_id'), 16, '0'))
    .withColumn('deal_reg_tp', F.lpad(F.col('creditor_deal_reg_tp'), 5, '0'))
    .withColumn('deal_branch_cd', F.lpad(F.col('creditor_deal_branch_cd'), 5, '0'))
    .withColumn('country_cd', F.col('transaction_country_nm'))
    .withColumn('direction', F.lit('inc'))
)

cash_deb = (
    tmp_cash
    .filter(F.col('transaction_tp').isin('WITHDRAWAL'))
    .withColumn('cntp_id', F.col('debtor_cntp_id'))
    .withColumn('deal_id', F.lpad(F.col('debtor_deal_id'), 16, '0'))
    .withColumn('deal_reg_tp', F.lpad(F.col('debtor_deal_reg_tp'), 5, '0'))
    .withColumn('deal_branch_cd', F.lpad(F.col('debtor_deal_branch_cd'), 5, '0'))
    .withColumn('country_cd', F.col('transaction_country_nm'))
    .withColumn('direction', F.lit('out'))
)

cash = (
    cash_crd
    .unionByName(cash_deb)
    .filter(F.col('cntp_id').isNotNull())
    .withColumn('pk', F.concat_ws("_", 'trans_id', 'direction'))
    .withColumn('deal_surrogate_id', F.concat_ws("_", 'deal_id', 'deal_branch_cd', 'deal_reg_tp'))
    .select(
        'trans_id',
        'trx_type',
        'trans_agg_01_tp',
        'trans_agg_02_tp',
        'eur_trans_am',
        'trans_dt',
        'trans_status_de',
        'paym_sett_tp',
        'cntp_id',
        'deal_surrogate_id',
        'country_cd',
        'direction',
        'pk'
    )
)

cash.show(5, truncate=False)

# print('Row count:', cash.count())

trx_union = (
    bt
    .unionByName(sdd)
    .unionByName(card)
    .unionByName(cash)
  )

trx_union.show(5, truncate=False)

# print('Row count:', trx_union.count())

tmp = (
    trx_union
    .groupBy(
        'trx_type',
    )
    .agg(
        F.count('trans_id').alias('trans_id_count'),
        F.countDistinct('cntp_id').alias('cntp_id_count_dist'),
        F.countDistinct('deal_surrogate_id').alias('deal_surrogate_id_count_dist')
    )
    .orderBy('trx_type')
)
tmp.show(100000, truncate=False)









# =========================
# 1. BASE DATASET ENRICHMENT
# =========================
# Join con tabella country risk + default 'dom'
trx_enriched = (
    trx_union
    .join(
        country_risk_table,
        trx_union.country_cd == country_risk_table.country_iso2_cd,
        'left'
    )
    .select(
        trx_union['*'],
        country_risk_table['country_risk']
    )
    .na.fill({'country_risk': 'LRG'})
)

# =========================
# 2. AGG: Direction & Type
# =========================
trx_dir_type_agg = (
    trx_enriched
    .groupBy('cntp_id', 'direction', 'trx_type')
    .agg(F.sum('eur_trans_am').alias('amt'))
    .withColumn('total_direction', F.sum('amt').over(W.partitionBy('cntp_id', 'direction')))
    .withColumn('perc', F.expr('try_divide(amt, total_direction)'))
    .withColumn('pivot_key', F.concat_ws('_', 'direction', 'trx_type'))
)

trx_dir_type_pivot = (
    trx_dir_type_agg
    .groupBy('cntp_id')
    .pivot('pivot_key')
    .agg(
        F.sum('amt').alias('amt'),
        F.max('perc').alias('perc')
    )
)

# =========================
# 3. AGG: Direction & Country Risk
# =========================
trx_dir_risk_agg = (
    trx_enriched
    .groupBy('cntp_id', 'direction', 'country_risk')
    .agg(F.sum('eur_trans_am').alias('amt'))
    .withColumn('total_direction', F.sum('amt').over(W.partitionBy('cntp_id', 'direction')))
    .withColumn('perc', F.expr('try_divide(amt, total_direction)'))
    .withColumn('pivot_key', F.concat_ws('_', 'direction', 'country_risk'))
)

trx_dir_risk_pivot = (
    trx_dir_risk_agg
    .groupBy('cntp_id')
    .pivot('pivot_key')
    .agg(
        F.sum('amt').alias('amt'),
        F.max('perc').alias('perc')
    )
)

# =========================
# 4. AGG: Yearly totals
# =========================
trx_yearly = (
    trx_enriched
    .groupBy('cntp_id', 'direction')
    .agg(F.sum('eur_trans_am').alias('yearly_amt'))
    .groupBy('cntp_id')
    .pivot('direction')
    .agg(F.sum('yearly_amt'))
    .withColumnRenamed('inc', 'inc_yearly_amt')
    .withColumnRenamed('out', 'out_yearly_amt')
)

# =========================
# 5. AGG: Monthly average
# =========================
trx_monthly_avg = (
    trx_enriched
    .groupBy('cntp_id', 'direction', F.month('trans_dt').alias('month'))
    .agg(F.sum('eur_trans_am').alias('amt'))
    .groupBy('cntp_id')
    .pivot('direction')
    .agg(F.avg('amt'))
    .withColumnRenamed('inc', 'inc_monthly_avg')
    .withColumnRenamed('out', 'out_monthly_avg')
)

# =========================
# 6. AGG: Weekly average
# =========================
trx_weekly_avg = (
    trx_enriched
    .groupBy('cntp_id', 'direction', F.weekofyear('trans_dt').alias('week'))
    .agg(F.sum('eur_trans_am').alias('amt'))
    .groupBy('cntp_id')
    .pivot('direction')
    .agg(F.avg('amt'))
    .withColumnRenamed('inc', 'inc_weekly_avg')
    .withColumnRenamed('out', 'out_weekly_avg')
)

# =========================
# 7. FINAL JOIN
# =========================
trx_type_risk = (
    trx_yearly
    .join(trx_monthly_avg, 'cntp_id', 'outer')
    .join(trx_weekly_avg, 'cntp_id', 'outer')
    .join(trx_dir_type_pivot, 'cntp_id', 'outer')
    .join(trx_dir_risk_pivot, 'cntp_id', 'outer')
)

trx_type_risk.show(5, truncate=False)

print('Row count:', trx_type_risk.count())

# trx_final_dataset.select(F.col('cntp_id')).distinct().count()

# trx_final_dataset.filter(F.col('cntp_id')=='0000000000000405').count()

# query_products is deprecated in favor of direct table load


tmp_products = (
    spark.read.format('bigquery')
    .option('table', f'{gcp_project}.{dataset_name}.deal_dossier_after_gap')
    .load()
    .filter(
        (F.col('deal_open_dt') <= f'{end_year}-{end_month}-{end_day}') &
        (F.col('deal_close_dt').isNull() | (F.col('deal_close_dt') > f'{end_year}-{end_month}-{end_day}'))
    )
    .select(
        'cntp_id',
        'deal_surrogate_id',
        'deal_id',
        'deal_branch_cd',
        F.col('deal_reg_tp_cd').alias('deal_reg_tp'),
        'deal_reg_tp_de',
        'cur_iso_cd',
        'business_tp',
        'overdue_repay_in'
    )
)


# tmp_products.show(10, truncate=False)

# tmp = (
#     tmp_products
#     .groupBy(
#         'deal_reg_tp_cd',
#         'deal_reg_tp_de',
#     )
#     .agg(
#         F.countDistinct('cntp_id').alias('cntp_id_count'),
#         F.countDistinct('deal_surrogate_id').alias('deal_surrogate_id_count')
#     )
#     .orderBy('deal_reg_tp_cd')
# )
# tmp.show(100000, truncate=False)

# -------------------------
# DEFINE CODE GROUPS
# -------------------------

corporate_loan_codes = [
    # Corporate / syndicated / term loans
    204,205,206,207,208,
    214,215,216,217,218,
    224,225,226,227,228,
    240,241,242,243,244,
    245,246,247,248,249,
    250,251,252,253,254,
]

mortgage_loan_codes = [
    # Mortgage loans
    270,271,272,273,274,
    275,276,277,278,279,
    280,281,282,283,284,

    # Private mortgage
    370,371,372,374,
    375,376,377,379,
    380,381,382,383,384,
]

consumer_loan_codes = [
    # Private loans
    345,346,347,348,349,
    350,351,352,354,

    # Consumer loans
    360,361,362,364,
    365,366,367,369,
]

moratorium_loan_codes = [

    # Moratorium / special loan states
    316,317,318,
    326,327,328,
]

overdue_loan_codes = [
    # Overdue / credit exposure
    614,615,616,617
]

deposit_codes = [
    # Term deposits
    651,652,653,654,

    # Syndicated deposit-like
    292,296,298,

    # Deposit bills
    794
]

current_account_codes = [
    # Current accounts core
    570,571,572,574,575,576,578,
    588,589,
    592,593,598
]

saving_account_codes = [
    # Savings / reserve / escrow-like
    581,582,583,
    601,
    608,609,
    612,613,
    619,620
]

products = (
    tmp_products
    .withColumn(
        'product',
        F
        .when(F.col('deal_reg_tp').isin(corporate_loan_codes), 'corporate_loan')
        .when(F.col('deal_reg_tp').isin(mortgage_loan_codes), 'mortgage_loan')
        .when(F.col('deal_reg_tp').isin(consumer_loan_codes), 'consumer_loan')
        .when(F.col('deal_reg_tp').isin(moratorium_loan_codes), 'moratorium_loan')
        .when(F.col('deal_reg_tp').isin(overdue_loan_codes), 'overdue_loan')
        .when(F.col('deal_reg_tp').isin(deposit_codes), 'deposit_account')
        .when(F.col('deal_reg_tp').isin(current_account_codes), 'current_account')
        .when(F.col('deal_reg_tp').isin(saving_account_codes), 'saving_account')
        .otherwise('other_account')
    )
    .withColumn('deal_id', F.lpad(F.col('deal_id'), 16, '0'))
    .withColumn('deal_reg_tp', F.lpad(F.col('deal_reg_tp'), 5, '0'))
    .withColumn('deal_branch_cd', F.lpad(F.col('deal_branch_cd'), 5, '0'))
    .withColumn('deal_surrogate_id', F.concat_ws("_", 'deal_id', 'deal_branch_cd', 'deal_reg_tp'))
)

products.show(5, truncate=False)

trx_union.show(5, truncate = False)

# =========================
# STEP 1: Enrich transactions with product information
# =========================
trx_with_product = (
    trx_union
    .join(
        products,
        on=['cntp_id', 'deal_surrogate_id'],  # join keys
        how='left'
    )
    # drop unnecessary columns after join
    .drop('deal_reg_tp', 'deal_reg_tp_de', 'deal_branch_cd')
    .distinct()  # safeguard against duplicates
)

# =========================
# STEP 2: Aggregate by customer, direction, and product
# =========================
agg_direction_product = (
    trx_with_product
    .groupBy('cntp_id', 'direction', 'product')
    .agg(
        F.sum('eur_trans_am').alias('amt')  # total amount per group
    )
)

# =========================
# STEP 3: Compute totals per direction and percentages
# =========================
window_by_cntp_direction = W.partitionBy('cntp_id', 'direction')

direction_metrics = (
    agg_direction_product
    # compute total amount per direction (e.g. incoming/outgoing)
    .withColumn(
        'total_direction',
        F.sum('amt').over(window_by_cntp_direction)
    )
    # calculate percentage contribution of each product
    .withColumn(
        'perc',
        F.expr('try_divide(amt, total_direction)')
    )
    # build pivot key (e.g. incoming_loan)
    .withColumn(
        'pivot_key',
        F.concat_ws('_', 'direction', 'product')
    )
)


# =========================
# STEP 4: Final pivot per customer
# =========================
trx_product = (
    direction_metrics
    .groupBy('cntp_id')
    .pivot('pivot_key')
    .agg(
        F.sum('amt').alias('amt'),   # aggregated amount
        F.max('perc').alias('perc')  # percentage (single value per group)
    )
    # remove unwanted columns
    .drop(
        'incoming_amt',
        'incoming_percent_prd',
        'outgoing_amt',
        'outgoing_percent_prd',
    )
    # replace nulls with 0
    .na.fill(0)
)

trx_product.show(5, truncate= False)

trx_final = (
    trx_type_risk
    .join(trx_product, 'cntp_id', 'outer')
    .join(perimeter, 'cntp_id', 'inner')
)

print(f"Rows: {trx_final.count()}, Columns: {len(trx_final.columns)}")

trx_final.show(5, truncate= False)

# Align decimal column precisions and scales with the destination BigQuery table to prevent write schema mismatch errors
#
# NOTE FOR TRAINEES:
# When running this cell/script for the first time, you will see a warning:
#   "⚠ Target table schema alignment skipped or failed (table may not exist yet): ..."
# This is completely normal and can be safely ignored. It occurs because the target
# table doesn't exist yet in BigQuery on your first run. The code safely catches this,
# skips the alignment, and then successfully creates and writes the table below.
# If you execute this a second time, the warning will disappear.
from pyspark.sql.types import DecimalType

try:
    dest_table = f'{gcp_project}.{dataset_name}.CZ_segm_trx_final_01'
    target_schema = spark.read.format('bigquery').option('table', dest_table).load().schema
    for field in target_schema:
        if field.name in trx_final.columns and isinstance(field.dataType, DecimalType):
            trx_final = trx_final.withColumn(field.name, F.col(field.name).cast(field.dataType))
    print('✓ Automatically aligned DataFrame decimal precision and scale with BigQuery target table.')
except Exception as e:
    print('⚠ Target table schema alignment skipped or failed (table may not exist yet):', e)

# save dataset
trx_final.write.format('bigquery') \
    .option('writeMethod', 'direct') \
    .mode('overwrite') \
    .save(f'{gcp_project}.{dataset_name}.CZ_segm_trx_final_01')




