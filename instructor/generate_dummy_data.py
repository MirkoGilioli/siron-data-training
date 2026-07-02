import os
import csv

# Create dedicated directory for dummy data
output_dir = 'dummy_data'
os.makedirs(output_dir, exist_ok=True)

print(f"Creating dummy data directory: {output_dir}")

# 1. COUNTRY RISK TABLE
country_risk_headers = [
    'country_iso2_cd', 'country_iso_cd', 'country_iso3_cd', 'country_de',
    'country_risk_cd', 'country_risk_de', 'country_offshore_in', 'terrorist_country_in',
    'high_risk_eu_third_country_in', 'sanctioned_country_in', 'sanctioned_country_de',
    'sanctioned_country_detail', 'country_risk'
]

country_risk_rows = [
    ['QZ', '0', 'NA', 'Stateless', 'H', 'High', 'false', 'false', 'false', 'false', 'NA', 'Not applicable', 'HRG'],
    ['XX', '0', 'NA', 'Stateless', 'H', 'High', 'false', 'false', 'false', 'false', 'NA', 'Not applicable', 'HRG'],
    ['SB', '90', 'SLB', 'Solomon Islands', 'H', 'High', 'false', 'false', 'false', 'false', 'NA', 'Not applicable', 'HRG'],
    ['BI', '108', 'BDI', 'Burundi', 'H', 'High', 'false', 'false', 'false', 'false', 'NA', 'Not applicable', 'HRG'],
    ['BY', '112', 'BLR', 'Belarus', 'H', 'High', 'false', 'false', 'false', 'true', 'TE', 'Total embargo', 'HRG'],
    ['CZ', '203', 'CZE', 'Czech Republic', 'L', 'Low', 'false', 'false', 'false', 'false', 'NA', 'Not applicable', 'LRG'],
    ['PL', '616', 'POL', 'Poland', 'M', 'Medium', 'false', 'false', 'false', 'false', 'NA', 'Not applicable', 'MRG'],
    ['UA', '804', 'UKR', 'Ukraine', 'VH', 'Very High', 'false', 'false', 'false', 'false', 'NA', 'Not applicable', 'VHRG'],
]

with open(os.path.join(output_dir, 'CZ_segm_country_risk_table.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(country_risk_headers)
    writer.writerows(country_risk_rows)

print("Created CZ_segm_country_risk_table.csv")


# 2. PERIMETER
perimeter_headers = ['cntp_id', 'segment', 'aml_business_segment', 'aml_business_segment_de']

perimeter_rows = [
    ['0000000000000405', 'CORP', 'Z14B', 'Small or Medium CORP - Seasoned Customers'],
    ['0000000000001458', 'CIB', 'Z15A', 'Large CORB CIB - Seasoned customers'],
    ['0000000000002428', 'CORP', 'Z14B', 'Small or Medium CORP - Seasoned Customers'],
    ['0000000000003688', 'RET_IND', 'Z11B', 'Individual Low Income Over 29 - Seasoned Customers'],
    ['0000000000004381', 'CORP', 'Z14B', 'Small or Medium CORP - Seasoned Customers'],
    ['0000000000004492', 'RET_IND', 'Z11B', 'Individual Low Income Over 29 - Seasoned Customers'],
]

with open(os.path.join(output_dir, 'CZ_segm_perimeter.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(perimeter_headers)
    writer.writerows(perimeter_rows)

print("Created CZ_segm_perimeter.csv")


# Helper function to generate standardized transaction row
def make_tx_row(trans_id, trans_agg_01, trans_agg_02, eur_trans_am, trans_dt, status, settlement, 
                deb_cntp, deb_country, deb_deal, deb_branch, deb_reg, 
                cred_cntp, cred_country, cred_deal, cred_branch, cred_reg):
    return [
        trans_id, trans_agg_01, trans_agg_02, eur_trans_am, trans_dt, status, settlement,
        deb_cntp, deb_country, deb_deal, deb_branch, deb_reg,
        cred_cntp, cred_country, cred_deal, cred_branch, cred_reg
    ]

# Common Headers for BT, Card, SDD (excluding deal_id for SDD)
tx_headers = [
    'trans_id', 'trans_agg_01_tp', 'trans_agg_02_tp', 'eur_trans_am', 'trans_dt', 'trans_status_de', 'paym_sett_tp',
    'debtor_cntp_id', 'debtor_country_cd', 'debtor_deal_id', 'debtor_deal_branch_cd', 'debtor_deal_reg_tp',
    'creditor_cntp_id', 'creditor_country_cd', 'creditor_deal_id', 'creditor_deal_branch_cd', 'creditor_deal_reg_tp'
]


# 3. BANK TRANSFER
bt_rows = [
    # 0405
    make_tx_row('BT0405_INC', 'JGF', 'DOMESTIC BANK TRANSFER', '2537104.38', '2025-02-26', 'PAID', 'OTH pay BAZ',
                '99999999', 'CZ', '999999', '99999', '99999',
                '0000000000000405', 'CZ', '2112168640', '0SW06', '592'),
    make_tx_row('BT0405_OUT', 'JGF', 'DOMESTIC BANK TRANSFER', '1524695.26', '2025-03-15', 'PAID', 'BAZ pay OTH',
                '0000000000000405', 'CZ', '2112168640', '0SW06', '592',
                '99999999', 'CZ', '999999', '99999', '99999'),
    
    # 1458
    make_tx_row('BT1458_INC', 'JGF', 'DOMESTIC BANK TRANSFER', '570590448.31', '2025-01-10', 'PAID', 'OTH pay BAZ',
                '99999999', 'CZ', '999999', '99999', '99999',
                '0000000000001458', 'CZ', '1059024028', '0ICM1', '592'),
    make_tx_row('BT1458_OUT1', 'G12', 'CROSS BOARDER WIRE TRANSFER', '17284312.27', '2025-04-12', 'PAID', 'BAZ pay OTH',
                '0000000000001458', 'CZ', '4444444444', '0SW06', '244',
                '99999999', 'BY', '999999', '99999', '99999'),
    make_tx_row('BT1458_OUT2', 'JGF', 'DOMESTIC BANK TRANSFER', '517036936.61', '2025-06-15', 'PAID', 'BAZ pay OTH',
                '0000000000001458', 'CZ', '4444444444', '0SW06', '244',
                '99999999', 'CZ', '999999', '99999', '99999'),
    make_tx_row('BT1458_OUT3', 'G12', 'CROSS BOARDER WIRE TRANSFER', '1755.00', '2025-07-22', 'PAID', 'BAZ pay OTH',
                '0000000000001458', 'CZ', '4444444444', '0SW06', '244',
                '99999999', 'PL', '999999', '99999', '99999'),
    make_tx_row('BT1458_OUT4', 'G12', 'CROSS BOARDER WIRE TRANSFER', '269417603.38', '2025-11-20', 'PAID', 'BAZ pay OTH',
                '0000000000001458', 'CZ', '4444444444', '0SW06', '244',
                '99999999', 'UA', '999999', '99999', '99999'),
    
    # 2428
    make_tx_row('BT2428_INC', 'JGF', 'DOMESTIC BANK TRANSFER', '837229.34', '2025-05-10', 'PAID', 'OTH pay BAZ',
                '99999999', 'CZ', '999999', '99999', '99999',
                '0000000000002428', 'CZ', '6861032001', '0M003', '593'),
    make_tx_row('BT2428_OUT', 'JGF', 'DOMESTIC BANK TRANSFER', '234341.84', '2025-06-12', 'PAID', 'BAZ pay OTH',
                '0000000000002428', 'CZ', '6861032001', '0M003', '593',
                '99999999', 'CZ', '999999', '99999', '99999'),
    
    # 3688
    make_tx_row('BT3688_INC', 'JGF', 'DOMESTIC BANK TRANSFER', '18535.93', '2025-04-08', 'PAID', 'OTH pay BAZ',
                '99999999', 'CZ', '999999', '99999', '99999',
                '0000000000003688', 'CZ', '2102063028', '0MC02', '592'),
    make_tx_row('BT3688_OUT', 'JGF', 'DOMESTIC BANK TRANSFER', '10155.86', '2025-04-18', 'PAID', 'BAZ pay OTH',
                '0000000000003688', 'CZ', '2102063028', '0MC02', '592',
                '99999999', 'CZ', '999999', '99999', '99999'),
    
    # 4381
    make_tx_row('BT4381_INC', 'JGF', 'DOMESTIC BANK TRANSFER', '7288968.08', '2025-11-27', 'PAID', 'OTH pay BAZ',
                '99999999', 'CZ', '999999', '99999', '99999',
                '0000000000004381', 'CZ', '2108545335', '0HEOF', '601'),
    make_tx_row('BT4381_OUT', 'JGF', 'DOMESTIC BANK TRANSFER', '7593866.77', '2025-12-30', 'PAID', 'BAZ pay OTH',
                '0000000000004381', 'CZ', '2108545335', '0HEOF', '601',
                '99999999', 'CZ', '999999', '99999', '99999'),

    # 4492
    make_tx_row('BT4492_INC', 'JGF', 'DOMESTIC BANK TRANSFER', '83150.71', '2025-11-27', 'PAID', 'OTH pay BAZ',
                '99999999', 'CZ', '999999', '99999', '99999',
                '0000000000004492', 'CZ', '2106578539', '0HEOF', '592'),
    make_tx_row('BT4492_OUT', 'JGF', 'DOMESTIC BANK TRANSFER', '96478.45', '2025-12-30', 'PAID', 'BAZ pay OTH',
                '0000000000004492', 'CZ', '2106578539', '0HEOF', '592',
                '99999999', 'CZ', '999999', '99999', '99999'),
]

with open(os.path.join(output_dir, 'bank_transfer_after_gap.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(tx_headers)
    writer.writerows(bt_rows)

print("Created bank_transfer_after_gap.csv")


# 4. CARD
card_rows = [
    # 0405
    make_tx_row('CR0405_INC', 'JB0', 'DEBIT CARD', '83.62', '2025-05-25', 'PAID', 'OTH pay BAZ',
                '99999999', 'CZ', '999999', '99999', '99999',
                '0000000000000405', 'CZ', '2112168640', '0SW06', '592'),
    make_tx_row('CR0405_OUT', 'JB0', 'DEBIT CARD', '33164.62', '2025-08-11', 'PAID', 'BAZ pay OTH',
                '0000000000000405', 'CZ', '2112168640', '0SW06', '592',
                '99999999', 'CZ', '999999', '99999', '99999'),
    
    # 2428
    make_tx_row('CR2428_INC', 'JB0', 'DEBIT CARD', '492.89', '2025-11-11', 'PAID', 'OTH pay BAZ',
                '99999999', 'CZ', '999999', '99999', '99999',
                '0000000000002428', 'CZ', '6861032001', '0M003', '593'),
    make_tx_row('CR2428_OUT', 'JB0', 'DEBIT CARD', '118014.63', '2025-12-31', 'PAID', 'BAZ pay OTH',
                '0000000000002428', 'CZ', '6861032001', '0M003', '593',
                '99999999', 'CZ', '999999', '99999', '99999'),
    
    # 3688
    make_tx_row('CR3688_OUT', 'JB0', 'DEBIT CARD', '6224.04', '2025-10-07', 'PAID', 'BAZ pay OTH',
                '0000000000003688', 'CZ', '2102063028', '0MC02', '592',
                '99999999', 'CZ', '999999', '99999', '99999'),
    
    # 4381
    make_tx_row('CR4381_INC', 'JB0', 'DEBIT CARD', '109.35', '2025-10-07', 'PAID', 'OTH pay BAZ',
                '99999999', 'CZ', '999999', '99999', '99999',
                '0000000000004381', 'CZ', '2108545335', '0HEOF', '601'),
    make_tx_row('CR4381_OUT', 'JB0', 'DEBIT CARD', '26757.11', '2025-11-11', 'PAID', 'BAZ pay OTH',
                '0000000000004381', 'CZ', '2108545335', '0HEOF', '601',
                '99999999', 'CZ', '999999', '99999', '99999'),

    # 4492
    make_tx_row('CR4492_OUT', 'JB0', 'DEBIT CARD', '81597.27', '2025-11-11', 'PAID', 'BAZ pay OTH',
                '0000000000004492', 'CZ', '2106578539', '0HEOF', '592',
                '99999999', 'CZ', '999999', '99999', '99999'),
]

with open(os.path.join(output_dir, 'card_after_gap.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(tx_headers)
    writer.writerows(card_rows)

print("Created card_after_gap.csv")


# 5. SDD (SEPA Direct Debit)
sdd_headers = tx_headers + ['deal_id']

sdd_rows = [
    # 2428 SDD
    ['SD2428_OUT', 'JGF', 'DOMESTIC BANK TRANSFER', '6795.39', '2025-03-28', 'PAID', 'BAZ pay OTH',
     '0000000000002428', 'CZ', '6861032001', '0M003', '593',
     '99999999', 'CZ', '999999', '99999', '99999', '6861032001'],
]

with open(os.path.join(output_dir, 'sdd_after_gap.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(sdd_headers)
    writer.writerows(sdd_rows)

print("Created sdd_after_gap.csv")


# 6. CASH
cash_headers = [
    'trans_id', 'trans_agg_01_tp', 'trans_agg_02_tp', 'eur_trans_am', 'trans_dt', 'trans_status_de', 'paym_sett_tp',
    'transaction_tp',
    'debtor_cntp_id', 'debtor_deal_id', 'debtor_deal_branch_cd', 'debtor_deal_reg_tp',
    'creditor_cntp_id', 'creditor_deal_id', 'creditor_deal_branch_cd', 'creditor_deal_reg_tp',
    'transaction_country_nm'
]

cash_rows = [
    # 2428 WITHDRAWAL
    ['CS2428_OUT', 'JKH', 'CASH ATM', '302.04', '2025-01-22', 'PAID', 'NULL',
     'WITHDRAWAL',
     '0000000000002428', '6861032001', '0M003', '593',
     '99999999', '999999', '99999', '99999',
     'CZ'],
    
    # 4492 DEPOSIT
    ['CS4492_INC', 'JKH', 'CASH ATM', '300.56', '2025-01-22', 'PAID', 'NULL',
     'DEPOSIT',
     '99999999', '999999', '99999', '99999',
     '0000000000004492', '2106578539', '0HEOF', '592',
     'CZ'],
]

with open(os.path.join(output_dir, 'cash.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(cash_headers)
    writer.writerows(cash_rows)

print("Created cash.csv")


# 7. DEAL DOSSIER (Query TRX PRODUCTS expects this schema)
deal_dossier_headers = [
    'cntp_id', 'deal_surrogate_id', 'deal_id', 'deal_branch_cd', 'deal_reg_tp_cd', 'deal_reg_tp_de',
    'cur_iso_cd', 'business_tp', 'overdue_repay_in', 'deal_open_dt', 'deal_close_dt'
]

deal_dossier_rows = [
    # 0405 deals
    ['0000000000000405', '0000002112168640_0SW06_00592', '2112168640', '0SW06', '592', 'CURRENT ACCOUNTS - CORP', 'CZK', '24', '', '2020-01-01', ''],
    
    # 1458 deals
    ['0000000000001458', '0000001059024028_0ICM1_00592', '1059024028', '0ICM1', '592', 'CURRENT ACCOUNTS - CIB', 'CZK', '24', '', '2019-05-15', ''],
    ['0000000000001458', '0000004444444444_0SW06_00244', '4444444444', 'SW06', '244', 'TERM LOANS', 'EUR', '24', '', '2022-01-01', ''],
    
    # 2428 deals
    ['0000000000002428', '0000006861032001_0M003_00593', '6861032001', '0M003', '593', 'CURRENT ACCOUNTS - CORP', 'CZK', '24', '', '2023-11-10', ''],
    
    # 3688 deals
    ['0000000000003688', '0000002102063028_0MC02_00592', '2102063028', '0MC02', '592', 'CURRENT ACCOUNTS - RET', 'CZK', '14', '', '2021-08-20', ''],
    
    # 4381 deals
    ['0000000000004381', '0000002108545335_0HEOF_00601', '2108545335', '0HEOF', '601', 'SAVINGS ACCOUNTS', 'CZK', '24', '', '2022-03-12', ''],

    # 4492 deals
    ['0000000000004492', '0000002106578539_0HEOF_00592', '2106578539', '0HEOF', '592', 'CURRENT ACCOUNTS - RET', 'CZK', '14', '', '2021-08-20', ''],
]

with open(os.path.join(output_dir, 'deal_dossier_after_gap.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(deal_dossier_headers)
    writer.writerows(deal_dossier_rows)

print("Created deal_dossier_after_gap.csv")

print("\n🎉 Success! All dummy CSV datasets generated successfully inside the 'dummy_data/' folder.")
