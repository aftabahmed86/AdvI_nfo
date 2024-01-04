# AdvI_nfo
# Advances Information is merged and filtered to reduce the rows from more than 500,000 to around 30,000 and unnecessary columns are removed while required columns are added
import pandas as pd
import numpy as np
import dask.dataframe as dd
import pyarrow as pa
from datetime import datetime

python_account_codes_file = 'Python Account Codes.xlsx'

file_name = 'AdvancesInformation30092023 txt file.txt'
bal_cutoff = 0

cols_to_keep = ["Region", "Branch Code", "Branch Name", "Account Type", "Account Type Desc", "Account No.", "Title", "CIF No", 
"Sanc. Date", "Disb. Date", "Expiry Date", "Sanction Amount", "Balance", "classification", "NTN", "SAPGLCODE"]

CW_Adv_Cols = pd.read_excel(python_account_codes_file, sheet_name='CW Adv Cols',)
paec = pd.read_excel(python_account_codes_file, sheet_name='PAEC')

df = pd.read_csv(file_name, low_memory=False)
current_col_names_list = {"current_col_names" : df.columns}
Current_CW_Adv_Cols = pd.DataFrame(current_col_names_list)
old_headers = Current_CW_Adv_Cols.merge(CW_Adv_Cols, how='left', left_on=['current_col_names'], right_on=['new_col_names'])['col_names'].values.tolist()

df = pd.read_csv(file_name, low_memory=False, skiprows=1, header=None, names=old_headers, usecols=cols_to_keep)
df = df[df['Balance'] >= bal_cutoff]
df = df.sort_values(by=['Balance'], ascending=False)



df = df.merge(paec, how='left', left_on='CIF No', right_on='CIF No', suffixes=('','_right'))
df.fillna('NaN', inplace=True)
# Replaced Values in Columns
df.loc[df['Region_right'] != 'NaN', 'Region'] = df['Region_right']
df.loc[df['Branch Code_right'] != 'NaN', 'Branch Code'] = df['Branch Code_right']
df.loc[df['Branch Name_right'] != 'NaN', 'Branch Name'] = df['Branch Name_right']

df['Branch Code'] = df['Branch Code'].astype(int)

df['Region All'] = np.where(df['Region'] == 'Corporate Banking', df['Branch Code'], df['Region'])
cols_to_keep.append('Region All')

# df['Sum'] = (df['Branch Code'] + df['Account Type'])
# cols_to_keep.extend(['Region All', 'Sum'])


df = df.filter( items= cols_to_keep, axis=1)



# # # # Replacing PAEC Region, Branch Code and Branch Name based on CIF
# # # paec_cif = 12949037
# # # # Replaced Values in Columns
# # # df['Region'] = df.apply(lambda row: 'Corporate Banking' if row['CIF No'] == paec_cif else row['Region'], axis=1)
# # # df['Branch Code'] = df.apply(lambda row: 1858 if row['CIF No'] == paec_cif else row['Branch Code'], axis=1)
# # # df['Branch Name'] = df.apply(lambda row: 'Bank Road Branch Rwp' if row['CIF No'] == paec_cif else row['Branch Name'], axis=1)



df = df[(df['Branch Code'] != 0) & (df['Balance'] > 0)]
df = df[(~df['Account Type Desc'].str.contains('Bank')) & (~df['Account Type Desc'].str.contains('GBM')) & (~df['Region'].str.contains('Head Office'))]
df['Test'] = df.apply(lambda row: 'Yes' if row['Region'] == "Corporate Banking" and row['Balance'] > 1  else 'No', axis=1)

# Conditionally update values in Column 'Test' based on 'Balance'
df.loc[df['Balance'] >= 1000000, 'Test'] = 'Yes'

df = df[(df['Test'] == 'Yes')]
df
