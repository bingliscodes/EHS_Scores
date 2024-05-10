from snowflake.connector.pandas_tools import write_pandas
from Spreadsheet_params import respfit_neshap_columns, hazcom_ppe_columns
from Data_Operations import get_data_from_spreadsheet
from Connection_paramaters import connection
import pandas as pd
from dotenv import load_dotenv
from datetime import date

# Load environment variables
load_dotenv()

respfit_neshap_table_name = "RESPFIT_NESHAP"
hazcom_ppe_table_name = "HAZCOM_PPE"

respfit_neshap_df = get_data_from_spreadsheet('March_24_RespFit_NESHAP.xlsx', 'FT.6H 4-01-24')
hazcom_ppe_df = get_data_from_spreadsheet('March_24_HazCom_PPE.xlsx', 'HC.PPE 4-01-24')
#Select desired columns
respfit_neshap_df = respfit_neshap_df[['ADP #', 'Workday #', 'Status', 'Region', 'Zone', 'Last Name', 'First Name', 'Fit Test Compliant ?', 'NESHAP Compliant?']] 
hazcom_ppe_df = hazcom_ppe_df[['ADP #', 'Workday #', 'Status', 'Region', 'Zone', 'Last Name', 'First Name', 'HazCom Compliant', 'PPE Compliant']]

#Add account period column (placeholder today)
respfit_neshap_df['Account_period'] = date.today()
hazcom_ppe_df['Account_period'] = date.today()

#Rename columns to match snowflake table
respfit_neshap_df.columns = respfit_neshap_columns
hazcom_ppe_df.columns = hazcom_ppe_columns                        

#Remove all rows that are not "Active"
respfit_neshap_filtered = respfit_neshap_df.loc[respfit_neshap_df['STATUS'] == 'Active']
hazcom_ppe_filtered = hazcom_ppe_df.loc[hazcom_ppe_df['STATUS'] == 'Active']

#Replace Compliant and NonCompliant with 1 and 0 respectively, and replace region names to match department tables in Snowflake
replace_dict = {
    'Compliant': 1,
    'NonCompliant': 0,
    'SE': 'Southeast',
    'NE': 'Northeast',
    'MDW': 'MidWest',
    'COR/FOP': 'Corporate'
}
respfit_neshap_filtered.replace(replace_dict, inplace=True)
hazcom_ppe_filtered.replace(replace_dict, inplace=True)

#Merge the two data frames
consolidated_df = pd.merge(respfit_neshap_filtered, hazcom_ppe_filtered, 
                           on=['ADP_NUMBER', 'WORKDAY_NUMBER', 'REGION', 'ZONE', 'LAST_NAME', 'FIRST_NAME'],
                           how='outer',
                           suffixes=('_respfit_neshap', '_hazcom_ppe')
                    )
print(consolidated_df)
#TODO: Is there a way to get the data all in one data frame for ease of use?

#Write rows to appropriate tables
#success, nchunks, nrows, _ = write_pandas(connection, respfit_neshap_df, respfit_neshap_table_name)
#success, nchunks, nrows, _ = write_pandas(connection, hazcom_ppe_df, hazcom_ppe_table_name)
                    

