import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from Spreadsheet_params import respfit_neshap_columns, hazcom_ppe_columns
from Data_Operations import get_data_from_spreadsheet
from Connection_paramaters import connection_params
import pandas as pd
from dotenv import load_dotenv
from datetime import date


#Load environment variables
load_dotenv()

def EHS_spreadsheets_to_snowflake(RF_NESHAP_file_name, RF_NESHAP_sheet_name, HCPPE_file_name, HCPPE_sheet_name, acc_date):
    connection = snowflake.connector.connect(
        user=connection_params['user'],
        password=connection_params['password'],
        account=connection_params['account'],
        warehouse=connection_params['warehouse'],
        database='DEPT_FINANCE',
        schema='PUBLIC',
        authenticator='snowflake',
    )
    
    #Get data from spreadsheets
    respfit_neshap_df = get_data_from_spreadsheet(RF_NESHAP_file_name, RF_NESHAP_sheet_name)
    hazcom_ppe_df = get_data_from_spreadsheet(HCPPE_file_name, HCPPE_sheet_name)

    #Select desired columns from original spreadsheet
    respfit_neshap_df = respfit_neshap_df[['ADP #', 'Workday #', 'Status', 'Region', 'Zone', 'Last Name', 'First Name', 'Fit Test Compliant ?', 'NESHAP Compliant?']] 
    hazcom_ppe_df = hazcom_ppe_df[['ADP #', 'Workday #', 'Status', 'Region', 'Zone', 'Last Name', 'First Name', 'HazCom Compliant', 'PPE Compliant']]

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

    #Add account period stamp
    consolidated_df['Account_period'] = acc_date

    #Set all columns to upper-case
    consolidated_df.columns = consolidated_df.columns.str.upper()

    #Write to table in Snowflake
    success, nchunks, nrows, _ = write_pandas(connection, consolidated_df, "ALL_EHS_DATA")

    return (success, nchunks, nrows)

#March Data
#EHS_spreadsheets_to_snowflake('March_24_RespFit_NESHAP.xlsx', 'FT.6H 4-01-24', 'March_24_HazCom_PPE.xlsx', 'HC.PPE 4-01-24', date(2024, 3, 1))

#April Data
EHS_spreadsheets_to_snowflake('April_24_RespFit_NESHAP.xlsx', 'FT.6H 5-01-24', 'April_24_HazCom_PPE.xlsx', 'HC.PPE 5-01-24', acc_date=date(2024, 4, 1))

#May Data
