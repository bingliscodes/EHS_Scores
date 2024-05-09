import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime, date

# Load environment variables
load_dotenv()

# Get the absolute path to the directory where the script is running
columns = {
    "ADP_number": str,
    "Workday_number": str,
    "Status": str,
    "Region": str,
    "Zone": str,
    "Last_name": str,
    "First_name": str,
    "FitTest_Compliant": str,
    "NESHAP_Compliant": str,
    "Account_period": datetime
}
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
data_dir = os.path.join(parent_dir, 'Data')
march_hazcom_ppe = os.path.join(data_dir, 'March_24_RespFit_NESHAP.xlsx')
raw_file_path = os.path.join(script_dir, 'test_table.csv')
# Replace backslashes with forward slashes
file_path = march_hazcom_ppe.replace('\\', '/')

#read in the data
df = pd.read_excel(file_path,sheet_name='FT.6H 4-01-24') 

#Select desired columns
df = df[['ADP #', 'Worday #', 'Status', 'Region', 'Zone', 'Last Name', 'First Name', 'Fit Test Compliant ?', 'NESHAP Compliant?']] 
#add account period column (placeholder today)
df['Account_period'] = date.today()

#rename columns to match snowflake table
df.columns = columns
df.columns = df.columns.str.upper()  # Ensure column names are uppercase                              

table_name = "respfit_neshap"


#Connection parameters from environment variables
connection_params = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('DW_PASSWORD'),
    'account': os.getenv('DW_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

#Create a connection
connection = snowflake.connector.connect(
    user=connection_params['user'],
    password=connection_params['password'],
    account=connection_params['account'],
    warehouse=connection_params['warehouse'],
    database='DEPT_FINANCE',
    schema='PUBLIC',
    authenticator='snowflake',
)

success, nchunks, nrows, _ = write_pandas(connection, df, "RESPFIT_NESHAP")
                    

