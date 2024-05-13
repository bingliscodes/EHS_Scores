import snowflake.connector
from Connection_paramaters import connection_params, execute_query_and_load_data
from session_commands import get_EHS_data
from datetime import date

def get_df(query):
    
    connection = snowflake.connector.connect(
        user=connection_params['user'],
        password=connection_params['password'],
        account=connection_params['account'],
        warehouse=connection_params['warehouse'],
        database='DEPT_FINANCE',
        schema='PUBLIC',
        authenticator='snowflake',
    )
    return execute_query_and_load_data(connection, query)

def get_denominators(div, acc_period):
    df = get_df(get_EHS_data)


    #filter for only actives
    df_filtered = df.loc[(df['STATUS_RESPFIT_NESHAP'] == "Active") | (df['STATUS_HAZCOM_PPE'] == "Active")]
    df_filtered = df_filtered.loc[(df['REGION'] == div) & (df['ACCOUNT_PERIOD'] == acc_period)]

    #Get the respective data frames
    PPE_HazCom_df = df_filtered.loc[
        (df['HAZCOM_COMPLIANT'] == 1) | (df['HAZCOM_COMPLIANT'] == 0) | 
        (df['PPE_COMPLIANT'] == 1) | (df['PPE_COMPLIANT'] == 0)
    ]
    #RespFit_NESHAP_df = df.loc[(~df['RESPFIT_COMPLIANT'].isnull().values.any() | ~df['NESHAP_COMPLIANT'].isnull().values.any())]

    print(len(PPE_HazCom_df.index))
    return 

d
NE_denom = get_denominators("Northeast", date(2024, 3, 1))