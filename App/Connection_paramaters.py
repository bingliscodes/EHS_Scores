import snowflake.connector
from dotenv import load_dotenv
import os
from session_commands import *

# Load environment variables
load_dotenv()

# Get the absolute path to the directory where the script is running
script_dir = os.path.dirname(os.path.abspath(__file__))
raw_file_path = os.path.join(script_dir, 'test_table.csv')
# Replace backslashes with forward slashes
file_path = raw_file_path.replace('\\', '/')
                                                                        
stage_name = "my_csv_stage"
table_name = "DEPT_FINANCE.PUBLIC.test_table"


#Connection parameters from environment variables
#Connection parameters from environment variables
connection_params = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('DW_PASSWORD'),
    'account': os.getenv('DW_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

#Function to execute session commands
def execute_session_command(connection, command):
    try:
        cursor = connection.cursor()
        cursor.execute(command)
        cursor.close()
        print("Session command executed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

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




#Close connection
connection.close()

if __name__ == "__main__":
    print("Executing connection_parameters")
    #Create file format
    print("Executing SQL command:", create_file_format_command)
    execute_session_command(connection, create_file_format_command)

    #Create a stage
    print("Executing SQL command:", create_stage_command)
    execute_session_command(connection, create_stage_command)

    #Upload the file to the stage
    print("Executing SQL command:", put_command)
    execute_session_command(connection, put_command)

    #Copy data from the stage to the table
    print("Executing SQL command:", copy_into_table_command)
    execute_session_command(connection, copy_into_table_command)