import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

def get_data_from_spreadsheet(file_name, sheetName):
    
    #Get the absolute path to the directory where the script is running
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(parent_dir, 'Data')
    
    #Append the file name
    full_path = os.path.join(data_dir, file_name)

    #Replace backslashes with forward slashes
    full_path = full_path.replace('\\', '/')

    #Read in the data
    df = pd.read_excel(full_path, sheet_name=sheetName, converters={'ADP #':str, 'Workday #':str})
    
    return df
