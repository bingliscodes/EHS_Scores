import os

script_dir = os.path.dirname(os.path.abspath(__file__))
script_dir = os.path.dirname(os.path.abspath(__file__))
raw_file_path = os.path.join(script_dir, 'test_table.csv')
# Replace backslashes with forward slashes
file_path = raw_file_path.replace('\\', '/')

# Now the file_path can be safely included in an f-string without syntax issues


stage_name = "my_csv_stage"

create_file_format_command = """
    CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';
"""

create_stage_command = """
    CREATE OR REPLACE STAGE my_csv_stage
    FILE_FORMAT = my_csv_format;
"""
put_command = f"PUT file://{file_path} @{stage_name} AUTO_COMPRESS=TRUE;"

copy_into_table_command = f"""
    COPY INTO DEPT_FINANCE.PUBLIC.TEST_TABLE
    FROM @{stage_name}/test_table.csv
    FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');
"""

get_EHS_data = """
    SELECT * FROM DEPT_FINANCE.PUBLIC.ALL_EHS_SCORES_WITH_DISTRICT
"""





