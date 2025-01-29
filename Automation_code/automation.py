import logging
import time
start_time = time.time()

global BytesIO
global ssh_exception
global text, String, Float, Date, select
global pytz
global datetime
import pandas as pd
import numpy as np
import paramiko
from paramiko import ssh_exception
import io
import os
import sys
import pyodbc
from sqlalchemy import create_engine, inspect, text, String, Date, Float, select
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from io import BytesIO
import importlib.util
from datetime import datetime
import json
import pytz

from Core.users.computations.db_credential_encrytion import decrypt_existing_db_credentials
from Core.users.computations.db_centralised_function import default_engine_creator  
global data_workbook
global map_dtype
global convert_all_columns_to_str
global convert_date_columns_to_datetime
global sheet_to_database
global sftp_client
global mapping
global types
global process_sheet, configs_to_table_map, source_to_table_map, technical_masters_to_table_map
global table_name_prefix
global conditional_update_configs, conditional_update_source
validation_error = Data2.copy()
# validation_error = pd.DataFrame()

global PLATFORM_FILE_PATH
from config.settings.base import PLATFORM_FILE_PATH


# Ensure the directory exists
log_directory = fr'{PLATFORM_FILE_PATH}/alm_data'

# Configure logging to write to a file
log_file = os.path.join(log_directory, 'logs_configuration_automation.txt')
logging.basicConfig(filename=log_file, level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

logging.warning("Started automation")

import sys
  
table_name_prefix = "users_"

# Connection to DB starts  
try:
    # json_path = f'{PLATFORM_FILE_PATH}user_databases.json'
    json_path = f'{PLATFORM_FILE_PATH}user_databases.json'
    with open(json_path, 'r') as file:
        db_json = json.load(file)
    
    db_data = db_json[Database_Name]
    
    if db_data is None:
        # Terminate if DB not found in system
        logging.warning("Database not found in system. Terminating the execution")
        sys.exit()

    global db_type
    db_type = db_data["db_type"]
    if db_type not in ["MSSQL", "PostgreSQL", "Oracle"]:
        raise Exception("db type should be ['MSSQL', 'PostgreSQL', 'Oracle']")
except Exception as e:
    logging.warning(f"Error in Db Connection:{e}")
    raise Exception(f"Error in Db Connection:{e}")

try:
    try:
        db_server, port, db_name, username, password = (
            decrypt_existing_db_credentials(
                db_data["server"],
                db_data["port"],
                db_data["db_name"],
                db_data["username"],
                db_data["password"],
                db_data["connection_code"],
            )
        )

        global schema
        schema = db_data.get("schema", "")
        db_data["server"] = db_server
        db_data["port"] = port
        db_data["db_name"] = db_name
        db_data["username"] = username
        db_data["password"] = password
        
        global engine, inspector, connection
        if db_type in ["MSSQL", "Oracle"]:
            engine = default_engine_creator(db_type=db_type, connection_details=db_data, return_pg_conn=False)
        else:
            connection_url = URL.create(
                "postgresql+psycopg2",
                username=db_data["username"],
                password=db_data["password"],
                host=db_data["server"],
                port=db_data["port"],
                database=db_data["db_name"]
            )
            engine = create_engine(connection_url)
    except Exception as e:
        logging.warning(f"Error:{e}")
        raise Exception(f"Error:{e}")

    inspector = inspect(engine)
    connection = engine.connect()
    logging.warning(f"Successfully connected to Database: {db_data['db_name']}")
except Exception as e:
    logging.warning(f"Error in connecting to Database: {db_data['db_name']}: {e}")
    raise Exception(f"Error in connecting to Database: {db_data['db_name']}: {e}")

# Function to map pandas dtypes to SQLAlchemy types
def map_dtype(dtype):
    if pd.api.types.is_float_dtype(dtype):
        return Float
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return Date
    else:
        return String

def convert_date_columns_to_datetime(df):
    logging.warning("Processing: Date Values")
    if df.columns.str.contains('date').any():
        object_columns = df.select_dtypes(include=['object']).columns
        date_columns = [col for col in object_columns if 'date' in col]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')
    return df

global columns_to_add
columns_to_add = {
    'created_by': Current_User,
    'created_date': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S'),
    'modified_by': Current_User,
    'modified_date': datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
}

global convert_numerics
def convert_numerics(df, table_name):

    logging.warning("Processing: Numeric values")
    try:
        columns = inspector.get_columns(f"{table_name_prefix}{table_name}", schema=schema)
    except Exception as e:
        logging.warning(f"Error in reading column names in table {table_name_prefix}{table_name}: {e}")
        return df
    
    table_cols = [col['name'] for col in columns]
    table_datatypes = [col['type'] for col in columns]
    for col in df.columns:

        if col in table_cols:
            index = table_cols.index(col)
            datatype = str(table_datatypes[index])

            if datatype in ['BOOLEAN', 'BIT', 'DOUBLE PRECISION', 'INTEGER']:
                if datatype in ['BOOLEAN', 'BIT']:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    df[col] = df[col].fillna(False)
                    df[col] = df[col].astype(bool)
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                else:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                  
    return df

# Push each sheet's data to the corresponding table in the SQL Server database
def sheet_to_database(df, sheet_name, table_name, operation):

    logging.warning(f"Pushing data from sheet '{sheet_name}' to table '{table_name}'...")
    # Add columns if they don't exist
    for col, value in columns_to_add.items():
        if col not in df.columns:
            df[col] = value
    
    table_name_with_prefix = table_name_prefix + table_name

    if schema == "":
        table_name_with_schema = table_name_with_prefix
    else:
        table_name_with_schema = schema + '.' + table_name_with_prefix

    # drop column if not exist in database table
    try:
        existing_columns = [col['name'] for col in inspector.get_columns(table_name_with_prefix, schema = schema)]
        df = df[[col for col in df.columns if col in existing_columns]]
    except Exception as e:
        logging.warning(f"Error in reading column names in table {table_name_with_prefix}: {e}")
        return
  
    # Map pandas dtypes to SQLAlchemy types
    sql_dtype = {col: map_dtype(dtype) for col, dtype in df.dtypes.items()}
    # logging.warning(f"SQL data types for sheet '{sheet_name}': {sql_dtype}")
    
    try:

        if operation == "Replace":
            
            if db_type in ['PostgreSQL']:
                result = connection.execute(text(f"DELETE FROM {table_name_with_schema};COMMIT"))
            else:
                result = connection.execute(text(f"DELETE FROM {table_name_with_schema}"))
            df.to_sql(table_name_with_prefix, engine, schema = schema, if_exists='append', index=False, dtype=sql_dtype)
            logging.warning(f"Replacing the Data in table : {table_name_with_schema}")
            
        elif operation == "Append":
          
            df.to_sql(table_name_with_prefix, engine, schema=schema ,if_exists='append', index=False, dtype=sql_dtype)
            logging.warning(f"Appending the Data in table : {table_name_with_schema}")
          
        elif operation == "Truncate":

            if db_type in ['PostgreSQL']:
                result = connection.execute(text(f"DELETE FROM {table_name_with_schema};COMMIT"))
            else:
                result = connection.execute(text(f"DELETE FROM {table_name_with_schema}"))
            logging.warning(f"Truncated the table : {table_name_with_schema}")
         
        logging.warning(f"Data from sheet '{sheet_name}' has been pushed to table '{table_name}'")
        
    except Exception as e:
        logging.warning(f"Error occurred while {operation} data from sheet '{sheet_name}' to table '{table_name}': {e}")
        
# Function to convert all columns to strings and keep empty cells empty
def convert_all_columns_to_str(df):
    logging.warning("Processing Str conversion")
    for col in df.columns:
        df[col] = df[col].apply(lambda x: '' if pd.isna(x) else str(x))
    return df

def conditional_update_configs(df, sheet_name, table_name, operation = None):
      
    logging.warning(f'Sheet: {sheet_name}, currency_scenario_id: {config_scenario_row["currency_scenario_id"]}')
    
    sheet_name_scenario = f'{sheet_name}_scenario'

    if schema == "":
        table_name_with_schema = table_name_prefix + table_name 
    else:
        table_name_with_schema = schema + '.' + table_name_prefix + table_name

    if (sheet_name in configuration_upload_master) and (sheet_name_scenario in config_scenario_row.index.tolist()):
        
        if configuration_upload_master[sheet_name].upper() == 'YES':
            sheet_scenario_id = config_scenario_row[sheet_name_scenario]
            
            # delete the rows where scenario id found 

            try:
                query = f"""
                            DELETE FROM {table_name_with_schema}
                            WHERE scenario_analysis_id = '{sheet_scenario_id}'
                            """
                
                if db_type in ['PostgreSQL']:
                    query += ";COMMIT"

                result = connection.execute(text(query))
            except Exception as e:
                logging.warning(f"table:{table_name}: scenario_analysis_id:{sheet_scenario_id}: {e}")
              
            # Push new data to db
            sheet_to_database(df, sheet_name, table_name, "Append")
              
    elif sheet_name == "currency_scenario_config":
      
        try:
            query = f"""
                    DELETE FROM {table_name_with_schema}
                    WHERE currency_scenario_id = '{scenario_id}'
                    """
                        
            if db_type in ['PostgreSQL']:
                query += ";COMMIT"

            result = connection.execute(text(query))
        except Exception as e:
            logging.warning(f"table:{table_name}: scenario_id:{scenario_id}: {e}")
    
        sheet_to_database(df, sheet_name, table_name, "Append")
        

def conditional_update_source(df, sheet_name, table_name, operation = None):
    
    # currency_conversion_master for reporting date column
    currency_conversion_master = data_workbook.parse('currency_conversion_master')
    if sheet_name not in currency_conversion_master['table_name'].unique():
        logging.warning(f"Cannot perform Conditional Update: {sheet_name} not present in currency_conversion_master.")
        return
    reporting_date_column = currency_conversion_master.loc[currency_conversion_master['table_name'] == sheet_name, 'date_column'].reset_index(drop=True).iloc[0]
    entity_column = currency_conversion_master.loc[currency_conversion_master['table_name'] == sheet_name, 'entity_column'].reset_index(drop=True).iloc[0]
    reporting_date_value = conditional_mapping['reporting_date'].strftime('%Y-%m-%d')
    entity_value = conditional_mapping['legal_entity']

    logging.warning(f'Sheet: {sheet_name}, entity_value: {entity_value}, reporting_date_value: {reporting_date_value}')
    
    if schema == "":
        table_name_with_schema = table_name_prefix + table_name 
    else:
        table_name_with_schema = schema + '.' + table_name_prefix + table_name

    if sheet_name in configuration_upload_master:
        if configuration_upload_master[sheet_name].upper() == 'YES':

            # delete the rows where scenario id found 
            try:
                query = f"""
                      DELETE FROM {table_name_with_schema}
                      WHERE {entity_column} = '{entity_value}' and {reporting_date_column} = '{reporting_date_value}'
                      """
                
                if db_type in ['PostgreSQL']:
                    query += ";COMMIT"
                    
                result = connection.execute(text(query))
            except Exception as e:
                logging.warning(f"table:{table_name}: column:{entity_column}: {e}")
              
            # Push new data to db
            sheet_to_database(df, sheet_name, table_name, "Append")   

def process_sheet(sheet_name, table_name):
    logging.warning(f"Reading sheet: '{sheet_name}' , table:{table_name} ...")
    df = data_workbook.parse(sheet_name)
    
    # Convert all columns to strings
    df = convert_all_columns_to_str(df)
    
    # Convert date columns to datetime if any
    df = convert_date_columns_to_datetime(df)
    
    # Convert numerics
    df = convert_numerics(df, table_name)

    df.replace('', np.nan, inplace=True)
    df = df.replace('nan', pd.NA)
    df = df.replace(" -", pd.NA)
    df = df.replace("-", pd.NA)
    df.dropna(how='all', inplace=True)
    
    return df
  
# to upload configs
def process_configs(operation):

      
    # Process each configs sheet
    if operation == 'Conditional Update':
        process_function = conditional_update_configs
    else:
        process_function = sheet_to_database
      
  
    for sheet_name, table_name in configs_to_table_map.items():
        logging.warning(f"Processing {sheet_name}")
        if sheet_name in error_sheets:
            logging.warning("Terminating source operation because Datatype error found in source sheets")
            continue
      
        if configuration_upload_master[sheet_name].upper() == 'YES':
            df = process_sheet(sheet_name, table_name)
            process_function(df, sheet_name, table_name, operation)
        logging.warning(f"Processed {sheet_name}\n")
        
    logging.warning("Configs sheet read successfully.")

# to upload sources
def process_source(operation):
      
    # Process each source sheet
    if operation == 'Conditional Update':
        process_function = conditional_update_source
    else:
        process_function = sheet_to_database
      
    for sheet_name, table_name in source_to_table_map.items():
        logging.warning(f"Processing {sheet_name}")
        if sheet_name in error_sheets:
            logging.warning("Terminating source operation because Datatype error found in source sheets")
            continue
          
        if configuration_upload_master[sheet_name].upper() == 'YES':
                df = process_sheet(sheet_name, table_name)
                process_function(df, sheet_name, table_name, operation)
        logging.warning(f"Processed {sheet_name}\n")
        
    logging.warning("Configs sheet read successfully.")

global process_masters
def process_masters():
  
    # Check if Datatype error found in master sheets
    if any(key in error_sheets for key in technical_masters_to_table_map.keys()):
        logging.warning("Terminating master operation because Datatype error found in master sheets")
        return
      
    for sheet_name, table_name in technical_masters_to_table_map.items():
        if configuration_upload_master[sheet_name].upper() == 'YES':
            logging.warning(f"Processing {sheet_name}")
            df = process_sheet(sheet_name, table_name) 
            sheet_to_database(df, sheet_name, table_name, "Replace")
            logging.warning(f"Processed {sheet_name}\n")

# Main Code

sftp_creds = Data1.copy()

if Sftp_Name is None or Sftp_Name == "":
    logging.warning("Terminating because sftp name not found")
    raise Exception("Terminating because sftp name not found")
  
try:
    filtered_sftp_df =sftp_creds[sftp_creds['variable_name'] == 'Sftp Name'].reset_index(drop=True)
    index = filtered_sftp_df[filtered_sftp_df['variable_value'] == Sftp_Name].index[0]
    
    hostname = sftp_creds.loc[sftp_creds["variable_name"] == "Hostname", "variable_value"].iloc[index]
    username = sftp_creds.loc[sftp_creds["variable_name"] == "Username", "variable_value"].iloc[index]
    user_secret_key = sftp_creds.loc[sftp_creds["variable_name"] == "Password", "variable_value"].iloc[index]
    port = sftp_creds.loc[sftp_creds["variable_name"] == "Port", "variable_value"].iloc[index]
    
    # Initialize SSH client
    global sftp_client
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    logging.warning(f"Connecting to the SFTP server: {Sftp_Name}...")
    ssh_client.connect(hostname=hostname, username=username, password=user_secret_key, port=port, look_for_keys=False, banner_timeout=200)
    sftp_client = ssh_client.open_sftp()
    logging.warning("Connected to the SFTP server.")
except ssh_exception.AuthenticationException as e:
    logging.warning(f"AuthenticationException Connnecting to the sftp: {Sftp_Name}: {e}")
except Exception as e:
    logging.warning(f"Error Connnecting to the sftp: {Sftp_Name}: {e}")
    raise Exception(f"Error Connnecting to the sftp: {Sftp_Name}: {e}")
    

try:
    # Load the workbooks into pandas ExcelFile objects
    data_workbook_file_path = Report_File
    with sftp_client.file(data_workbook_file_path, 'rb') as remote_file:
        file_data = remote_file.read()
    file_buffer = BytesIO(file_data)
    
    data_workbook = pd.ExcelFile(file_buffer)
    # data_workbook = pd.ExcelFile(Report_File)
    logging.warning("Workbooks loaded into pandas successfully.")
except Exception as e:
    logging.warning(f"Error reading Config Files:{e}")
    raise Exception(f"Error reading Config Files:{e}")

# Creating mapping sheet
tech_master_df = data_workbook.parse("Technical Masters")

if tech_master_df.empty:
    # Terminating cause Master Sheet is Empty
    logging.warning("Terminating cause Master Sheet is Empty")
    sys.exit()
else:
    # Create a dictionary mapping excel sheet names to database table names
    tech_master_df['sheet_type'] = tech_master_df['sheet_type'].fillna('').str.strip().str.upper()
    config_df = tech_master_df[tech_master_df['sheet_type'] == 'CONFIGS']
    source_df = tech_master_df[tech_master_df['sheet_type'] == 'SOURCE']
    technical_master_df = tech_master_df[tech_master_df['sheet_type'] == 'TECHNICAL MASTER']
    configs_to_table_map = dict(zip(config_df["excel_sheet_name"], config_df["system_table_name"].str.lower()))
    source_to_table_map = dict(zip(source_df["excel_sheet_name"], source_df["system_table_name"].str.lower()))
    technical_masters_to_table_map = dict(zip(technical_master_df["excel_sheet_name"], technical_master_df["system_table_name"].str.lower()))
    logging.warning("Sheet to table mappings loaded from 'Technical Master'.")

if Config_Operation == 'Conditional Update' or Source_Operation == 'Conditional Update':
    # load the global_variables sheet for conditional mapping
    global conditional_mapping
    global_variables_df = data_workbook.parse('global_variables')
    conditional_mapping = dict(zip(global_variables_df['variable'], global_variables_df['value']))

    global config_scenario_row, scenario_id
    scenario_id = conditional_mapping['scenario_id']
    config_scenario_row = None
    if scenario_id:
        currency_scenario_config_df = data_workbook.parse('currency_scenario_config')
        filtered_df = currency_scenario_config_df.loc[currency_scenario_config_df['currency_scenario_id'] == scenario_id]
    else:
        logging.warning("Terminating because scenario_id not found in global_variables")
        raise Exception("Terminating because scenario_id not found in global_variables")

    if filtered_df.empty:
        logging.warning("Terminating because scenario_id not found in currency_scenario_config")
        raise Exception("Terminating because scenario_id not found in currency_scenario_config")
    else:
        # Otherwise, get the filtered row
        config_scenario_row = filtered_df.iloc[0]
  
global configuration_upload_master
configuration_upload_master_df = data_workbook.parse('configuration_upload_master')
configuration_upload_master = dict(zip(configuration_upload_master_df['excel_sheet_name'], configuration_upload_master_df['upload_indication']))

# Check for Datatype error in validation_error
global error_sheets
if len(validation_error):
    error_sheets = list(validation_error.loc[validation_error["error_type"] == "Datatype", "sheet_name"])
else:
    error_sheets = []
  
# Process Configs sheets based on input    

if Config_Operation != "Not required":    
    logging.warning(f"Started Processing {Config_Operation} on Config Sheets")
    process_configs(Config_Operation)
    logging.warning("Processed Config Sheets")

# Process Source sheets based on input
if Source_Operation != "Not required":
    logging.warning(f"Started Processing {Source_Operation} Source Sheets")
    process_source(Source_Operation)
    logging.warning("Processed Source Sheets")

# Process Technical Master 
logging.warning("Started Processing Technical Master Sheets")
process_masters()
logging.warning("Processed Technical Master Sheets")

connection.close()
sftp_client.close()
ssh_client.close()  

output_data = validation_error

end_time = time.time()
execution_time = end_time - start_time
minutes = int(execution_time // 60)
seconds = int(execution_time % 60)

logging.warning(f"Execution Time: {minutes} minutes and {seconds} seconds")