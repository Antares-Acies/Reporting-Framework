import pandas as pd
import numpy as np
import paramiko
from io import BytesIO
import logging
from sqlalchemy import create_engine, text, URL, inspect
from datetime import datetime
global re
import re
import json
from Core.users.computations.db_credential_encrytion import decrypt_existing_db_credentials
from Core.users.computations.db_centralised_function import default_engine_creator  
import os
global PLATFORM_FILE_PATH
from config.settings.base import PLATFORM_FILE_PATH

# Ensure the directory exists
log_directory = fr'{PLATFORM_FILE_PATH}alm_data'

# Configure logging to write to a file
log_file = os.path.join(log_directory, "logs_configuration_automation.txt")
logging.basicConfig(filename=log_file, level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# Connection to DB starts  
json_path = 'Platform_Configs/user_databases.json'
with open(json_path, 'r') as file:
    db_json = json.load(file)
  
db_data = db_json[Database_Name]
if db_data is None:
    # Terminate if DB not found in system
    logging.info("Database not found in system. Terminating the execution")
    sys.exit()

db_type = db_data["db_type"]
if db_type not in ["MSSQL", "PostgreSQL", "Oracle"]:
    raise Exception("db type should be ['MSSQL', 'PostgreSQL', 'Oracle']")
  
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
  schema = db_data["schema"]
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
    
  inspector = inspect(engine)
  connection = engine.connect()
  logging.info(f"Successfully connected to Database: {db_data['db_name']}")
  
except Exception as e:
  logging.info(f"{e}")
  raise Exception(f"Error in connecting to Database: {db_data['db_name']}: {e}")
  
sftp_creds = Data1.copy()

if Sftp_Name is None or Sftp_Name == "":
    logging.warning("Terminating because sftp name not found")
    sys.exit()

try:
    filtered_sftp_df =sftp_creds[sftp_creds['variable_name'] == 'Sftp Name'].reset_index(drop=True)
    index = filtered_sftp_df[filtered_sftp_df['variable_value'] == Sftp_Name].index[0]
    
    hostname = sftp_creds.loc[sftp_creds["variable_name"] == "Hostname", "variable_value"].iloc[index]
    username = sftp_creds.loc[sftp_creds["variable_name"] == "Username", "variable_value"].iloc[index]
    user_secret_key = sftp_creds.loc[sftp_creds["variable_name"] == "Password", "variable_value"].iloc[index]
    port = sftp_creds.loc[sftp_creds["variable_name"] == "Port", "variable_value"].iloc[index]
    
    # Initialize SSH client
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    logging.warning(f"Connecting to the SFTP server: {Sftp_Name}...")
    ssh_client.connect(hostname=hostname, username=username, password=user_secret_key, port=port)
    sftp_client = ssh_client.open_sftp()
    logging.warning("Connected to the SFTP server.")
except Exception as e:
    raise Exception(f"Error Connnecting to the sftp: {Sftp_Name}: {e}")

try:
    with sftp_client.file(Report_File, 'rb') as remote_file:
        file_data = remote_file.read()
    report_file = BytesIO(file_data)
    
    global report_workbook
    report_workbook = pd.ExcelFile(report_file)
    technical_masters_df = report_workbook.parse("Technical Masters")
    
    global configuration_upload_master
    configuration_upload_master_df = report_workbook.parse("configuration_upload_master")
    configuration_upload_master = dict(zip(configuration_upload_master_df['excel_sheet_name'], configuration_upload_master_df['upload_indication']))
except Exception as e:
    raise Exception(f"Error reading Config Files:{e}")
  
global table_name_prefix
table_name_prefix = "users_"

# Validation file format
global validation_error
validation_error = {'index': [], 'sheet_name': [], 'error_type':[], 'entity': [], 'error': []}

global validate_sheet_datatype
def validate_sheet_datatype(sheet_name, table_name):
    try:
        columns = inspector.get_columns(table_name, schema=schema)
        table_cols = [col['name'] for col in columns]
        table_datatypes = [col['type'] for col in columns]
        df = report_workbook.parse(sheet_name)
    except Exception as e:
        logging.warning(f"Error reading columns data: {table_name}, {e}")
        return
    
    # Iterating over each columns to be inserted
    for i, col in enumerate(df.columns):
        wrong_values = None
        if col in table_cols:
          try:
            # get datatype
            index = table_cols.index(col)
            datatype = str(table_datatypes[index])
            wrong_values = ""

            if df[col].empty:
                continue
            
            # get actual and expected datatype
            if datatype == 'DATE':
                expected_type = 'Date'
                try:
                    df[col] = pd.to_datetime(df[col], errors='raise', dayfirst=True)
                    actual_dtype = "Date"
                except ValueError:
                    actual_dtype = "Character" if df[col].dtype == "object" else df[col].dtype
                    # get invalid values
                    wrong_values = df[~pd.to_datetime(df[col], errors='coerce').notna() & df[col].notna()][col].tolist()


            elif datatype == 'DOUBLE PRECISION':
                expected_type = 'Float64'
                actual_dtype = "Float64" if pd.api.types.is_numeric_dtype(df[col]) else df[col].dtype
              
            elif datatype == 'BOOLEAN':
                expected_type = 'Boolean'
              
                # get invalid boolean values
                valid_true_values = ['t', 'true', 'y', 'yes', 'on', 1, 1.0]
                valid_false_values = ['f', 'false', 'n', 'no', 'off', 0, 0.0]
                valid_boolean_values = valid_true_values + valid_false_values
              
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(9)
                df[col] = df[col].apply(lambda x: int(x) if isinstance(x, float) else x)
                df.loc[df[col] == '', col] = None
                df.loc[df[col] == 9, col] = None
                
                wrong_values = df[~df[col].isin(valid_boolean_values)][col].dropna()

                if wrong_values.empty:
                    actual_dtype = "Boolean"
                else:
                    actual_dtype = "Invalid Boolean"
                  
            else:
                expected_type = actual_dtype = "Character"

            if actual_dtype != expected_type:
                validation_error['index'].append(i + 1)
                validation_error['sheet_name'].append(sheet_name)
                validation_error['error_type'].append("Datatype")
                validation_error['entity'].append(f"column: {col}")
                validation_error['error'].append(f"expected type: '{expected_type}', actual type: '{actual_dtype}' wrong_values: {wrong_values}")
          except Exception as e:
            logging.warning(f"Error in datatype check column: {col}: {e}")

logging.warning("Datatype Validation started")

# Iterate over tables present in Technical Masters
for i, row in technical_masters_df.iterrows(): 
    table_name = row['system_table_name'].lower()
    sheet_name = row['excel_sheet_name']
    if configuration_upload_master[sheet_name].upper() == 'YES':
        try:
          validate_sheet_datatype(sheet_name, f"{table_name_prefix}{table_name}")
        except Exception as e:
          raise Exception(f"{sheet_name}: {e}")
logging.warning("Datatype Validation finished")

# Other Validations starts here
# Parse necessary sheets
try:
    report_format_df = report_workbook.parse("report_format")
    column_type_df = report_workbook.parse("column_type")
    rule_group_definition_df = report_workbook.parse("rule_group_definition")
    rule_definition_df = report_workbook.parse("rule_definition")
    mapping_set_df = report_workbook.parse("mapping_set")
    rule_based_bucketing_df = report_workbook.parse("rule_based_bucketing")
    aggregation_master_df = report_workbook.parse("aggregation_master")
    groupby_operation_master_df = report_workbook.parse("groupby_operation_master")
    logging.warning("Read necessary sheets successfully")
except Exception as e:
    raise Exception(f"Error in reading necessary sheets: {e}")

# Filter necessary columns
columns_to_validate_report_format = column_type_df[column_type_df['calculated_column']
                                                   == "Yes"]['column_name']
rule_group_definition_df = rule_group_definition_df[[
    'rule_group', 'rule_group_operation', 'rule_set', 'sub_rule_group']]
rule_definition_df = rule_definition_df[[
    'condition_rule_set', 'condition_groupby_operation', 'condition_type', 'condition_value']]
logging.warning("Filtered the sheets with necessary columns")

# Coverted to list for easier access
all_rule_groups = list(rule_group_definition_df['rule_group'].unique())
sub_rule_groups = list(rule_group_definition_df['sub_rule_group'].unique())
all_rule_set = list(rule_definition_df['condition_rule_set'].unique())
all_mapping_set = list(mapping_set_df['mapping_set'].unique())
all_operations = list(aggregation_master_df['aggregation_name'].unique())
all_groupby_operations = list(groupby_operation_master_df['conditionality_type'].unique())
logging.warning("Created list of columns") 

validated_rule_set_rule_group = []

# Necessary regex pattern
rule_group_pattern = r"Rule_Grp\d{4}[A-Z]?"
contains_pattern = r"MAP\d{4}"

logging.warning(f"\n\nStarted Validating sheet: report_format")

# Iterate over rows of report_format
for i, row in report_format_df.iterrows():
    logging.warning("")

    # Iterate over columns mentioned in column_type
    for col in columns_to_validate_report_format:

        logging.warning(f"Row: {i}")
        logging.warning(f"Validating column: {col}")

        rule_grp = row[col]
        if pd.isna(rule_grp):
            continue

        match = re.match(rule_group_pattern, rule_grp)
        match = True # Remove this if pattern check is needed
        if not match:

            logging.warning(f"{col}: {rule_grp} does not match the pattern")
            validation_error['index'].append(i + 1)
            validation_error['sheet_name'].append('report_format')
            validation_error['error_type'].append('Rule Group')
            validation_error['entity'].append(f"{col}: {rule_grp}")
            validation_error['error'].append("Does not match the pattern")

        else:

            # Validate Rule_grp in rule_group_definition

            error_msg, is_error = "", False

            # Check if it's a rule_grp or sub_rule_grp
            if not re.search(r"[A-Z]$", rule_grp):

                if rule_grp not in all_rule_groups:

                    is_error = True
                    error_msg = "Rule_group not defined in rule_group_definition"
                    logging.warning(f"Rule_group:{rule_grp} not defined in rule_group_definition")

            else:

                if (rule_grp in all_rule_groups) and (
                        rule_grp in sub_rule_groups):
                    is_error = True
                    error_msg = "Sub_Rule_Grp Not defined and/or(under any rule_group) in rule_group_definition"
                    logging.warning(f"Sub_Rule_Grp: {rule_grp} not defined and/or(under any rule_group) in rule_group_definition")

            logging.warning("Validated Rule_grp")

            # If rule_grp defined, check further for other validations
            if is_error:

                validation_error['index'].append(i + 1)
                validation_error['sheet_name'].append('report_format')
                validation_error['error_type'].append('Rule Group')
                validation_error['entity'].append(f"{col}: {rule_grp}")
                validation_error['error'].append(error_msg)

            else:

                # Validate Rule_Set in rule_definition
                rule_set = rule_group_definition_df.loc[rule_group_definition_df['rule_group']
                                                        == rule_grp, 'rule_set']
                rule_set = rule_set.iloc[0] if not rule_set.empty else None
                is_continue = True
                temp_rule_grp = rule_grp
                error_msg = ""
                rule_set_validated = False

                # Search for rule_set
                while pd.isna(rule_set) and is_continue:
                    if temp_rule_grp not in validated_rule_set_rule_group:
                        sub_rule_group = rule_group_definition_df.loc[
                            rule_group_definition_df['rule_group'] == temp_rule_grp, 'sub_rule_group']
                        if sub_rule_group.empty:
                            is_continue = False
                            logging.warning(
                                f"Sub_Rule_Group: {temp_rule_grp} not found")
                            error_msg += f"Sub_Rule_Group: {temp_rule_grp} not found"
                            break

                        sub_rule_group = sub_rule_group.iloc[0] if not sub_rule_group.empty else None
                        rule_set = rule_group_definition_df.loc[
                            rule_group_definition_df['rule_group'] == sub_rule_group, 'rule_set']
                        rule_set = rule_set.iloc[0] if not rule_set.empty else None
                        temp_rule_grp = sub_rule_group
                        error_msg += f"Sub_Rule_Group: {temp_rule_grp} > "

                    else:

                        rule_set_validated = True
                        logging.warning(
                            f"{i}: Rule set already validated under rule_grp: {temp_rule_grp}")
                        break
                      
                # if rule_set not validated already
                if not rule_set_validated:

                    if not is_continue:

                        # It means, no rule set found
                        validation_error['index'].append(i + 1)
                        validation_error['sheet_name'].append('report_format')
                        validation_error['error_type'].append('Rule Set')
                        validation_error['entity'].append(f"{col}: {rule_grp}")
                        validation_error['error'].append(
                            f"No rule set found: {error_msg}")
                        logging.warning(f"No rule set found: {error_msg}")

                    else:

                        # rule_set found, Validating rule_set
                        is_error = False
                        if rule_set in all_rule_set:
                            condition_value = rule_definition_df[
                                (rule_definition_df['condition_rule_set'] == rule_set) &
                                (rule_definition_df['condition_type'] == 'Contains')
                            ]['condition_value']

                            if not condition_value.empty:

                                mapping_set = condition_value.iloc[0]

                                if not pd.isna(mapping_set):

                                    match = re.match(contains_pattern, mapping_set)
                                    match = True # Remove this if pattern check is needed
                                    if not match:

                                        # Mapping set does not match the
                                        # pattern
                                        is_error = True
                                        error_msg = f"Contains: {mapping_set}, does not match the pattern"

                                    else:

                                        # Validate mapping_set in mapping_set
                                        if mapping_set not in all_mapping_set:
                                            is_error = True
                                            error_msg = f"Contains: {mapping_set}, not defined in mapping_set"

                                        logging.warning("Validated Mapping Set")

                                else:

                                    is_error = True
                                    error_msg = "Contains is Empty"

                            
                            # Validating condition_groupby_operation of rule_definition in groupby_operation_master
                            rule_groupby_operation = rule_definition_df.loc[
                                rule_definition_df['condition_rule_set'] == rule_set, 'condition_groupby_operation']
                            rule_groupby_operation = rule_groupby_operation.iloc[
                                0] if not rule_groupby_operation.empty else None
                            
                            if rule_groupby_operation is not None:
                                if rule_groupby_operation not in all_groupby_operations:
                                    if is_error:
                                        error_msg += f". rule_groupby_operation: {rule_groupby_operation} not defined in groupby_operation_master" 
                                    else:
                                        is_error = True
                                        error_msg = f"rule_groupby_operation: {rule_groupby_operation} not defined in groupby_operation_master" 

                            else:
                                if is_error:
                                    error_msg += ". rule_groupby_operation not mentioned" 
                                else:
                                    is_error = True
                                    error_msg = "rule_groupby_operation not mentioned" 


                            logging.warning("Validated condition_groupby_operation of rule_set")

                        else:
                            is_error = True
                            error_msg = "Rule set defined in rule_definition"

                        # Log the error
                        if is_error:
                            validation_error['index'].append(i + 1)
                            validation_error['sheet_name'].append(
                                'report_format')
                            validation_error['error_type'].append('Rule Set')
                            validation_error['entity'].append(
                                f"{col}: {rule_grp}: {rule_set}")
                            validation_error['error'].append(error_msg)
                            logging.warning(error_msg)
                        else:
                            validated_rule_set_rule_group.append(rule_grp)

                    logging.warning("Validated rule_set")


                # Validating rule_group_operation in aggregation_master
                rule_group_operation = rule_group_definition_df.loc[
                    rule_group_definition_df['rule_group'] == rule_grp, 'rule_group_operation']
                rule_group_operation = rule_group_operation.iloc[
                    0] if not rule_group_operation.empty else None

                if not pd.isna(rule_group_operation):

                    if rule_group_operation not in all_operations:

                        validation_error['index'].append(i + 1)
                        validation_error['sheet_name'].append(
                            'rule_group_definition')
                        validation_error['error_type'].append('Rule Group Operation')
                        validation_error['entity'].append(
                            f"{col}: {rule_grp}: {rule_group_operation}")
                        validation_error['error'].append(
                            f"{rule_group_operation} not defined in aggregation_master")
                        logging.warning(f"{rule_group_operation} not defined in aggregation_master")
                else:
                    logging.warning("rule_group_operation not mentioned")
                
                logging.warning("Validated rule_group_operation in aggregation_master")

                # More validations goes here
                ############################

        logging.warning("Validated Rule_grp in rule_group_definition")  

logging.warning("Started Validating rule_based_bucketing in groupby_operation_master")
# Validating rule_based_bucketing in groupby_operation_master
for i, row in rule_based_bucketing_df.iterrows():
    is_error = False

    condition_groupby_operation = row['condition_groupby_operation']

    if (condition_groupby_operation is not None) and (not pd.isna(condition_groupby_operation)):
        if condition_groupby_operation not in all_groupby_operations:
            is_error = True
            error_msg = f"condition_groupby_operation: {condition_groupby_operation} not defined in groupby_operation_master" 
    else:
        is_error = True
        error_msg = "condition_groupby_operation not mentioned."
    
    if is_error:
        validation_error['index'].append(i + 1)
        validation_error['sheet_name'].append(
            'rule_based_bucketing')
        validation_error['error_type'].append('Condition GroupBy Function')
        validation_error['entity'].append(
            f"condition_groupby_operation: {condition_groupby_operation}")
        validation_error['error'].append(error_msg)
        logging.warning(error_msg)

logging.warning("Validating rule_based_bucketing in groupby_operation_master finished")

validation_error_df = pd.DataFrame(validation_error)

global columns_to_add
columns_to_add = {
    'created_by': Current_User,
    'created_date': datetime.today().strftime('%Y-%m-%d'),
    'modified_by': Current_User,
    'modified_date': datetime.today().strftime('%Y-%m-%d')
}

for col, value in columns_to_add.items():        
    validation_error_df[col] = value

logging.warning("Saved the validation_error into database.")
connection.close()

output_data = validation_error_df
