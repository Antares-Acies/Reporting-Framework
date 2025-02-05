# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 17:57:04 2024

@author: KumarAkashdeep
"""

import numpy as np
import logging
import pandas as pd
import xlsxwriter
import time, os, datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor


pd.set_option('display.max_rows', None)

# Set the location of your files
location = r"C:\Users\DeveshArya\Reporting Framework\Reporting-Framework\Code"

# Define the paths to your files
org_path = fr"{location}\SLS Configurations - Suryoday.xlsx"

file_path_rule_template = fr"{location}\rule_value_{{}}.xlsx"
file_path_rule_grp_template = fr"{location}\rule_grp_value_{{}}.xlsx"
file_path_ans_template = fr"{location}\result_report_format_{{}}.xlsx"

# Read all sheets at once
all_sheets = pd.read_excel(org_path, sheet_name=None, engine='openpyxl')
logging.warning("Read the Workbook successfully.")

# Extract the required sheets
report_format = all_sheets['report_format']
rule_group_def = all_sheets['rule_group_definition']
rule_def = all_sheets['rule_definition']
mapping_set = all_sheets['mapping_set']
merge_master = all_sheets['merge_master']
source_master = all_sheets['source_master']
source_column_list = all_sheets['source_column_list']
currency_pair_master = all_sheets['currency_pair_master']
currency_conversion_master = all_sheets['currency_conversion_master']
quoted_security_data = all_sheets['quoted_security_data']
currency_scenario_config = all_sheets['currency_scenario_config']
currency_conversion_exemption = all_sheets['currency_conversion_exemption']
column_type = all_sheets['column_type']
bucket_definition = all_sheets['bucket_definition']
bucket_rule_mapping = all_sheets['bucketing_rule_mapping']
bucketing_type = all_sheets['bucketing_type']
rule_based_bucketing = all_sheets['rule_based_bucketing']
static_pattern_bucketing = all_sheets['static_pattern_bucketing']
reporting_pattern_bucketing = all_sheets['reporting_pattern_bucketing']
reporting_bucketing_adjustment = all_sheets['reporting_bucketing_adjustment']
limit_setup = all_sheets['limit_setup']

# Example of processing data
table_primary_keys = source_master.copy()
table_primary_keys['Primary key'] = table_primary_keys.apply(
    lambda row: f"{row['source_table_name']}_+_{row['primary_key']}", axis=1
)

# Further processing...
currency_conversion_exemption['value_source_column'] = currency_conversion_exemption['value_source_table'] + "_+_" + currency_conversion_exemption['value_source_column']
rule_based_bucketing['condition_column_name'] = rule_based_bucketing['condition_source_table'] + "_+_" + rule_based_bucketing['condition_column_name']

# Get the list of columns that need to be calculated
calculated_columns = column_type[column_type['calculated_column'] == 'Yes']['column_name'].tolist()
logging.warning(f"Columns to be calculated: {calculated_columns}")

# Check if bucketing is applicable for any of the columns
bucketing_flag_global = 'Yes' if 'Yes' in column_type['bucketing_applicability'].values else 'No'
logging.warning(f"Bucketing applicability flag: {bucketing_flag_global}")

# Read bucketing-related data if any column requires bucketing
if 'Yes' in column_type['bucketing_applicability'].values:
    logging.warning("Reading bucketing-related data...")
    bucket_ids = bucket_definition['bucket_id'].unique().tolist()
    if 'Unbucketed' not in bucket_ids:
        bucket_ids.append('Unbucketed')
else:
    # If bucketing is not applicable, set bucket_ids to []
    bucket_ids = []

logging.warning("All dataframes read successfully.")

# Function definitions

def read_dataframes(org_path):
    """
    Reads required sheets from the Excel file based on unique source tables in rule_def and rule_group_def.
    """
    unique_source_sheet = rule_def_scenario['condition_source_table'].unique().tolist() + rule_def_scenario['value_source_table'].unique().tolist()
    
    unique_source_sheet = list(set(unique_source_sheet))  # Remove duplicates

    # Include any additional tables needed for threshold values from rule_group_def_scenario
    threshold_source_tables = rule_group_def_scenario['threshold_source_table'].dropna().unique().tolist()
    unique_source_sheet += threshold_source_tables
    unique_source_sheet = list(set(unique_source_sheet))

    logging.warning(f"Unique source sheets to read: {unique_source_sheet}")

    dataframes = {}
    for sheet in unique_source_sheet:
        try:
            logging.warning(f"Reading sheet: {sheet}")
            sheet_data = pd.read_excel(org_path, sheet_name=sheet)
            # Prefix column names with table_name to avoid conflicts
            sheet_data.columns = [f"{sheet}_+_{col}" for col in sheet_data.columns]
            #sheet_data.columns = [f"{sheet}_+_{col}" if not col.startswith(f"{sheet}_+_") else f"{sheet}_+_{col}" for col in sheet_data.columns]
            dataframes[sheet] = sheet_data
            logging.warning(f"Sheet {sheet} read successfully.")
        except Exception as e:
            logging.warning(f"EXCEL SHEET {sheet} is MISSING: {e}")
    return dataframes


def group_filter_data():
    """
    Reads required sheets from the Excel file and groups columns by table_name in a dictionary.
    """
    # Helper function to check if a dataset exists and is not empty
    def dataset_exists(dataset_name):
        if dataset_name not in globals():
            logging.warning(f"Dataset '{dataset_name}' is not defined!")
            return False
        dataset = globals()[dataset_name]
        if dataset is None or dataset.empty:
            logging.warning(f"Dataset '{dataset_name}' is empty!")
            return False
        return True

    # Helper function to prepare DataFrame with uniform columns
    def prepare_dataframe(df, columns, new_column_names):
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            logging.warning(f"Missing columns: {missing_columns}")
            raise ValueError(f"Missing columns: {missing_columns}")
        df_copy = df[columns].copy()
        df_copy.columns = new_column_names
        return df_copy

    # List of datasets and their corresponding parameters for preparation
    datasets = [
        ("rule_def_scenario", ['condition_source_table', 'condition_column_name'], ['table_name', 'column_name'], "rule_def_scenario_copy_1"),
        ("rule_def_scenario", ['value_source_table', 'value_source_column'], ['table_name', 'column_name'], "rule_def_scenario_copy_2"),
        ("rule_def_scenario", ['weight_source_table', 'weight_source_column'], ['table_name', 'column_name'], "rule_def_scenario_copy_3"),
        ("rule_group_def_scenario", ['threshold_source_table', 'threshold_filter_column'], ['table_name', 'column_name'], "rule_group_def_scenario_copy_1"),
        ("rule_group_def_scenario", ['threshold_source_table', 'threshold_value'], ['table_name', 'column_name'], "rule_group_def_scenario_copy_2"),
        ("rule_based_bucketing", ['condition_source_table', 'condition_column_name'], ['table_name', 'column_name'], "rule_based_bucketing_copy_1"),
        ("reporting_bucketing_adjustment", ['condition_source_table', 'condition_column_name'], ['table_name', 'column_name'], "reporting_bucketing_adjustment_copy_1"),
        ("reporting_bucketing_adjustment", ['value_source_table', 'value_source_column'], ['table_name', 'column_name'], "reporting_bucketing_adjustment_copy_2"),
        ("merge_master", ['value_source_table', 'left_key'], ['table_name', 'column_name'], "merge_master_copy_1"),
        ("merge_master", ['condition_source_table', 'right_key'], ['table_name', 'column_name'], "merge_master_copy_2"),
        ("source_master", ['source_table_name', 'primary_key'], ['table_name', 'column_name'], "source_master_copy_1"),
        ("source_column_list", ['source_table_name', 'source_table_column'], ['table_name', 'column_name'], "source_column_list_copy_1"),
        ("currency_conversion_master", ['table_name', 'currency_column'], ['table_name', 'column_name'], "currency_conversion_master_copy_1"),
        ("currency_conversion_master", ['table_name', 'date_column'], ['table_name', 'column_name'], "currency_conversion_master_copy_2"),
        ("currency_conversion_master", ['table_name', 'entity_column'], ['table_name', 'column_name'], "currency_conversion_master_copy_3"),
        ("limit_setup", ['column_identifier_table', 'column_identifier'], ['table_name', 'column_name'], "limit_setup_copy_1"),
        ("limit_setup", ['column_identifier_table', 'rule_group'], ['table_name', 'column_name'], "limit_setup_copy_2"),
    ]

    prepared_dataframes = []

    # Prepare DataFrames only if the dataset exists and is defined
    for dataset_name, columns, new_column_names, var_name in datasets:
        if dataset_exists(dataset_name):
            try:
                dataset = globals()[dataset_name]
                prepared_dataframes.append(prepare_dataframe(dataset, columns, new_column_names))
                logging.info(f"Prepared DataFrame for '{var_name}' successfully.")
            except ValueError as ve:
                logging.warning(f"Column error in '{var_name}': {ve}")
            except Exception as e:
                logging.warning(f"Error preparing DataFrame for '{var_name}': {e}")

    # Concatenate all the prepared DataFrames
    unique_sheet_column_combination = pd.concat(prepared_dataframes, ignore_index=True).drop_duplicates()

    # Create dictionary: Group by 'table_name'
    grouped_data = (
        unique_sheet_column_combination.groupby('table_name')['column_name']
        .apply(list)
        .to_dict()
    )
    logging.info(f"Processed DataFrame (first 5 rows):\n{unique_sheet_column_combination.head()}")

    logging.info("\nStep 2: Grouping by 'table_name'.")
    for table, columns in grouped_data.items():
        logging.info(f"Table: {table} | Mapped Columns: {columns}")

    return grouped_data

def filter_dataframes_by_grouped_data(dataframes, grouped_data):
    for table_name, columns in grouped_data.items():
        # Ensure the dataframe for this table exists
        logging.warning(f"Table Name: {table_name}")
        if table_name not in dataframes:
            logging.warning(f"Table {table_name} not found in dataframes.")
            continue
        df = dataframes[table_name]
        # Prepare a list to hold all relevant columns (transformed and standalone)
        relevant_columns = []
        for column in columns:
            transformed_column = f"{table_name}_+_{column}"
            # Check if the transformed column already exists in relevant_columns
            if transformed_column in df.columns and transformed_column not in relevant_columns:
                relevant_columns.append(transformed_column)
                logging.warning(f"Adding Transformed_column : {transformed_column}")
            elif column in df.columns and column not in relevant_columns:
                # Add the standalone column if it exists
                relevant_columns.append(column)
                logging.warning(f"Original column is added {column}")
            else:
                logging.warning(f"{transformed_column} or {column} not found.")
        # Remove duplicates (if any) to ensure no redundancy
        relevant_columns = list(set(relevant_columns))
        # Filter the dataframe to only include the relevant columns
        df = df[relevant_columns]
        # Log the result
        logging.warning(f"Table: {table_name} | Filtered Columns: {relevant_columns} | Filtered Columns' Length : {len(relevant_columns)}")
        # Ensure you update the dataframe in the dataframes list
        dataframes[table_name] = df
    return dataframes

def dynamic_merge(merge_master, dataframes):
    """
    Dynamically merges DataFrames based on instructions in merge_master,
    preserving the row-by-row order from merge_master and allowing
    multi-key merges for consecutive lines of the same (value_source_table, condition_source_table).
    
    In addition, once a table is merged, the updated version will be used
    in subsequent merges.
    """
    merged_data = {}
    
    # Keep track of the current "block" of merges (same value/condition pair).
    last_pair = None
    accumulated_left_keys = []
    accumulated_right_keys = []
    
    def do_merge_step(left_table, right_table, left_keys, right_keys):
        """
        Perform one merge step for a pair (left_table, right_table) over 
        the accumulated lists of keys in left_keys and right_keys.
        """
        logging.warning(f"  ")
        logging.warning(f"Merging {left_table} with {right_table}")
        logging.warning(f"left_keys: {left_keys}, right_keys: {right_keys}")
        
        # --- 1. Determine the most up-to-date left_df and right_df ---
        if left_table in merged_data:
            left_df = merged_data[left_table]
        else:
            if left_table not in dataframes:
                logging.warning(f"Left table {left_table} not in `dataframes`. Skipping.")
                return
            left_df = dataframes[left_table]
        
        if right_table in merged_data:
            right_df = merged_data[right_table]
        else:
            if right_table not in dataframes:
                logging.warning(f"Right table {right_table} not in `dataframes`. Skipping.")
                return
            right_df = dataframes[right_table]
        
        # --- 2. Log shapes ---
        logging.warning(f"Before merge: {left_table} shape = {left_df.shape}")
        logging.warning(f"Before merge: {right_table} shape = {right_df.shape}")

        left_on = [f"{left_table}_+_{k}" for k in left_keys]
        right_on = [f"{right_table}_+_{k}" for k in right_keys]
        
        # --- 3. Merge ---
        merged_df = pd.merge(
            left=left_df,
            right=right_df,
            left_on=left_on,
            right_on=right_on,
            how='left'
        )
        
        logging.warning(f"After merge: {left_table} shape = {merged_df.shape}")
        
        # --- 4. Update merged_data so next merges will see the new version ---
        merged_data[left_table] = merged_df

    # Iterate row by row in the original order
    for idx, row in merge_master.iterrows():
        pair = (row['value_source_table'], row['condition_source_table'])

        # if we're on a new pair, flush the old block
        if last_pair is not None and pair != last_pair:
            do_merge_step(
                left_table=last_pair[0],
                right_table=last_pair[1],
                left_keys=accumulated_left_keys,
                right_keys=accumulated_right_keys
            )
            accumulated_left_keys = []
            accumulated_right_keys = []

        accumulated_left_keys.append(row['left_key'])
        accumulated_right_keys.append(row['right_key'])
        last_pair = pair
    
    # final flush
    if last_pair is not None:
        do_merge_step(
            left_table=last_pair[0],
            right_table=last_pair[1],
            left_keys=accumulated_left_keys,
            right_keys=accumulated_right_keys
        )
    
    logging.warning(f"  ")
    logging.warning("All merges completed in config table order.")
    time.sleep(7)

    return merged_data

def get_all_dataframes_dict(merge_master, merged_data, dataframes):
    """
    Combines original and merged dataframes into a single dictionary.
    """
    all_dataframes_dict = dataframes.copy()
    for index, row in merge_master.iterrows():
        table_name = row['value_source_table']
        try:
          if table_name in merged_data:
              
              all_dataframes_dict[table_name] = merged_data[table_name]
          else:
              
              if table_name in dataframes:
                  logging.warning(f'This Table : {table_name} is not found. The size is {dataframes[table_name].shape}.')
                  all_dataframes_dict[table_name] = dataframes[table_name]
              else:
                  logging.warning(f'This Table : {table_name} is not found. Initializing an empty DataFrame.')
                  all_dataframes_dict[table_name] = pd.DataFrame()
        except KeyError:
            logging.warning(f'This Table : {table_name} is not found. Initializing an empty DataFrame.')
            all_dataframes_dict[table_name] = pd.DataFrame()
    return all_dataframes_dict

def conversion(dataframes, currency_conversion_master, currency_conversion_rate, base, reporting_currency):
    """
    Converts amounts to the reporting currency using conversion rates, considering exemptions.
    """
    logging.warning(f" currency conversion fxn start ")
    for index, row in base.iterrows():
        table_name = row['value_source_table']
        column_name = row['value_source_column']
        
        logging.warning(f" table_name {table_name} ")
        logging.warning(f" column_name {column_name} ")
        
        
        identifier_column_series = currency_conversion_master[currency_conversion_master['table_name'] == table_name]['currency_column']
        if identifier_column_series.empty:
            print(f"No currency_column found for table {table_name}")
            continue
        identifier_column = identifier_column_series.values[0]
        df = dataframes[table_name]
        
        # Get from_currency column
        from_currency_series = df[identifier_column]
        # Build mask where from_currency != reporting_currency
        currency_mask = from_currency_series != reporting_currency        
        # Build exemption mask
        exemption_rows = currency_conversion_exemption[
            (currency_conversion_exemption['value_source_table'] == table_name) &
            (currency_conversion_exemption['value_source_column'] == column_name)
        ]
        
        logging.warning(f" exemption_rows {exemption_rows} ")
        
        if not exemption_rows.empty:
            logging.warning(f" currency conversion exemption is not empty ")
            # There is an exemption, get the condition column and criteria
            for _, exemption_row in exemption_rows.iterrows():
                exemption_condition_column = exemption_row['exemption_condition_column']
                exemption_condition_criteria = exemption_row['exemption_condition_criteria']
                
                # Build full column name (since columns are prefixed)
                full_exemption_condition_column = f"{table_name}_+_{exemption_condition_column}"
                
                logging.warning(f" full_exemption_condition_column {full_exemption_condition_column} ")
                
                if full_exemption_condition_column not in df.columns:
                    print(f"Exemption condition column {full_exemption_condition_column} not found in table {table_name}")
                    continue
                
                exemption_mask = df[full_exemption_condition_column] == exemption_condition_criteria
                logging.warning(f"len exemption_mask {len(exemption_mask)} ")
                
                # Update the currency_mask to exclude rows where exemption applies
                currency_mask = currency_mask & (~exemption_mask)
        
        # Now, for rows where currency_mask is True, perform currency conversion    # Get the from_currency values for these rows
        currencies_to_convert = df.loc[currency_mask, identifier_column]
        
        # Map from_currency to rates
        rates = currencies_to_convert.map(currency_conversion_rate.set_index('from_currency')['quoted_price'])
        rates.fillna(1, inplace=True)
        logging.warning(f"currencies_to_convert {len(currencies_to_convert)} ")
        logging.warning(f"rates {len(rates)} ")
        logging.warning(f"column_name {column_name} ")
        logging.warning(f"currency_mask {len(currency_mask)} ")
        
        df.loc[currency_mask, column_name] = df.loc[currency_mask, column_name] * rates
        dataframes[table_name] = df
    
    logging.warning(f" currency conversion fxn end's  ")
    return dataframes


def evaluate_condition(df, condition):
    """
    Evaluates a condition on a DataFrame based on the condition type.
    """
    #logging.warning(f"Evaluating condition: {condition}")

    # Check if 'condition_datatype' key exists in the condition dictionary
    condition_datatype = condition.get('condition_datatype')
    if condition_datatype is None:
        logging.warning("KeyError: 'condition_datatype' not found in condition dictionary")
        return pd.Series([True] * len(df), index=df.index)

    condition_column_name = condition['condition_column_name']
    if condition_column_name not in df.columns:
        logging.warning(f"Failed to find the target column: {condition_column_name}")
        # Return a Series of True with the same index as df
        return pd.Series([True] * len(df), index=df.index)
    
    df_column = df[condition_column_name]
    ####### Convert the column to the specified datatype
    try:
        if condition_datatype == 'Integer':
            df[condition_column_name] = df[condition_column_name].astype(str)
            df[condition_column_name] = df[condition_column_name].str.lower()
            df[condition_column_name] = df[condition_column_name].replace({'false': 0, 'true': 1})
            df[condition_column_name] = df[condition_column_name].astype(int)
            logging.warning(f" inside integer conversion for column ")
            df[condition_column_name] = df[condition_column_name].astype(int)
        
            df_column = df[condition_column_name].astype(int)
        elif condition_datatype == 'Numeric' or condition_datatype == 'Float':
            df_column = df[condition_column_name].astype(float)
        elif condition_datatype == 'Character':
            df_column = df[condition_column_name].astype(str)
        elif condition_datatype == 'Date':
            df_column = pd.to_datetime(df[condition_column_name])
        elif condition_datatype == 'Boolean':
            df_column = df[condition_column_name].astype(bool)
        else:
            logging.warning(f"Unknown condition_datatype: {condition_datatype}")
            return pd.Series([True] * len(df), index=df.index)
    except Exception as e:
        logging.warning(f"Error converting column to datatype {condition_datatype}: {e}")
        return pd.Series([True] * len(df), index=df.index)

    condition_value = condition['condition_value']
    # Convert condition_value to the specified datatype
    try:
        if condition_datatype == 'Integer':
            value = int(condition_value)
        elif condition_datatype == 'Numeric' or condition_datatype == 'Float':
            value = float(condition_value)
        elif condition_datatype == 'Character':
            value = str(condition_value)
        elif condition_datatype == 'Date':
            value = pd.to_datetime(condition_value)
        elif condition_datatype == 'Boolean':
            if condition_value in ["Yes", "TRUE", "1", 1, "YES", "true", "True"]:
                value = True        
            else:
                value = False
        else:
            logging.warning(f"Unknown condition_datatype: {condition_datatype}")
            return pd.Series([True] * len(df), index=df.index)
    except ValueError as e:
        logging.warning(f"ValueError: {e}")
        return pd.Series([True] * len(df), index=df.index)
    
    logging.warning(f" final condition_value post conversion's and all : {condition_value}")
    
    operation_type = condition['condition_type'].lower()

    if operation_type == 'contains':
        mapping_values = mapping_set[mapping_set['mapping_set'] == condition_value]['mapping_criteria'].tolist()
        # logging.warning(f"Mapping values for 'Contains': {mapping_values}")
        return df_column.isin(mapping_values)
    elif operation_type == 'noes not contain':
        mapping_values = mapping_set[mapping_set['mapping_set'] == condition_value]['mapping_criteria'].tolist()
        # logging.warning(f"Mapping values for 'Does not Contain': {mapping_values}")
        return ~df_column.isin(mapping_values)
    elif operation_type == 'equal to':
        return df_column == value
    elif operation_type == 'not equals':
        return df_column != value
    elif operation_type == 'greater than':
        return df_column > value
    elif operation_type == 'smaller than':
        return df_column < value
    elif operation_type == 'greater than equal to':
        return df_column >= value
    elif operation_type == 'smaller than equal to':
        return df_column <= value
    else:
        logging.warning(f"Unknown condition type: -{operation_type}-")
        # Return a Series of True with the same index as df
        return pd.Series([True] * len(df), index=df.index)


def filter_dataframes_by_currency(dataframes, currency_conversion_master, currency_list):
    """
    Filters dataframes based on the specified currency list.
    """
    logging.warning(f"Inside filter_dataframes_by_currency")
    for table_name, df in dataframes.items():
        logging.warning(f" table_name {table_name}")
        currency_col_series = currency_conversion_master[currency_conversion_master['table_name'] == table_name]['currency_column']
        if not currency_col_series.empty:
            currency_col = currency_col_series.values[0]
            dataframes[table_name] = df[df[currency_col].isin(currency_list)]
    return dataframes

def combine_bucketed_values(dict1, dict2, operation, operation_parameter=None):
    """
    Combines two bucketed values dictionaries based on the specified operation.
    """
    logging.warning(f"inside combine_bucketed_values fxn ")
    logging.warning(f"operation {operation}")
    combined = {}
    if operation == 'CUMULATIVE_SUM':
        # Use the mapping set to get the bucket order
        mapping_values = mapping_set[mapping_set['mapping_set'] == operation_parameter]['mapping_criteria'].tolist()
        logging.warning(f"Operation parameter (mapping set): {operation_parameter}")
        logging.warning(f"Mapping values (bucket order): {mapping_values}")

        # Merge dict1 and dict2
        combined_values = dict1.copy()
        for k, v in dict2.items():
            combined_values[k] = combined_values.get(k, 0) + v

        # Now perform cumulative sum
        cumulative_sum = 0
        cumulative_bucketed_values = {}
        for bucket_id in mapping_values:
            value = combined_values.get(bucket_id, 0)
            cumulative_sum += value
            cumulative_bucketed_values[bucket_id] = cumulative_sum
            logging.warning(f"Cumulative sum for {bucket_id}: {cumulative_sum}")

        combined = cumulative_bucketed_values
    else:
        all_bucket_ids = set(dict1.keys()).union(dict2.keys())
        logging.warning(f"All bucket IDs: {all_bucket_ids}")
        for bucket_id in all_bucket_ids:
            
            value1 = dict1.get(bucket_id, 0)
            value2 = dict2.get(bucket_id, 0)
            
            if value2 is None:
                value2 = 0.0

            if value1 is None:
                value1 = 0.0
            
            
            if operation == 'MULTIPLY':
               value1 = value1 if value1 is not None else 1
               value2 = value2 if value2 is not None else 1
               combined_value = value1 * value2
               # If both original values are None, set combined_value to 0
               if dict1.get(bucket_id, None) is None and dict2.get(bucket_id, None) is None:
                   combined_value = 0
               combined[bucket_id] = combined_value
            else:
               value1 = value1 if value1 is not None else 0
               value2 = value2 if value2 is not None else 0
                
            logging.warning(f"bucket_id {bucket_id} value1 {value1} - {operation} - value2 {value2}")
            
            if operation == 'ADD':
                combined[bucket_id] = value1 + value2
            elif operation == 'SUBTRACT':
                combined[bucket_id] = value1 - value2
            elif operation == 'MAX':
                combined[bucket_id] = max(value1, value2)
            elif operation == 'MIN':
                combined[bucket_id] = min(value1, value2)
            elif operation == 'ABS':
                combined[bucket_id] = abs(value1) + abs(value2)
            elif operation == 'DIVIDE':
                if value2 != 0:
                    combined[bucket_id] = value1 / value2
                else:
                    logging.warning(f"Division by zero for bucket_id {bucket_id}")
                    combined[bucket_id] = None  # Or set to zero or appropriate value
            else:
                logging.warning(f"Unknown operation: {operation}")
                combined[bucket_id] = None

    logging.warning(f"Combined bucketed values: {combined}")    
    return combined

# Group the scenarios by 'currency_scenario_id' and aggregate the 'currency_list' into a list
grouped_scenarios = currency_scenario_config.groupby(
    ['currency_scenario_id', 'reporting_currency', 'drill_down_report_flag', 'configuration_date']
).agg({'currency_list': lambda x: x.tolist()}).reset_index()

logging.warning("Starting processing for each unique scenario...")
scenario_indexer = 0
for idx, scenario in grouped_scenarios.iterrows():
    scenario_indexer += 1
    ##### For testing purposes, you can limit the number of scenarios processed
    # if scenario_indexer > 1:
    #     break

    start_time = time.time()
    scenario_analysis_id = scenario['currency_scenario_id']
    currency_list_values = scenario['currency_list']
    reporting_currency = scenario['reporting_currency']
    drill_down_report_flag = scenario['drill_down_report_flag']
    reporting_date = scenario['configuration_date']
    logging.warning(f"\nProcessing scenario: {scenario_analysis_id}")
    logging.warning(f"Currency list: {currency_list_values}, Reporting currency: {reporting_currency}, Drill report flag: {drill_down_report_flag}, Reporting date: {reporting_date}")

    # Process currency_list_values to create a flat list
    currency_list = []
    for currency_str in currency_list_values:
        currency_list.extend([x.strip() for x in str(currency_str).split(',')])
    currency_list = list(set(currency_list))  # Remove duplicates
    logging.warning(f"Processed currency list: {currency_list}")

    # Prepare output file paths
    file_path_rule = file_path_rule_template.format(scenario_analysis_id)
    file_path_rule_grp = file_path_rule_grp_template.format(scenario_analysis_id)
    file_path_ans = file_path_ans_template.format(scenario_analysis_id)
    drill_down_file_path = fr"{location}\drill_down_report_{scenario_analysis_id}.xlsx"

    # Initialize DataFrames for this scenario
    rule_group_def_scenario = rule_group_def.copy()
    rule_def_scenario = rule_def.copy()
    report_format_scenario = report_format.copy()
    currency_conversion_master_scenario = currency_conversion_master.copy()

    # Adjust columns in rule_def_scenario
    rule_group_def_scenario['final_value'] = np.nan
    rule_def_scenario['final_value'] = np.nan
    rule_def_scenario['condition_column_name'] = rule_def_scenario['condition_source_table'] + "_+_" + rule_def_scenario['condition_column_name']
    rule_def_scenario['value_source_column'] = rule_def_scenario['value_source_table'] + "_+_" + rule_def_scenario['value_source_column']
    currency_conversion_master_scenario['currency_column'] = currency_conversion_master_scenario['table_name'] + "_+_" + currency_conversion_master_scenario['currency_column']

    # Prepare currency conversion data for this scenario
    currency_pair = currency_pair_master[currency_pair_master['to_currency'] == reporting_currency]
    currency_conversion_rate = currency_pair.merge(quoted_security_data, left_on='currency_pair', right_on='security_identifier', how='left')

    # Read DataFrames from Excel
    dataframes = read_dataframes(org_path)

    # Filter dataframes based on currency_list,  source_filtering_logic
    
    grouped_data = group_filter_data()
    dataframes = filter_dataframes_by_grouped_data(dataframes, grouped_data)
    
    dataframes = filter_dataframes_by_currency(dataframes, currency_conversion_master_scenario, currency_list)
    # Perform currency conversion
    base = rule_def_scenario[['value_source_table', 'value_source_column']].drop_duplicates()
    dataframes = conversion(dataframes, currency_conversion_master_scenario, currency_conversion_rate, base, reporting_currency)

    # Perform dynamic merge
    merged_data = dynamic_merge(merge_master, dataframes)
    # raise Exception("break after dynamic merge")


    # for key, df in dataframes.items():
    #     time.sleep(1)
    #     logging.warning(f"The shape of the dataframe '{key}' is: {df.shape}")
    #     df.to_csv(f"{key}_data.csv", index = False)


    # for key, df in merged_data.items():
    #     time.sleep(1)
    #     logging.warning(f"The shape of the dataframe '{key}' is: {df.shape}")
    #     df.to_csv(f"{key}_.csv", index = False)

    # Get all dataframes
    all_dataframes_dict = get_all_dataframes_dict(merge_master, merged_data, dataframes)

    # Re-initialize drill_down_data and bucketed_values_dict
    drill_down_data = []    
    bucketed_values_dict = {}  # Stores bucketed values for each label_id

    # Create mapping from rule_group to bucketing_applicability
    logging.warning("Creating mapping from rule_group to bucketing_applicability...")
    rule_group_to_bucketing_applicability = {}
    for col_name in calculated_columns:
        bucketing_applicability = column_type[column_type['column_name'] == col_name]['bucketing_applicability'].values[0]
        for index, row in report_format_scenario.iterrows():
            rule_group = row[col_name]
            if pd.notna(rule_group):
                if rule_group in rule_group_to_bucketing_applicability:
                    if rule_group_to_bucketing_applicability[rule_group] != bucketing_applicability:
                        logging.warning(f"Warning: Inconsistent bucketing applicability for rule_group {rule_group}")
                else:
                    rule_group_to_bucketing_applicability[rule_group] = bucketing_applicability
    logging.warning("Mapping from rule_group to bucketing_applicability created.")

    label_id_column_to_bucketing_applicability = {}
    for index, row in report_format_scenario.iterrows():
        label_id = row['label_id']
        for col_name in calculated_columns:
            rule_group = row[col_name]
            if pd.notna(rule_group):
                bucketing_applicability = column_type[column_type['column_name'] == col_name]['bucketing_applicability'].values[0]
                label_id_column_to_bucketing_applicability[(label_id, col_name)] = bucketing_applicability

    # Create mapping from rule_group to label_ids
    logging.warning("Creating mapping from rule_group to label_ids...")
    rule_group_to_label_ids = {}
    for index, row in report_format_scenario.iterrows():
        label_id = row['label_id']
        for col_name in calculated_columns:
            rule_group_in_row = row[col_name]
            if pd.notna(rule_group_in_row):
                if rule_group_in_row in rule_group_to_label_ids:
                    rule_group_to_label_ids[rule_group_in_row].append(label_id)
                else:
                    rule_group_to_label_ids[rule_group_in_row] = [label_id]
    logging.warning("Mapping created successfully.")
    
    
    logging.warning(f"rule_group_to_bucketing_applicability: {rule_group_to_bucketing_applicability}")
    logging.warning(f"label_id_column_to_bucketing_applicability: {label_id_column_to_bucketing_applicability}")
    
    # Wrap dictionaries in a list to create DataFrames
    rule_group_df = pd.DataFrame([rule_group_to_bucketing_applicability])
    label_id_df = pd.DataFrame([label_id_column_to_bucketing_applicability])
    
    # Transpose the DataFrames
    rule_group_df_transposed = rule_group_df.T
    label_id_df_transposed = label_id_df.T
    
    # Optionally, you can reset the index and rename columns for clarity
    rule_group_df_transposed.reset_index(inplace=True)
    rule_group_df_transposed.columns = ['Key', 'Value']
    
    label_id_df_transposed.reset_index(inplace=True)
    label_id_df_transposed.columns = ['Key', 'Value']
    
    # Save the transposed DataFrames to CSV without the index column
    # rule_group_df_transposed.to_csv('rule_group_to_bucketing_applicability_pd.csv', index=False)
    # label_id_df_transposed.to_csv('label_id_to_bucketing_applicability_pd.csv', index=False)

    # Define functions that use scenario-specific variables
    def evaluate_rule_set(rule_set, rule_group):
        """
        Evaluates a rule set and returns bucketed values and final value.
        """
        logging.warning(f"Evaluating rule set: {rule_set}")
        bucketing_applicability = rule_group_to_bucketing_applicability.get(rule_group, 'Yes')
        if bucketing_flag_global == "No":
            bucketing_applicability = "No"
      
        conditions = rule_def_scenario[rule_def_scenario['condition_rule_set'] == rule_set]
        if len(conditions) < 1:
            logging.warning(f"Missing rule set: {rule_set}")
            logging.warning("Returning default value 0.")
            return {}, 0  # Return empty bucket dict and zero value
    
        sheet_name = conditions['value_source_table'].iloc[0]
        if sheet_name not in all_dataframes_dict:
            logging.warning(f"Missing sheet: {sheet_name}")
            logging.warning("Returning default value 0.")
            return {}, 0
    
        df = all_dataframes_dict[sheet_name]
        if df is None:
            logging.warning(f"Dataframe for sheet {sheet_name} is None")
            logging.warning("Returning default value 0.")
            return {}, 0
    
        # Apply conditions to filter the DataFrame
        for i in range(len(conditions)):
            logging.warning("   ")
            condition = conditions.iloc[i]
            logging.warning(f"Applying condition: {condition}")
            column_name = condition['condition_column_name']
            if column_name not in df.columns:
                logging.warning(f"Missing column: {column_name}  in {sheet_name}")
                logging.warning("Returning default value 0.")
                return {}, 0
    
            condition_result = evaluate_condition(df, condition)
            logging.warning("Filtering based on condition")
            df = df.reset_index(drop=True)
            
            logging.warning(f"before condition lenght of resultant df {len(df)}")
            condition_result = condition_result.reset_index(drop=True)
            df = df[condition_result]
            
            logging.warning(f"after condition lenght of resultant df {len(df)}")
            
            # logging.warning(f"after  {df}")
            
            if df.empty:
                logging.warning("DataFrame is empty after applying condition.")
                break
    
        if df.empty:
            logging.warning("No data after applying all conditions.")
            return {}, 0
    
        # Perform the final operation
        operation_to_perform = conditions['condition_groupby_operation'].iloc[-1].lower()
        logging.warning(f"operation_to_perform {operation_to_perform}")
        
        logging.warning(f"      ")
        logging.warning(f"      ")
        logging.warning(f"      ")
        # df.to_csv("target_dataframe_.csv", index = False )
        logging.warning(f"      ")
        logging.warning(f"      ")
        logging.warning(f"      ")
        
        if operation_to_perform == 'weighted average' or  operation_to_perform == 'sum product':
            logging.warning("Performing weighted average or sum product  calculation")
            # Get the necessary columns from the last condition
            value_source_table = conditions['value_source_table'].iloc[-1]
            value_source_column = conditions['value_source_column'].iloc[-1]
            weight_source_table = conditions['weight_source_table'].iloc[-1]
            weight_source_column = conditions['weight_source_column'].iloc[-1]
            
            # Check if weight_source_table is in all_dataframes_dict
            if weight_source_table not in all_dataframes_dict:
                logging.warning(f"710 Missing weight source table: {weight_source_table}")
                return {}, 0
            
            weight_df = all_dataframes_dict[weight_source_table]
            
            # Get merge keys from merge_master
            merge_instructions = merge_master[
                (merge_master['value_source_table'] == value_source_table) & 
                (merge_master['condition_source_table'] == weight_source_table)
            ]
            
            logging.warning(f" both table's are same {value_source_table} and {weight_source_table}")
            
            
            value_column_full =  value_source_column
            weight_column_full = weight_source_table + "_+_" + weight_source_column
            
            logging.warning(f"  ")
            logging.warning(f"value_column_full {value_column_full}")
            logging.warning(f"weight_column_full {weight_column_full}")
            logging.warning(f"  ")
            
            if value_source_table != weight_source_table :
                if merge_instructions.empty:
                    logging.warning(f"No merge instructions found for {value_source_table} and {weight_source_table}")
                    return {}, 0
                
                left_keys = [value_source_table + "_+_" + key for key in merge_instructions['left_key']]
                right_keys = [weight_source_table + "_+_" + key for key in merge_instructions['right_key']]
                
                logging.warning(f"left_keys {left_keys}")
                logging.warning(f"right_keys {right_keys}")    
                
                # Merge df and weight_df
                if value_column_full not in df.columns and weight_column_full not in df.columns: #as my dataframes are already merege not imp
                    logging.warning(f"columns  found")
                    df = pd.merge(df, weight_df, left_on=left_keys, right_on=right_keys, how='left')
                
                logging.warning(f" name of the column's are {df.columns}")
                logging.warning(f"lenght of  df is  {len(df)}")
                
            else:
                logging.warning(f"both table's are same {value_source_table} and {weight_source_table}")
                
            
            # Check if columns exist
            if weight_column_full not in df.columns:
                logging.warning(f" weight columns not found after merge.")
                return {}, 0
            
            if value_column_full not in df.columns :
                logging.warning(f"Value columns not found after merge.")
                return {}, 0    
                       
            
            # Calculate weighted average
            logging.warning(f"Columns: {list(df.columns)}")
            logging.warning(f"Length of df: {len(df)}")
 
            # Check dtypes
            logging.warning(f"Dtypes:\n{df.dtypes}")
            logging.warning(f"Dtypes:\n{df.shape}")
 
            # Convert to float if necessary
            df[value_column_full] = df[value_column_full].astype(float)
            df[weight_column_full] = df[weight_column_full].astype(float)
 
            try:
                start = time.time()
                a = df[value_column_full].sum()
                logging.warning(f"Summation '{value_column_full}' took {time.time()-start:.2f}s; result={a}")
 
                start = time.time()
                b = df[weight_column_full].sum()
                logging.warning(f"Summation '{weight_column_full}' took {time.time()-start:.2f}s; result={b}")
 
                start = time.time()
                weighted_sum = (df[value_column_full] * df[weight_column_full]).sum()
                logging.warning(f"Weighted_sum took {time.time()-start:.2f}s; result={weighted_sum}")
 
                total_weight = b  # or df[weight_column_full].sum() again, but we already have b
                logging.warning(f"total_weight = {total_weight}")

                df_copy = df.copy(deep=True)
                df_copy[value_column_full] = (df[value_column_full] * df[weight_column_full])
                df = df_copy
              
                if operation_to_perform == 'weighted average' and total_weight != 0:
                    final_value = weighted_sum / total_weight
                    logging.warning(f"weighted average final_value {final_value}")
                elif operation_to_perform == 'sum product':
                    final_value = weighted_sum
                    logging.warning(f"sum product final_value {final_value}")
                else:
                    logging.warning("Total weight is zero or unknown operation.")
                    final_value = 0
 
            except Exception as e:
                logging.warning(f"Error during calculation: {e}")
                final_value = 0
            
        else:
          
          column_name = conditions['value_source_column'].iloc[-1]
          df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
          logging.warning(f" conveting numerical values ")
          final_value = df[conditions['value_source_column'].iloc[-1]].agg(operation_to_perform)
            
        rule_def_scenario.loc[conditions.index, 'final_value'] = final_value
        logging.warning(f"Final value for {rule_set} is {final_value}")
    
        # Apply bucketing if applicable
        bucketed_values = {}  # Dictionary to hold bucketed values
        
        logging.warning(f"Final value for {rule_set} is {final_value}  -- bucketing_applicability {bucketing_applicability}  ")
        if bucketing_applicability == 'Yes':
            # Get the bucketing_rule_set for this rule_id
            bucketing_rule_row = bucket_rule_mapping[bucket_rule_mapping['reporting_rule_id'] == rule_set]
            if not bucketing_rule_row.empty:
                bucketing_rule_set = bucketing_rule_row['bucketing_rule'].values[0]
                adjustment_rule = bucketing_rule_row.get('adjustment_rule', np.nan).values[0]
                # Get the bucketing type
                bucketing_type_row = bucketing_type[bucketing_type['bucketing_rule_set'] == bucketing_rule_set]
                if not bucketing_type_row.empty:
                    bucketing_type_value = bucketing_type_row['bucketing_type'].values[0]
                    if bucketing_type_value == 'rule_based_bucketing':
                        # Apply rule-based bucketing
                        bucketed_values = apply_rule_based_bucketing(df, bucketing_rule_set, conditions['value_source_column'].iloc[-1], adjustment_rule)
                    elif bucketing_type_value == 'static_pattern_bucketing':
                        # Apply static pattern bucketing
                        bucketed_values = apply_static_pattern_bucketing(final_value, bucketing_rule_set)
                    elif bucketing_type_value == 'reporting_pattern_bucketing':
                        # Apply reporting pattern bucketing
                        bucketed_values = apply_reporting_pattern_bucketing(rule_set)
                    else:
                        logging.warning(f"Unknown bucketing type: {bucketing_type_value}")
                        bucketed_values['Unbucketed'] = final_value
                else:
                    logging.warning(f"No bucketing type found for bucketing_rule_set: {bucketing_rule_set}")
                    bucketed_values['Unbucketed'] = final_value
            else:   
                logging.warning(f"No bucketing rule mapping found for rule_id: {rule_set}")
                bucketed_values['Unbucketed'] = final_value
        else:
            # Bucketing is not applicable; assign the final value to 'Unbucketed'
            bucketed_values['Unbucketed'] = final_value
    
        # Collect data for drill-down report if required
        if str(drill_down_report_flag).lower() == 'yes':
            collect_drill_down_data(df, rule_set, rule_group, conditions['value_source_column'].iloc[-1], bucketed_values)
        
        # Store the bucketed values in rule_def_scenario
        logging.warning(f"Storing bucketed values in rule_def_scenario for rule_set: {rule_set}  bucketing_applicability {bucketing_applicability}")
        rule_def_indices = conditions.index
        if bucketing_applicability == 'Yes':
            for bucket_id in bucket_ids:
                value = bucketed_values.get(bucket_id, 0)
                rule_def_scenario.loc[rule_def_indices, bucket_id] = value
        
        logging.warning(f"Returning evaluate_rule_set with bucketed_values {bucketed_values}")
        logging.warning(f"Returning evaluate_rule_set with final_value {final_value}")
        return bucketed_values, final_value

    def collect_drill_down_data(df, rule_set, rule_group, value_source_column, bucketed_values):
        """
        Collects data for the drill-down report.
        """
        logging.warning(f"Collecting drill-down data for rule_set: {rule_set}")
        sheet_name = df.columns[0].split('_+_')[0]  # Extract table_name from column names
        currency_col_name_series = currency_conversion_master_scenario[
            currency_conversion_master_scenario['table_name'] == sheet_name
        ]['currency_column']
        if not currency_col_name_series.empty:
            currency_col_name = currency_col_name_series.values[0]
            unique_identifier_col_series = table_primary_keys[
                table_primary_keys['source_table_name'] == sheet_name
            ]['Primary key']
            if not unique_identifier_col_series.empty:
                unique_identifier_col = unique_identifier_col_series.values[0]
                drill_down_df = df.copy()
                drill_down_df['unique_identifier'] = df[unique_identifier_col].values
                drill_down_df['currency_column'] = currency_col_name
                drill_down_df['base_currency'] = df[currency_col_name].values
                drill_down_df['Amount_column'] = value_source_column
                drill_down_df['amount_value'] = df[value_source_column].values
                drill_down_df['reporting_date'] = reporting_date
                drill_down_df['reporting_currency'] = reporting_currency
                drill_down_df['rule_grp'] = rule_group
                drill_down_df['rule_id'] = rule_set
                drill_down_df['table_name'] = sheet_name
                drill_down_df['column name'] = value_source_column
                # Add bucket_id if available
                if 'bucket_id' in df.columns:
                    drill_down_df['bucket_id'] = df['bucket_id']
                else:
                    drill_down_df['bucket_id'] = 'Unbucketed'
                drill_down_data.append(drill_down_df)
            else:
                logging.warning(f"No unique identifier column found for table {sheet_name}")
        else:
            logging.warning(f"No currency_column found for table {sheet_name}")

    def apply_rule_based_bucketing(df, bucketing_rule_set, value_source_column, adjustment_rule=None):
        """
        Applies rule-based bucketing to the DataFrame and returns bucketed values.
        """
        logging.warning(f"Applying rule-based bucketing for bucketing_rule_set: {bucketing_rule_set}")
        bucketing_conditions = rule_based_bucketing[rule_based_bucketing['bucketing_rule_set'] == bucketing_rule_set]
        bucketed_data = []
        
        for bucket_id in bucketing_conditions['bucket_id'].unique():
            bucket_conditions = bucketing_conditions[bucketing_conditions['bucket_id'] == bucket_id]
            temp_df = df.copy()
        
            # Apply conditions for each bucket
            for idx, condition in bucket_conditions.iterrows():
                logging.warning(f"  ")
                logging.warning(f"  ")
                if 'tenor' in condition and 'tenor_unit' in condition:
                    if pd.notnull(condition['tenor']) and pd.notnull(condition['tenor_unit']):
                        tenor_unit = condition['tenor_unit'].lower()  # Normalize to lowercase
                        tenor_value = int(condition['tenor'])
                
                        if tenor_unit == 'd':  # Days
                            new_date = reporting_date + pd.Timedelta(days=tenor_value)
                        elif tenor_unit == 'm':  # Months
                            new_date = reporting_date + relativedelta(months=tenor_value)
                        elif tenor_unit == 'q':  # Quarters
                            new_date = reporting_date + relativedelta(months=tenor_value * 3)
                        elif tenor_unit == 'y':  # Years
                            new_date = reporting_date + relativedelta(years=tenor_value)
                        else:
                            new_date = None
                            calculated_time_to_maturity = None
                            logging.warning(f"Invalid tenor_unit: {tenor_unit}")
                
                        if new_date is not None:
                            if condition['condition_datatype'] == 'Date':
                                condition['condition_value'] = new_date
                                logging.warning(f"Calculated date: {new_date}")
                            elif condition['condition_datatype'] == 'Numeric':
                                days_difference = (new_date - reporting_date).days
                                calculated_time_to_maturity = days_difference / 365
                                condition['condition_value'] = calculated_time_to_maturity
                                logging.warning(f"Calculated time to maturity: {calculated_time_to_maturity}")
                            else:
                                condition['condition_value'] = None
                                logging.warning("Failed to calculate condition value due to invalid condition dataype.")    
                        else:
                            condition['condition_value'] = None
                            logging.warning("Failed to calculate new_date due to invalid tenor_unit.")
                    else:
                        logging.warning("Tenor or tenor_unit is null.")
                else:
                    logging.warning("Tenor or tenor_unit is missing from condition.")
                
                
                logging.warning(f" apply rule based bucketing before filter lenght {len(temp_df)}")
                # logging.warning(f" print temp_df {temp_df}")
                condition_result = evaluate_condition(temp_df, condition)
                temp_df = temp_df[condition_result]
                
                logging.warning(f" apply rule based bucketing  after filter lenght {len(temp_df)}")
                
                
                if temp_df.empty:
                    break
    
            if not temp_df.empty:
                # Tag the data with bucket_id for drill-down report
                temp_df['bucket_id'] = bucket_id
                bucketed_data.append(temp_df)
        
        # logging.warning(f" bucketed_data: {bucketed_data}")
        if bucketed_data:
            # Combine all bucketed data
            df_bucketed = pd.concat(bucketed_data, ignore_index=True)
        else:
            # If no bucketed data, create an empty df_bucketed
            df_bucketed = pd.DataFrame(columns=df.columns.tolist() + ['bucket_id'])
        
        # Apply adjustments if adjustment_rule is provided
        if pd.notna(adjustment_rule):
            logging.warning(f"Applying adjustments using adjustment_rule: {adjustment_rule}")
            adjusted_bucketed_values, adjusted_df = apply_bucket_adjustments(df_bucketed, value_source_column, adjustment_rule)
            # Logging the bucketed values after adjustment
            logging.warning(f"Bucketed values after adjustment: {adjusted_bucketed_values}")
            # Update df_bucketed with adjusted_df
            df_bucketed = adjusted_df
        else:
            logging.warning("No adjustment rule provided.")
            # Calculate bucketed_values from df_bucketed
            adjusted_bucketed_values = df_bucketed.groupby('bucket_id')[value_source_column].sum().to_dict()
        
        # Logging the final bucketed values
        logging.warning(f"Final bucketed values: {adjusted_bucketed_values}")
        
        # Return adjusted_bucketed_values
        return adjusted_bucketed_values
        
    def apply_bucket_adjustments(df, value_source_column, adjustment_rule):
        """
        Applies adjustments to the bucketed values based on the adjustment_rule.
        """
        logging.warning(f"Applying bucket adjustments for adjustment_rule: {adjustment_rule}")
        adjustments = reporting_bucketing_adjustment[
            reporting_bucketing_adjustment['bucketing_rule_id'] == adjustment_rule
        ]
        adjusted_df = df.copy()
        # logging.warning(f" column name is  {adjusted_df.columns}  ")
    
        # Log initial bucketed values
        initial_bucketed_values = adjusted_df.groupby('bucket_id')[value_source_column].sum().to_dict()
        logging.warning(f"Initial bucketed values: {initial_bucketed_values}")
    
        for idx, row in adjustments.iterrows():
            logging.warning(f"    ")

            from_bucket = row['from_bucket_id']
            to_bucket = row['to_bucket_id']
            if from_bucket == to_bucket:
                logging.warning(f" {from_bucket}  movement in   {to_bucket}")
                logging.warning(f" Skipped as netting was taken into account the movemnet it quafiled ")
                continue
            
            impact_value = row['impact_value']
            condition_operation = row.get('condition_operation', 'And').strip().lower()
    
            logging.warning(f"Processing adjustment {idx}: from_bucket {from_bucket} to_bucket {to_bucket}")
    
            # Get the condition
            condition = {
                'condition_source_table': row['condition_source_table'],
                'condition_column_name': row['condition_column_name'],
                'condition_datatype': row['condition_datatype'],
                'condition_type': row['condition_type'],
                'condition_value': row['condition_value']
            }
    
            # Build full condition column name
            condition_column_full = f"{condition['condition_source_table']}_+_{condition['condition_column_name']}"
            condition['condition_column_name'] = condition_column_full
    
            logging.warning(f"Condition column full name: {condition_column_full}")
    
            # Check if the column exists
            if condition_column_full not in adjusted_df.columns:
                logging.warning(f"Condition column {condition_column_full} not found in df")
                continue
    
            # Apply condition only to rows in from_bucket
            from_bucket_mask = adjusted_df['bucket_id'] == from_bucket
            df_from_bucket = adjusted_df[from_bucket_mask]
            
            logging.warning(f"Before condition filter, from_bucket {from_bucket} has {len(df_from_bucket)} rows")
    
            if df_from_bucket.empty:
                logging.warning(f"No data in from_bucket {from_bucket}")
                continue
    
            # Apply condition to df_from_bucket
            condition_result = evaluate_condition(df_from_bucket, condition)
            df_condition_met = df_from_bucket[condition_result]
    
            logging.warning(f"After condition filter, {len(df_condition_met)} rows meet the condition")
    
            if df_condition_met.empty:
                logging.warning(f"No rows in from_bucket {from_bucket} satisfy the condition")
                continue
    
            adjustment_indices = df_condition_met.index
    
            # Calculate amount to move
            initial_values = adjusted_df.loc[adjustment_indices, value_source_column]
            amount_to_move = initial_values * impact_value
    
            logging.warning(f"Initial values in from_bucket {from_bucket} for adjustment: {initial_values.sum()}")
            logging.warning(f"Impact value: {impact_value}, amount to move: {amount_to_move.sum()}")
    
            # If from_bucket == to_bucket and impact_value == 1, no adjustment needed
            if from_bucket == to_bucket and impact_value == 1:
                logging.warning(f"No adjustment needed for from_bucket {from_bucket} to_bucket {to_bucket} with impact_value 1")
                continue
    
            # Subtract amount from from_bucket
            adjusted_df.loc[adjustment_indices, value_source_column] -= amount_to_move
    
            # Ensure no negative values after subtraction
            adjusted_df.loc[adjustment_indices, value_source_column] = adjusted_df.loc[adjustment_indices, value_source_column].clip(lower=0)
    
            logging.warning(f"After subtraction, total in from_bucket {from_bucket}: {adjusted_df.loc[from_bucket_mask, value_source_column].sum()}")
    
            # Add amount to to_bucket
            if from_bucket != to_bucket:
                # Create a copy of the rows to add to to_bucket
                moved_rows = adjusted_df.loc[adjustment_indices].copy()
                moved_rows['bucket_id'] = to_bucket
                moved_rows[value_source_column] = amount_to_move
                adjusted_df = pd.concat([adjusted_df, moved_rows], ignore_index=True)
    
                logging.warning(f"Moved {amount_to_move.sum()} from bucket {from_bucket} to bucket {to_bucket}")
            else:
                # When from_bucket == to_bucket and impact_value != 1
                # Adjust the values in place
                adjusted_df.loc[adjustment_indices, value_source_column] += amount_to_move
    
                logging.warning(f"Adjusted values in bucket {from_bucket} with impact_value {impact_value}")
    
        # Recalculate bucketed_values from adjusted_df
        adjusted_bucketed_values = adjusted_df.groupby('bucket_id')[value_source_column].sum().to_dict()
    
        logging.warning(f"Final bucketed values after adjustments: {adjusted_bucketed_values}")
        return adjusted_bucketed_values, adjusted_df


    def apply_static_pattern_bucketing(final_value, bucketing_rule_set):
        """
        Applies static pattern bucketing to the final value.
        """
        logging.warning(f"Applying static pattern bucketing for bucketing_rule_set: {bucketing_rule_set}")
        static_buckets = static_pattern_bucketing[static_pattern_bucketing['bucketing_rule_set'] == bucketing_rule_set]
        bucketed_values = {}

        for idx, row in static_buckets.iterrows():
            
            percentage = float(row['percentage']) / 100
            logging.warning(f" percentage at {idx} : {percentage}")
            bucket_id = row['bucket_id']
            bucketed_values[bucket_id] = final_value * percentage

        return bucketed_values

    def apply_reporting_pattern_bucketing(rule_set):
        """
        Applies reporting pattern bucketing by inheriting from another rule set.
        """
        logging.warning(f"Applying reporting pattern bucketing for rule_set: {rule_set}")
        reporting_pattern = reporting_pattern_bucketing[reporting_pattern_bucketing['rule_id'] == rule_set]
        bucketed_values = {}

        if not reporting_pattern.empty:
            inherit_rule_id = reporting_pattern['inherit_rule_id'].values[0]
            # Retrieve bucketed values from the inherited rule_id
            inherited_values = bucketed_values_dict.get(inherit_rule_id, {})
            if inherited_values:
                bucketed_values = inherited_values
            else:
                logging.warning(f"No bucketed values found to inherit from rule_id: {inherit_rule_id}")
        else:
            logging.warning(f"No reporting pattern found for rule_set: {rule_set}")

        return bucketed_values

    def evaluate_rule_group(rule_group):
        """
        Evaluates a rule group and returns combined bucketed values and final value.
        """
        logging.warning(f"Evaluating rule group: {rule_group}")
        bucketing_applicability = rule_group_to_bucketing_applicability.get(rule_group, 'Yes')
        logging.warning(f"Evaluating rule group: {rule_group} and bucketing_applicability: {bucketing_applicability}")
        group_def = rule_group_def_scenario[rule_group_def_scenario['rule_group'] == rule_group]
        
        try:
            logging.warning(f'Rule Group Def head before sorting : {group_def.head()}')    
            group_def['execution_order'].fillna(100000, inplace=True)
            group_def['execution_order'] = group_def['execution_order'].astype(int)
            group_def.sort_values('execution_order', ascending=True, inplace=True)
            logging.warning(f'Rule Group Def head after sorting : {group_def.head()}') 
        except KeyError:
            logging.warning('Execution order is not found')  

        if len(group_def) < 1:
            logging.warning(f"Missing rule group: {rule_group}")
            logging.warning("Returning default value 0.")
            rule_group_def_scenario.loc[group_def.index, 'final_value'] = 0
            return {}, 0  # Return empty bucket dict and zero value
    
        if pd.notna(group_def['final_value'].iloc[0]):
            # If final_value is already calculated, return it along with stored bucketed values
            group_def_indices = group_def.index
            logging.warning(f"Values are already stored")
            stored_bucketed_values = {}
            if bucketing_applicability == 'Yes':
                for bucket_id in bucket_ids:
                    value = rule_group_def_scenario.loc[group_def_indices, bucket_id].values[0]
                    stored_bucketed_values[bucket_id] = value if pd.notna(value) else 0
            else:
                # If bucketing is not applicable, get the 'Unbucketed' value
                value = rule_group_def_scenario.loc[group_def_indices, 'Unbucketed'].values[0]
                stored_bucketed_values['Unbucketed'] = value if pd.notna(value) else 0
            return stored_bucketed_values, group_def['final_value'].iloc[0]
    
        combined_bucketed_values = {}
        combined_final_value = None  # Start with None for operations like MULTIPLY
    
        # For operations involving two operands, we need to handle them differently
        if len(group_def) == 1 and group_def['rule_group_operation'].iloc[0] in ['MULTIPLY']:
            operation = group_def['rule_group_operation'].iloc[0]
            sub_rule_group_1 = group_def['sub_rule_group'].iloc[0]
            sub_rule_group_2 = group_def['operation_parameter'].iloc[0]
    
            # Evaluate both sub_rule_groups
            bucketed_values_1, value_1 = evaluate_rule_group(sub_rule_group_1)
            bucketed_values_2, value_2 = evaluate_rule_group(sub_rule_group_2)
    
            # Combine bucketed values based on the operation
            combined_bucketed_values = combine_bucketed_values(
                bucketed_values_1, bucketed_values_2, operation
            )
    
            # Combine final values
            if operation == 'MULTIPLY':
                value_1 = value_1 if value_1 is not None else 1
                value_2 = value_2 if value_2 is not None else 1
                combined_final_value = value_1 * value_2
                # If both values are None, set to 0
                if value_1 == 1 and value_2 == 1:
                    combined_final_value = 0
            elif operation == 'DIVIDE':
                if value_2 != 0 and value_2 is not None:
                    combined_final_value = (value_1 if value_1 is not None else 0) / value_2
                else:
                    logging.warning("Division by zero or None encountered.")
                    combined_final_value = 0
            else:
                logging.warning(f"Unknown operation: {operation}")
                combined_final_value = 0
    
        else:
            ii = 0
            for _, row in group_def.iterrows():
                logging.warning(f" FOR loop start combined_final_value - {combined_final_value} at {ii}")
    
                if pd.notna(row['rule_set']):
                    bucketed_values, value = evaluate_rule_set(row['rule_set'], rule_group)
                elif pd.notna(row['sub_rule_group']):
                    # Recursively evaluate the sub-rule group and get its bucketed values and final value
                    bucketed_values, value = evaluate_rule_group(row['sub_rule_group'])
                else:
                    logging.warning("No rule_set or sub_rule_group specified.")
                    bucketed_values, value = {}, 0
    
                operation = row['rule_group_operation']
                logging.warning(f" bucketed_values : {bucketed_values} value : {value} at {ii}")
    
                # Combine bucketed values based on the operation
                logging.warning(f" operation : {operation} combined_bucketed_values : {combined_bucketed_values}")
                # Pass operation_parameter if needed
                # operation_parameter = row.get('operation_parameter', None)
                operation_parameter = row['operation_parameter']
                logging.warning(f" bucketing_applicability : {bucketing_applicability}  1069")
                logging.warning(f"Operation parameter (mapping set): {operation_parameter}  1070")
                logging.warning(f" bucketing_applicability : {bucketing_applicability}  line number  1071 ")
        
    
                if operation == 'RANGE_SUM':
                    logging.warning(f"Inside RANGE_SUM")
                    # Implement the RANGE_SUM operation
                    mapping_set_id = operation_parameter
                    mapping_values = mapping_set[mapping_set['mapping_set'] == mapping_set_id]['mapping_criteria'].tolist()
                    logging.warning(f"Operation parameter (mapping set): {mapping_set_id}")
                    logging.warning(f"Mapping values (bucket IDs to sum): {mapping_values}")
                    # Sum the values of the specified bucket IDs from bucketed_values
                    range_sum_value = sum(bucketed_values.get(bucket_id, 0) for bucket_id in mapping_values)
                    combined_final_value = range_sum_value
                    # For RANGE_SUM, we can set combined_bucketed_values to the summed value
                    combined_bucketed_values = {'Unbucketed': combined_final_value}
                    logging.warning(f"Inside RANGE_SUM range_sum_value {range_sum_value}")
                else:
                    combined_bucketed_values = combine_bucketed_values(
                        combined_bucketed_values, bucketed_values, operation,operation_parameter
                    )
                    # Combine final values
                    if operation == 'ADD':
                        if combined_final_value is None:
                            combined_final_value = value
                        else:
                            combined_final_value += value
                    elif operation == 'SUBTRACT':
                        if combined_final_value is None:
                            combined_final_value = value
                        else:
                            combined_final_value -= value
                    elif operation == 'MAX':
                        if combined_final_value is None:
                            combined_final_value = value
                        else:
                            combined_final_value = max(combined_final_value, value)
                    elif operation == 'MIN':
                        if combined_final_value is None:
                            combined_final_value = value
                        else:
                            combined_final_value = min(combined_final_value, value)
                    elif operation == 'ABS':
                        if combined_final_value is None:
                            combined_final_value = abs(value)
                        else:
                            combined_final_value += abs(value)
                    elif operation == 'DIVIDE':
                        if value != 0 and value is not None:
                            if combined_final_value is None:
                                combined_final_value = value
                            else:
                                combined_final_value /= value
                        else:
                            logging.warning("Division by zero or None encountered.")
                    elif operation == 'CUMULATIVE_SUM':
                        logging.warning("Inside CUMULATIVE_SUM")
                        operation_parameter = row['operation_parameter']
                        logging.warning(f"Operation parameter (mapping set): {operation_parameter}")
                        if bucketing_applicability == 'Yes':
                            bucketed_values = combined_bucketed_values
                            if bucketed_values:
                                combined_final_value = list(bucketed_values.values())[-1]
                            else:
                                combined_final_value = 0
                        else:
                            logging.warning("Bucketing is not applicable; cannot perform cumulative sum.")
                    else:
                        logging.warning(f"Unknown operation: {operation}")
    
                logging.warning(f" FOR loop end combined_final_value - {combined_final_value} at {ii}")
                ii += 1

        # If combined_final_value is still None after processing, set it to 0
        if combined_final_value is None:
            combined_final_value = 0
            
        # Perform limit checks and store breach results immediately after calculating the combined_final_value
        logging.warning(f"Performing limit checks for rule_group: {rule_group}")
        limit_rows = limit_setup[limit_setup['rule_group'] == rule_group]
        if not limit_rows.empty:
            for idx, limit_row in limit_rows.iterrows():
                limit_value = limit_row['limit_value']
                limit_condition = limit_row['limit_condition']
                true_value = limit_row['true_value']
                false_value = limit_row['false_value']
                column_name_breach = limit_row['column_identifier']
                # Use 'breach_test' as the column name
                breach_column_name = column_name_breach + "_" + 'breach_identifier'
                # Value to check is combined_final_value
                value_to_check = combined_final_value
                # Perform limit check with detailed logging.warning statements
                logging.warning(f"Checking limit condition for rule_group: {rule_group}")
                logging.warning(f"Value to check: {value_to_check}")
                logging.warning(f"Limit condition: {limit_condition} {limit_value}")
                breach_result = false_value  # Default to false_value
                if limit_condition == 'Greater than':
                    if value_to_check > limit_value:
                        breach_result = true_value
                elif limit_condition == 'Smaller than':
                    if value_to_check < limit_value:
                        breach_result = true_value
                elif limit_condition == 'Equal to':
                    if value_to_check == limit_value:
                        breach_result = true_value
                else:
                    logging.warning(f"Unknown limit_condition: {limit_condition}")
                    logging.warning(f"Breach result for rule_group {rule_group}: {breach_result}")
                # Store the breach result in rule_group_def_scenario
                rule_group_def_scenario.loc[group_def.index, breach_column_name] = breach_result
                # Also store in report_format_scenario
                # For all calculated columns
                for col in calculated_columns:
                    if rule_group in report_format_scenario[col].values:
                        report_format_scenario.loc[report_format_scenario[col] == rule_group, breach_column_name] = breach_result
        else:
            logging.warning(f"No limit checks defined for rule_group: {rule_group}")

        # Apply threshold criteria if any
        threshold_criteria = group_def.iloc[0]['threshold_criteria']
        threshold_value = group_def.iloc[0]['threshold_value']
        threshold_source_table = group_def.iloc[0]['threshold_source_table']
        threshold_filter_column = group_def.iloc[0]['threshold_filter_column']
        threshold_filter_value = group_def.iloc[0]['threshold_filter_value']
        
        logging.warning(f"Applying threshold criteria: {threshold_criteria} with threshold value: {threshold_value}")
        try:
            threshold_value = float(threshold_value)
            logging.warning(f" converted values to flaot  {threshold_value}")
        except (ValueError, TypeError):
            logging.warning(f" passing as thrsohold is string cant be converted {threshold_value}")
            pass

        # Fetch dynamic threshold value if necessary
        if isinstance(threshold_value, str) and not threshold_value.replace('.', '', 1).isdigit():
            # Fetch the threshold value from the specified table and filters
            if threshold_source_table and threshold_filter_column and threshold_filter_value and threshold_value:
                logging.warning(f"Fetching dynamic threshold value from table: {threshold_source_table}")
                if threshold_source_table in all_dataframes_dict:
                    threshold_df = all_dataframes_dict[threshold_source_table]
                    # Construct full column names
                    threshold_filter_column_full = f"{threshold_source_table}_+_{threshold_filter_column}"
                    threshold_value_column_full = f"{threshold_source_table}_+_{threshold_value}"
                    
                    if threshold_filter_column_full in threshold_df.columns and threshold_value_column_full in threshold_df.columns:
                        # Apply filter and fetch the threshold value
                        filtered_df = threshold_df[threshold_df[threshold_filter_column_full] == threshold_filter_value]
                        if not filtered_df.empty:
                            threshold_value = filtered_df[threshold_value_column_full].iloc[0]
                            logging.warning(f"Fetched threshold value: {threshold_value}")
                        else:
                            logging.warning("No matching rows found for threshold filter.")
                            threshold_value = 1
                    else:
                        logging.warning("Threshold filter column or value column not found in threshold source table.")
                        threshold_value = 0
                else:
                    logging.warning(f"Threshold source table {threshold_source_table} not found.")
                    threshold_value = 0
            else:
                # If 'Factor' is specified, handle accordingly
                if threshold_value == "factor":
                    # For all calculated columns
                    for col in calculated_columns:
                        factor_series = report_format_scenario[report_format_scenario[col] == rule_group]['factor']
                        if not factor_series.empty:
                            threshold_value = factor_series.iloc[0] / 100
                            logging.warning(f"Using factor from report_format: {threshold_value}")
                            break
                        else:
                            logging.warning("No factor found in report_format_scenario.")
                            threshold_value = 1  # Default factor
                else:
                    logging.warning("Threshold value is not a number or a recognized keyword.")
                    threshold_value = 1  # Default value
        else:
            # Convert threshold_value to float if it's a numeric string
            if threshold_value is not None:
                threshold_value = float(threshold_value)
        
        logging.warning(f" line 1035")
        # Apply thresholds to each bucketed value individually
        if threshold_criteria == 'Greater than':
            logging.warning(f" inside greater than")
            for bucket_id in combined_bucketed_values:
                if combined_bucketed_values[bucket_id] <= threshold_value:
                    combined_bucketed_values[bucket_id] = 0
            # Recalculate combined_final_value based on updated bucketed values
            combined_final_value = sum(combined_bucketed_values.values())
        elif threshold_criteria == 'Smaller than':
            logging.warning(f" inside smaller than")
            for bucket_id in combined_bucketed_values:
                logging.warning(f" bucket_id  {bucket_id} ")
                logging.warning(f" combined Bucked aleus  {combined_bucketed_values[bucket_id]} ")
                if combined_bucketed_values[bucket_id] >= threshold_value:
                    combined_bucketed_values[bucket_id] = 0
            # Recalculate combined_final_value based on updated bucketed values
            combined_final_value = sum(combined_bucketed_values.values())
        elif threshold_criteria == 'Multiply':
            combined_final_value *= threshold_value
            # Multiply each bucketed value
            for bucket_id in combined_bucketed_values:
                combined_bucketed_values[bucket_id] *= threshold_value
        elif threshold_criteria == 'Divide':
            if threshold_value != 0:
                combined_final_value /= threshold_value
                # Divide each bucketed value
                for bucket_id in combined_bucketed_values:
                    combined_bucketed_values[bucket_id] /= threshold_value
            else:
                logging.warning("Division by zero in threshold criteria.")

        # Store the final value
        rule_group_def_scenario.loc[group_def.index, 'final_value'] = combined_final_value
    
        logging.warning(f"Just after writing evaluate_rule_group, combined_final_value: {combined_final_value}")
    
        # Store bucketed values in rule_group_def_scenario
        logging.warning(f"Storing bucketed values in rule_group_def_scenario for rule_group: {rule_group}")
        group_def_indices = group_def.index
        if bucketing_applicability == 'Yes':
            for bucket_id in bucket_ids:
                value = combined_bucketed_values.get(bucket_id, 0)
                rule_group_def_scenario.loc[group_def_indices, bucket_id] = value
        else:
            ######## If bucketing is not applicable, store 'Unbucketed' value
            rule_group_def_scenario.loc[group_def_indices, 'Unbucketed'] = combined_final_value
    
        
        # Store bucketed values for the label_ids that use this rule_group
        label_ids = rule_group_to_label_ids.get(rule_group, [])
        for label_id in label_ids:
            for col_name in calculated_columns:
                if report_format_scenario.loc[report_format_scenario['label_id'] == label_id, col_name].values[0] == rule_group:
                    bucketed_values_dict[(label_id, col_name)] = combined_bucketed_values

    
        logging.warning(f"evaluate_rule_group fxn result combined_bucketed_values: {combined_bucketed_values}")
        logging.warning(f"evaluate_rule_group fxn result combined_final_value: {combined_final_value}")
    
        return combined_bucketed_values, combined_final_value

    # Copy of report_format to maintain original order
    final_report_format = report_format_scenario.copy()

    # Sort report_format_scenario based on 'execution_order'
    report_format_scenario['execution_order'].fillna(100000, inplace=True)
    report_format_scenario['execution_order'] = report_format_scenario['execution_order'].astype(int)
    report_format_scenario.sort_values('execution_order', ascending=True, inplace=True)

    # Evaluate calculated columns based on 'column_type' sheet
    col_name_indexer = 0
    for col_name in calculated_columns:
        logging.warning(f"   ")
        logging.warning(f"   ")
        logging.warning(f"   ")
        logging.warning(f"   ")
        col_name_indexer+=1
        if col_name_indexer > 1:
            # break
            pass
        
        logging.warning(f"Calculating column: {col_name}")
        bucketing_applicability = column_type[column_type['column_name'] == col_name]['bucketing_applicability'].values[0]
        
        loop_indexer = 0 
        
        for index, row in report_format_scenario.iterrows():
            loop_indexer+=1
            execution_order = report_format_scenario['execution_order'][index]
            logging.warning(f"   ")
            logging.warning(f"  loop_indexer   -- {loop_indexer}  execution_order {execution_order} col_name_indexer{col_name_indexer}")
            logging.warning(f"   ")
            logging.warning(f"   ")
            
            if col_name_indexer == 1 and loop_indexer > 38:
                # break
                pass
            
            rule_group = row[col_name]
            if pd.isna(rule_group):
                continue
            logging.warning(f"Evaluating value for rule group: {rule_group}")
            bucketed_values, final_value = evaluate_rule_group(rule_group)
            report_format_scenario.at[index, f'final_{col_name}'] = final_value

        logging.warning(f"Calculation done for column: {col_name} with col_name_indexer {col_name_indexer}")

    # Collect all columns that have 'breach_identifier' in their name
    breach_columns = [col for col in report_format_scenario.columns if 'breach_identifier' in col]
    logging.warning(f" breach_columns {breach_columns}")
    # Merge calculated values back to final_report_format
    merge_columns = ['label_id'] + [f'final_{col}' for col in calculated_columns] + breach_columns
    logging.warning(f" merge_columns {merge_columns}")

    final_report_format = final_report_format.merge(
        report_format_scenario[merge_columns], on='label_id', how='left'
    )

    # Read expected output and merge
    expected_output = pd.read_excel(org_path, sheet_name='Expected Output')
    # Keep only expected columns for calculated columns
    expected_columns = ['label_id'] + [f'expected_{col}' for col in calculated_columns]
    expected_output = expected_output[expected_columns]

    final_report_format = final_report_format.merge(expected_output, on='label_id', how='left')


    ###### Calculate differences between expected and final values
    # for col in calculated_columns:
    #     final_report_format[f'{col} Diff'] = final_report_format[f'final_{col}'] - final_report_format[f'expected_{col}']

    # logging.warning(f"Differences calculated for calculated columns.")

    # Add 'scenario_analysis_id' column
    final_report_format['scenario_analysis_id'] = scenario_analysis_id
    rule_group_def_scenario['scenario_analysis_id'] = scenario_analysis_id
    rule_def_scenario['scenario_analysis_id'] = scenario_analysis_id
    
    
    bucketed_values_dict_pd =  pd.DataFrame(bucketed_values_dict)
    # bucketed_values_dict_pd.to_csv('bucketed_values_dict_pd.csv', index=False)
    

    #########Add bucketed values to final report if bucketing is applicable
    if bucket_ids:
        logging.warning(f"Number of buckets in bucket_definition: {bucket_ids}")
        for bucket_id in bucket_ids:
            logging.warning(f"Iterating over bucket: {bucket_id}")
            for col_name in calculated_columns:
                # Create a column name for the bucketed values per calculated column
                bucket_column_name = f"{col_name}_{bucket_id}"
                final_report_format[bucket_column_name] = final_report_format.apply(
                    lambda row: bucketed_values_dict.get((row['label_id'], col_name), {}).get(bucket_id, 0)
                    if label_id_column_to_bucketing_applicability.get((row['label_id'], col_name), 'No') == 'Yes' else None,
                    axis=1
                )


    # Remove columns that are entirely None or NaN
    final_report_format.dropna(axis=1, how='all', inplace=True)

    # Save outputs
    # Remove existing files if necessary
    for file_path in [file_path_ans, file_path_rule, file_path_rule_grp]:
        if os.path.exists(file_path):
            os.remove(file_path)

    # Clean up DataFrames before saving
    final_report_format.dropna(axis=1, how='all', inplace=True)
    rule_group_def_scenario.dropna(axis=1, how='all', inplace=True)
    rule_def_scenario.dropna(axis=1, how='all', inplace=True)

    # Save the outputs
    final_report_format.to_excel(file_path_ans, index=False)
    rule_group_def_scenario.to_excel(file_path_rule_grp, index=False)
    rule_def_scenario.to_excel(file_path_rule, index=False)

    # Save drill-down report if drill_down_report_flag is 'Yes'
    if str(drill_down_report_flag).lower() == 'yes' and drill_down_data:
        # Combine drill_down_data
        drill_down_report = pd.concat(drill_down_data, ignore_index=True)
        # Add 'scenario_analysis_id' column
        drill_down_report['scenario_analysis_id'] = scenario_analysis_id
        new_order = [
            'scenario_analysis_id',
            'reporting_currency',
            'reporting_date',
            'rule_grp',
            'rule_id',
            'bucket_id',
            'table_name',
            'column name',
            'unique_identifier',
            'currency_column',
            'base_currency',
            'Amount_column',
            'amount_value'
        ]
        # Ensure all columns are present
        for col in new_order:
            if col not in drill_down_report.columns:
                drill_down_report[col] = np.nan
        drill_down_report = drill_down_report[new_order]
        # Save to Excel
        # drill_down_report.to_excel(drill_down_file_path, index=False)

    # logging.warning total time taken for the scenario
    logging.warning(f"Total time taken for scenario {scenario_analysis_id}: {time.time() - start_time} seconds")
    # End of scenario loop
logging.warning("Processing completed for all unique scenarios.")
output_data = final_report_format.copy()

