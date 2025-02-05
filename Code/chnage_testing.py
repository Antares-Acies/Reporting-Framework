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
        # df.to_csv("target_dataframe_247.csv", index = False )
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
                df_copy = df.copy(deep=True)
                df_copy[value_column_full] = (df[value_column_full] * df[weight_column_full])
                df = df_copy

                logging.warning(f"Weighted_sum took {time.time()-start:.2f}s; result={weighted_sum}")

                total_weight = b  # or df[weight_column_full].sum() again, but we already have b
                logging.warning(f"total_weight = {total_weight}")

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
                #changed unweighted to bucketing_rule
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
        
        if rule_set == "Rule_36":
            df.to_csv("sum_product.csv", index = False)
            #raise Exception(f" Breaking Post Product Sum")
    
        logging.warning(f"Returning evaluate_rule_set with bucketed_values {bucketed_values}")
        logging.warning(f"Returning evaluate_rule_set with final_value {final_value}")
        return bucketed_values, final_value

