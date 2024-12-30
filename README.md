# Reporting-Framework

This codebase provides a flexible bucket-based adjustment framework that applies various rules and operations to data. It includes:

Rule-Based Bucketing: Assigns data to buckets based on configurable conditions.
Condition Evaluation: Dynamically checks values (including data types like numeric, string, date, and boolean) against multiple condition types (e.g., Contains, Equal to, Greater Than, etc.).
Adjustments: Moves or modifies amounts among buckets when specified adjustment conditions are met.
Logging: Extensively logs each step of the process to facilitate debugging and auditing.
Key Components
evaluate_condition(df, condition)

Dynamically evaluates whether rows in a DataFrame match a given condition.
Supports different data types and condition types (e.g., Contains, Equal to, Greater Than, etc.).
apply_rule_based_bucketing(df, bucketing_rule_set, value_source_column, adjustment_rule=None)

Assigns rows to specified buckets based on configured rules.
Optionally applies adjustments (e.g., moving amounts between buckets) if an adjustment_rule is provided.
apply_bucket_adjustments(df, value_source_column, adjustment_rule)

Applies adjustments (based on conditions) to reallocate amounts between buckets.
Logs initial and final bucketed values, as well as any intermediate movements.
Configuration Files / Tables

reporting_bucketing_adjustment: Specifies how amounts should move between buckets under certain conditions.
mapping_set: Provides lists of possible values for “Contains” or “Does not Contain” operations.
rule_based_bucketing: Defines which conditions determine the bucket to which each row is assigned.
How It Works
Data Preparation

Ensure your DataFrame columns have the correct prefixes (e.g., position_data_+_column_name) and data types.
Bucketing

Call apply_rule_based_bucketing(df, bucketing_rule_set, value_source_column, adjustment_rule=None) to:
Filter rows by conditions.
Assign rows to buckets.
Optionally adjust amounts between buckets if an adjustment_rule is specified.
Condition Evaluation

evaluate_condition is automatically called to check each row against the rules.
It looks at condition types (Contains, Equal to, etc.) and data types (Character, Numeric, etc.) to decide how to filter rows.
Adjustments

If an adjustment rule is provided, apply_bucket_adjustments is called.
This function:
Logs initial bucketed values.
Applies conditions to the relevant from_bucket and moves amounts to the specified to_bucket.
Logs final bucketed values.
Logging

The code uses logging.warning (or similar) to record detailed step-by-step processes, including:
Condition checks
Amount calculations
Bucket reassignments
