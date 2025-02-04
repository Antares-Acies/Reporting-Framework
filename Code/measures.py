#This file is responsible for picking up the Measures - parquet value from the revolutio

import time
import pandas as pd
from pathlib import Path
import logging
from config.settings.base import DISKSTORE_PATH


logging.warning(f"   ")
logging.warning(f"   ")
logging.warning(f"   ")
logging.warning(f"Start of Measure's editor")


# Example variable; in your actual code, ensure 'Reporting_Date' is properly defined or passed in.
valuation_date = Reporting_Date  
logging.warning(f"valuation_date : {valuation_date} ")

valuation_date_str = valuation_date.strftime("%Y-%m-%d")
logging.warning(f"valuation_date: {valuation_date_str}")

logging.warning(f"1 DISKSTORE_PATH : {DISKSTORE_PATH}")
# Define the root path to the disk store
logging.warning(f"2 DISKSTORE_PATH : {DISKSTORE_PATH}")

# Set up the directory for measures files
measures_dir = Path(f"{DISKSTORE_PATH}Cashflow_Engine_Outputs/Measures")
logging.warning(f" measures_dir : {measures_dir}")


all_parquet_files = list(measures_dir.glob("*.parquet"))
logging.warning(f"Number of .parquet files found in {measures_dir}: {len(all_parquet_files)}")



all_files_for_date = list(measures_dir.glob(f"*_{valuation_date_str}_*.parquet"))
total_files_count = len(all_files_for_date)

logging.warning(
    f"Number of .parquet files found for date {valuation_date_str}: {total_files_count}"
)


if not all_files_for_date:
    logging.warning(
        "No matching cashflow files found for the specified valuation date."
    )
    final_measures_df = pd.DataFrame()  # Return or handle empty DataFrame
else:

    # 2) Identify product variants from file names and group them
    #    variant_to_files = {variant_name: [Path1, Path2, ...]}

    variant_to_files = {}

    for file_path in all_files_for_date:
        # Split filename by underscores
        parts = file_path.stem.split("_")
        #fetching the product_variant_name and valuation_date
        try:
            product_variant_name = parts[-4]  # fourth-to-last part
            valuation_date = parts[-5]  # fifth-to-last part
 
            # Validate the valuation date
            if valuation_date != valuation_date_str:
                logging.warning(f"Valuation date mismatch in file: {file_path.name}")
                product_variant_name = None
                continue
        except IndexError:
            logging.warning(f"Unexpected filename format: {file_path.name}")
            continue

        # Collect file paths for each variant
        variant_to_files.setdefault(product_variant_name, []).append(file_path)

    # Prepare final DataFrame and a read function

    final_measures_df = pd.DataFrame()

    def read_parquet_file(path: Path) -> pd.DataFrame:
        """Helper function to read a single Parquet file (sequentially)."""
        return pd.read_parquet(path)

    # 3) TRACK GLOBAL START TIME

    global_start_time = time.time()

    # Keep track of how many files we've processed so far (for cumulative %).
    processed_files_count = 0

    # 4) Read each variant’s files sequentially, measure time & memory usage

    for variant_name, file_list in variant_to_files.items():
        variant_start_time = time.time()  # start time for this variant

        num_files_for_variant = len(file_list)
        logging.warning(
            f"Product Variant: {variant_name}, " f"Files Found: {num_files_for_variant}"
        )

        # Percentage of total files that belong to this variant
        if total_files_count > 0:
            variant_file_percentage = (num_files_for_variant / total_files_count) * 100
        else:
            variant_file_percentage = 0

        logging.warning(
            f"{variant_name} accounts for {variant_file_percentage:.2f}% "
            "of all files for this valuation date."
        )

        # Read files for this variant

        dfs_for_variant = []
        for f in file_list:
            try:
                df = read_parquet_file(f)
                dfs_for_variant.append(df)
            except Exception as e:
                logging.warning(f"Error reading file {f.name}: {e}")

        # Concatenate DataFrames for this variant & log memory usage

        if dfs_for_variant:
            variant_df = pd.concat(dfs_for_variant, ignore_index=True)

            variant_mem_usage_mb = variant_df.memory_usage(deep=True).sum() / (1024**2)
            logging.warning(
                f"Memory usage for {variant_name} DataFrame: "
                f"{variant_mem_usage_mb:.2f} MB"
            )

            # Combine into final DataFrame
            final_measures_df = pd.concat(
                [final_measures_df, variant_df], ignore_index=True
            )

        # Update how many files are processed in total
        processed_files_count += num_files_for_variant

        # Log cumulative percentage of files processed so far
        if total_files_count > 0:
            cumulative_percentage = (processed_files_count / total_files_count) * 100
        else:
            cumulative_percentage = 0

        # 5) VARIANT END TIME & LOG
        variant_end_time = time.time()
        time_for_variant = variant_end_time - variant_start_time
        logging.warning(
            f"Finished reading {variant_name} in "
            f"{time_for_variant:.2f} seconds. "
            f"Cumulative files processed: {processed_files_count} / {total_files_count} "
            f"({cumulative_percentage:.2f}%)."
        )

    # 6) GLOBAL END TIME & LOG FINAL STATS
    global_end_time = time.time()
    total_time = global_end_time - global_start_time

    logging.warning(f"All variants processed in {total_time:.2f} seconds.")

    # Log the memory usage of the final combined DataFrame
    if not final_measures_df.empty:
        total_mem_usage_mb = final_measures_df.memory_usage(deep=True).sum() / (1024**2)
        logging.warning(
            f"Final combined DataFrame memory usage: {total_mem_usage_mb:.2f} MB"
        )
    else:
        logging.warning("Final DataFrame is empty (no data was read).")
  
output_data = final_measures_df

logging.warning("Converting to STR")
logging.warning("Editor measures data end")
logging.warning("  ") 
logging.warning("  ")