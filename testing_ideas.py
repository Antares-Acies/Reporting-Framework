import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    # -------------------------------------------------------------------------
    # 1. Define file paths (adjust as needed)
    # -------------------------------------------------------------------------
    cashflow_one_path = r"c:\Users\KumarAkashdeep\Downloads\BR.net Cashflows 1\BR.net Cashflows 1.csv"
    cashflow_two_path = r"c:\Users\KumarAkashdeep\Downloads\BR.net Cashflows 1\BR.net Cashflows 2.csv"
    position_data_path = r"c:\Users\KumarAkashdeep\Downloads\BR.net Cashflows 1\Position Data.csv"
    
    # This is the output CSV (with rows where outstanding != cashflow sum, or 'not-found')
    output_csv_path = r"c:\Users\KumarAkashdeep\Downloads\BR.net Cashflows 1\principal_mismatch.csv"

    logging.warning("Starting the process...")

    # -------------------------------------------------------------------------
    # 2. Read the cashflow and position data
    # -------------------------------------------------------------------------
    cashflow_one = pd.read_csv(cashflow_one_path)
    cashflow_two = pd.read_csv(cashflow_two_path)
    position_data = pd.read_csv(position_data_path)

    # Log shapes and unique_reference_id counts
    logging.warning(f"cashflow_one shape: {cashflow_one.shape}, "
                    f"unique IDs: {cashflow_one['unique_reference_id'].nunique()}")
    logging.warning(f"cashflow_two shape: {cashflow_two.shape}, "
                    f"unique IDs: {cashflow_two['unique_reference_id'].nunique()}")
    logging.warning(f"position_data shape: {position_data.shape}, "
                    f"unique IDs: {position_data['unique_reference_id'].nunique()}")

    # -------------------------------------------------------------------------
    # 3. Combine cashflow DataFrames
    # -------------------------------------------------------------------------
    cashflow_data = pd.concat([cashflow_one, cashflow_two], ignore_index=True)
    logging.warning(f"Combined cashflow_data shape: {cashflow_data.shape}, "
                    f"unique IDs: {cashflow_data['unique_reference_id'].nunique()}")

    # -------------------------------------------------------------------------
    # 4. Filter for "Principal Proceeds" and group by unique_reference_id
    # -------------------------------------------------------------------------
    filtered_cashflow = cashflow_data[cashflow_data["cashflow_type"] == "Principal Proceeds"]
    logging.warning(f"filtered_cashflow shape (Principal Proceeds only): {filtered_cashflow.shape}, "
                    f"unique IDs: {filtered_cashflow['unique_reference_id'].nunique()}")

    # Group by unique_reference_id and sum the cashflow column
    cashflow_agg = (
        filtered_cashflow
        .groupby("unique_reference_id", as_index=False)["cashflow"]
        .sum()
        .rename(columns={"cashflow": "cashflow_sum"})
    )

    logging.warning(f"cashflow_agg shape after groupby: {cashflow_agg.shape}, "
                    f"unique IDs: {cashflow_agg['unique_reference_id'].nunique()}")

    # -------------------------------------------------------------------------
    # 5. Compare with position_data's outstanding_amount
    # -------------------------------------------------------------------------
    # Merge on unique_reference_id, left side is position_data 
    # -> ensures we keep ALL positions from 'position_data'
    merged_df = position_data.merge(cashflow_agg, on="unique_reference_id", how="left")

    logging.warning(f"merged_df shape after merge (left join): {merged_df.shape}, "
                    f"unique IDs: {merged_df['unique_reference_id'].nunique()}")

    # If there's no match, fill with "not-found"
    merged_df["principal_proceeds"] = merged_df["cashflow_sum"].fillna("not-found")

    # -------------------------------------------------------------------------
    # 6. Calculate the difference
    # -------------------------------------------------------------------------
    def calculate_diff(row):
        if row["principal_proceeds"] == "not-found":
            # Return None or 0 based on your business logic
            return None  
        else:
            return row["outstanding_amount"] - row["principal_proceeds"]

    merged_df["diff"] = merged_df.apply(calculate_diff, axis=1)

    # -------------------------------------------------------------------------
    # 7. Filter rows for mismatch
    # -------------------------------------------------------------------------
    # We only keep rows where:
    #   - principal_proceeds == "not-found", OR
    #   - diff != 0
    mismatch_df = merged_df.copy()
    
    # mismatch_df = merged_df[
    #     (merged_df["principal_proceeds"] == "not-found") |
    #     (merged_df["diff"] != 0)
    # ].copy()

    logging.warning(f"mismatch_df shape: {mismatch_df.shape}, "
                    f"unique IDs: {mismatch_df['unique_reference_id'].nunique()}")

    # -------------------------------------------------------------------------
    # 8. Generate a new output CSV with the mismatch rows
    # -------------------------------------------------------------------------
    mismatch_df[[
        "unique_reference_id",
        "outstanding_amount",
        "principal_proceeds",
        "diff"
    ]].to_csv(output_csv_path, index=False)

    logging.warning(f"Mismatch file generated at: {output_csv_path}")
    logging.warning("Process completed.")

if __name__ == "__main__":
    main()
