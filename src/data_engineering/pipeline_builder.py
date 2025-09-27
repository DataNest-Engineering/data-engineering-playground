
from data_engineering.parquet_zip_loader import load_all_parquet_from_zips
import os
import pandas as pd
from pathlib import Path

def data_pipeline(artifacts_folder: str = 'data/artifacts3', output_path: str = 'data/output'):
    """
    Load and merge all parquet files from all zip files in a folder.
    
    Args:
        artifacts_folder (str): Path to the folder containing zip files.
    """
    output_path = Path(output_path)
    print(f"Loading parquet files from zip files in {artifacts_folder}")
    final_df = load_all_parquet_from_zips(artifacts_folder)
    print("Final DataFrame shape:", final_df.shape)
    print(final_df.head())
    save_data_to_parquet(final_df, output_path)


def save_data_to_parquet(output: pd.DataFrame, output_path: str):
    """
    Save the DataFrame to a parquet file.
    Args:
        output (pd.DataFrame): DataFrame to save.
        output_path (str): Path to save the parquet file.
    """
    output_path.mkdir(parents=True, exist_ok=True)
    print(f"Saving merged data to {output_path}")
    output_file = os.path.join(output_path, 'merged_parquet.parquet')
    output.to_parquet(output_file, index=False)


def main():
    """
    Main function to execute the statistics pipeline.
    """
    artifacts_folder = os.getenv('ARTIFACTS_FOLDER', 'data/artifacts3')
    output_path = os.getenv('OUTPUT_PATH', 'data/output')
    data_pipeline(artifacts_folder, output_path)


if __name__ == "__main__":
    main()

