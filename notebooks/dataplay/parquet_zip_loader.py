import pandas as pd
import os
import zipfile
from io import BytesIO
from collections import defaultdict
from typing import List

def find_zip_files(folder: str) -> List[str]:
    """Return list of zip files in the given folder."""
    return [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".zip")]

def group_parquet_files(zip_file: zipfile.ZipFile) -> (List[str], dict):
    """Group parquet files in zip into full files and part groups."""
    part_groups = defaultdict(list)
    full_files = []
    for file in zip_file.namelist():
        if file.endswith(".parquet"):
            if "_part" in os.path.basename(file):
                prefix = os.path.basename(file).split("_part")[0]
                part_groups[prefix].append(file)
            else:
                full_files.append(file)
    return full_files, part_groups

def read_parquet_from_zip(zip_file: zipfile.ZipFile, file: str) -> pd.DataFrame:
    """Read a parquet file from a zip archive."""
    with zip_file.open(file) as f:
        return pd.read_parquet(BytesIO(f.read()))

def merge_part_files(zip_file: zipfile.ZipFile, files: List[str]) -> pd.DataFrame:
    """Merge part parquet files column-wise, removing duplicate columns."""
    part_dfs = [read_parquet_from_zip(zip_file, file) for file in sorted(files)]
    merged_df = pd.concat(part_dfs, axis=1)
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
    return merged_df

def load_all_parquet_from_zips(artifacts_folder: str) -> pd.DataFrame:
    """Load and merge all parquet files from all zip files in a folder."""
    dfs = []
    zip_files = find_zip_files(artifacts_folder)
    print("Zip files found:", zip_files)
    for zip_path in zip_files:
        print(f"Processing {zip_path}...")
        with zipfile.ZipFile(zip_path, "r") as z:
            print("Files in zip:", z.namelist())
            full_files, part_groups = group_parquet_files(z)
            # Handle full files
            for file in full_files:
                dfs.append(read_parquet_from_zip(z, file))
            # Handle part files
            for prefix, files in part_groups.items():
                print(f"Processing part group {prefix} with files: {files}")
                dfs.append(merge_part_files(z, files))
    if not dfs:
        raise ValueError("No parquet data found in any zip files.")
    final_df = pd.concat(dfs, ignore_index=True)
    return final_df