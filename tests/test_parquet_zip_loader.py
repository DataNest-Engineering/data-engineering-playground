import os
import zipfile
import pandas as pd
import pytest
from io import BytesIO
from data_engineering import parquet_zip_loader

@pytest.fixture
def sample_zip_with_full_and_part_files(tmp_path):
    # Create DataFrames
    df_full = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    df_part1 = pd.DataFrame({'C': [5, 6]})
    df_part2 = pd.DataFrame({'D': [7, 8]})

    # Write to parquet in memory
    full_bytes = BytesIO()
    df_full.to_parquet(full_bytes)
    full_bytes.seek(0)

    part1_bytes = BytesIO()
    df_part1.to_parquet(part1_bytes)
    part1_bytes.seek(0)

    part2_bytes = BytesIO()
    df_part2.to_parquet(part2_bytes)
    part2_bytes.seek(0)

    # Create zip file
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("full.parquet", full_bytes.read())
        z.writestr("prefix_part1.parquet", part1_bytes.read())
        z.writestr("prefix_part2.parquet", part2_bytes.read())
    return zip_path

def test_find_zip_files(tmp_path):
    zip1 = tmp_path / "a.zip"
    zip2 = tmp_path / "b.zip"
    zip1.write_bytes(b"dummy")
    zip2.write_bytes(b"dummy")
    files = parquet_zip_loader.find_zip_files(str(tmp_path))
    assert zip1.as_posix() in files
    assert zip2.as_posix() in files

def test_group_parquet_files(sample_zip_with_full_and_part_files):
    with zipfile.ZipFile(sample_zip_with_full_and_part_files, "r") as z:
        full_files, part_groups = parquet_zip_loader.group_parquet_files(z)
        assert "full.parquet" in full_files
        assert "prefix" in part_groups
        assert len(part_groups["prefix"]) == 2

def test_read_parquet_from_zip(sample_zip_with_full_and_part_files):
    with zipfile.ZipFile(sample_zip_with_full_and_part_files, "r") as z:
        df = parquet_zip_loader.read_parquet_from_zip(z, "full.parquet")
        assert list(df.columns) == ["A", "B"]
        assert df.shape == (2, 2)

def test_merge_part_files(sample_zip_with_full_and_part_files):
    with zipfile.ZipFile(sample_zip_with_full_and_part_files, "r") as z:
        files = ["prefix_part1.parquet", "prefix_part2.parquet"]
        merged = parquet_zip_loader.merge_part_files(z, files)
        assert set(merged.columns) == {"C", "D"}
        assert merged.shape == (2, 2)

def test_load_all_parquet_from_zips(tmp_path, sample_zip_with_full_and_part_files):
    # Place the zip in a folder
    folder = tmp_path
    zip_path = sample_zip_with_full_and_part_files
    os.rename(zip_path, folder / "test.zip")
    df = parquet_zip_loader.load_all_parquet_from_zips(str(folder))
    # Should have columns from both full and merged part files
    assert set(df.columns) == {"A", "B", "C", "D"}
    assert df.shape[0] == 4  # 2 from full, 2 from merged part

def test_load_all_parquet_from_zips_empty(tmp_path):
    # No zip files in folder
    with pytest.raises(ValueError, match="No parquet data found in any zip files."):
        parquet_zip_loader.load_all_parquet_from_zips(str(tmp_path))