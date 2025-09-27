# data-engineering-playground

start data engineering

## Project setup

- choose appropriate python enviornment

## Installation

From the project root (where `setup.py` is located):

```sh
pip install -U setuptools setuptools_scm wheel
```

```sh
python setup.py bdist_wheel
```

```sh
pip install dist/*.whl
```

# Unit Test

Run all unit tests with coverage:

```sh
pytest
```

or with converage reporting:

```sh
pytest --cov=parquet --cov-report=term-missing
```

## Unit Test Coverage for `load_parquet_data`

All the unit tests for `load_parquet_data` are located in `tests/test_load_parquet.py`. The tests cover the following scenarios:

- **Valid Parquet file loads correctly.**
- **File not found raises `FileNotFoundError`.**
- **Non-Parquet file raises `ValueError` with "not a Parquet file".**
- **Empty Parquet file raises `ValueError` with "contains no data".**
- **Parquet file with null values raises `ValueError` with "contains null values".**
- **Unreadable file raises `PermissionError`.**

These tests match the validation logic in `load_parquet_data`, so all tests are appropriate and should pass if the function is implemented as
