import pandas as pd

def check_not_null(df: pd.DataFrame, column:str) -> None:
    """Check that a column contains no null values."""

    if df[column].isnull().any():
        raise ValueError(f"Data quality check failed: {column} contains null values.")


def check_unique(df: pd.DataFrame, column:str) -> None:
    """Check that a column contains unique values."""

    if df[column].duplicated().any():
        raise ValueError(f"Data quality check failed: {column} contains duplicate values.")


def check_positive(df: pd.DataFrame, column: str) -> None:
    """Check that all values in a numeric column are greater than zero."""

    if (df[column] <= 0).any():
        raise ValueError(f"Data quality check failed: {column} contains non-positive values.")


def check_accepted_values(df: pd.DataFrame, column:str, accepted_values: list[str]) -> None:
    """Check that a column only contains accepted values."""

    invalid_values = set(df[column].dropna()) - set(accepted_values)

    if invalid_values:
        raise ValueError(f"Data quality check failed: {column} contains {invalid_values}")

        
