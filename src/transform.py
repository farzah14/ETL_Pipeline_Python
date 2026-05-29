import sys
import logging
import pandas as pd
from pathlib import Path

# Add the root directory to sys.path
# This allows you to run scripts as modules (e.g., python src/transform.py)
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.extract import extract_get_data

logger = logging.getLogger(__name__)

def fix_data_type(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("PHASE : FIXING DATA TYPE")
    try:
        logger.info("Process Load Dataframe Files")
        df_new = df.copy()
        logger.info("Load Successfully")
    except TypeError as e:
        logger.error(f"DF failed to load : {e}")
        raise

    logger.info("Process Fixing Data Type")
    for col in df_new.columns:
        if col == "InvoiceDate":
            df_new[col] = pd.to_datetime(df_new[col], errors="coerce")
        elif col == "CustomerID":
            df_new[col] = pd.to_numeric(df_new[col], errors="coerce").astype("Int64")
    logger.info("✅ Fixing Data Type Successfully")
    return df_new

def standardization_text(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("PHASE : STANDARDIZATION TEXT")
    df_new = df.copy()
    
    # Total whitespace, Every Columns
    total_whitespace = 0

    logger.info("Process Clean Whitespace")
    for col in df_new.columns:
        # Check Every COlumns. Non Numerical.
        if df_new[col].dtype == "object":
            # Comparing real values with values after strip()
            whitespace = df_new[col].notna() & (
                df_new[col] != df_new[col].str.strip()
            )
            # Total Whitespace
            count = whitespace.sum()
            total_whitespace += count

            if count > 0:
                logger.info(f"Total Clear Count : {count} From Columns : {col}")
            else:
                logger.info(f"Not Have Whitespace In Columns : {col}")
            df_new[col] = df_new[col].str.strip()
            
    logger.info(f"✅ Whitespace cleaning complete: {total_whitespace} values cleaned")

    return df_new

def handling_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    logger.info("PHASE : HANDLING MISSING VALUES")
    
    total_missing_values = df_clean.isnull().sum()
    cols_with_missing_values = []

    for col, total in total_missing_values.items():
        if total > 0:
            logger.info(f"Column {col} have {total} missing values")
            cols_with_missing_values.append(col)
    
    # Drop rows with missing values in the identified columns
    rows_before = len(df_clean)

    # Remove null values in selected columns
    logger.info("Process Remove Null Values")
    df_clean = df_clean.dropna(subset=cols_with_missing_values)

    rows_after = len(df_clean)
    rows_dropped = rows_before - rows_after
    
    logger.info(f"Drop Missing Values : {rows_dropped} rows")
    logger.info(f"Remaining Rows : {rows_after}")

    logger.info(f"✅ Handling Missing Values Complete")
    return df_clean

def handling_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("PHASE : HANDLING DUPLICATES")
    
    df_clean = df.copy()
    rows_before = len(df_clean)
    
    total_duplicates = df.duplicated().sum()
    # Remove duplicate values
    logger.info(f"Total Duplicates Values : {total_duplicates}")

    if total_duplicates == 0:
        logger.info("✅ Not Have Duplicate Values")
        return df_clean

    logger.info("Process Drop Duplicate Values")
    df_clean = df_clean.drop_duplicates()

    rows_after = len(df_clean)
    rows_dropped = rows_before - rows_after
    
    logger.info(f"Drop Duplicate Values : {rows_dropped} rows")
    logger.info(f"Remaining Rows : {rows_after}")
    
    logger.info("✅ Handling Duplicates Complete")
    return df_clean
    
def transform_all(df:pd.DataFrame) -> pd.DataFrame:
    """
    Transform all data from DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame from the CSV file.
    
    Returns:
        pd.DataFrame: DataFrame from the CSV file.
    """
    print("="*60)
    print("PHASE 2: TRANSFORMATION")
    print("="*60)
    df_transform = df.copy()

    # Pipeline Transform Phase
    df_transform = standardization_text(df_transform)
    df_transform = fix_data_type(df_transform)
    df_transform = handling_missing_values(df_transform)
    df_transform = handling_duplicates(df_transform)
    
    print("="*60)
    print("TRANSFORMATION IS SUCCESSFULLY")
    print("="*60)
    return df_transform

if __name__ == "__main__":
    df = extract_get_data(project_root)
    df = transform_all(df)
