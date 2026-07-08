import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)
project_root = Path(__file__).resolve().parent.parent

def fix_data_type(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("PHASE : FIXING DATA TYPE")
    try:
        logger.info("Process Fixing Data Type")
        for col in df.columns:
            if col == "InvoiceDate":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif col == "CustomerID":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        logger.info("✅ Fixing Data Type Successfully")
    except TypeError as e:
        logger.error(f"DF failed to load : {e}")
    return df

def standardization_text(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("PHASE : STANDARDIZATION TEXT")
    df_new = df.copy()
    
    # Total whitespace, Every Columns
    total_whitespace = 0

    logger.info("Process Clean Whitespace")

    # Remove Whitespace
    text_columns = ["Description", "Country", "StockCode", "InvoiceNo"]
    for col in text_columns:
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
    logger.info("PHASE : HANDLING MISSING VALUES")
    
    total_missing_values = df.isnull().sum()
    cols_with_missing_values = []

    for col, total in total_missing_values.items():
        if total > 0:
            logger.info(f"Column {col} have {total} missing values")
            cols_with_missing_values.append(col)
    
    # Drop rows with missing values in the identified columns
    rows_before = len(df)

    # Remove null values in selected columns
    logger.info("Process Remove Null Values")
    df = df.dropna(subset=cols_with_missing_values)

    rows_after = len(df)
    rows_dropped = rows_before - rows_after
    
    logger.info(f"Drop Missing Values : {rows_dropped} rows")
    logger.info(f"Remaining Rows : {rows_after}")

    logger.info(f"✅ Handling Missing Values Complete")
    return df

def handling_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("PHASE : HANDLING DUPLICATES")
    
    rows_before = len(df)
    
    total_duplicates = df.duplicated().sum()
    # Remove duplicate values
    logger.info(f"Total Duplicates Values : {total_duplicates}")

    if total_duplicates == 0:
        logger.info("✅ Not Have Duplicate Values")
        return df

    logger.info("Process Drop Duplicate Values")
    df = df.drop_duplicates()

    rows_after = len(df)
    rows_dropped = rows_before - rows_after
    
    logger.info(f"Drop Duplicate Values : {rows_dropped} rows")
    logger.info(f"Remaining Rows : {rows_after}")
    
    logger.info("✅ Handling Duplicates Complete")
    return df
    
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

    # Get data better than zero
    df = df[
        (df["UnitPrice"] >= 0) & (df["Quantity"] >= 0)
    ]
    
    # Creates Columns TotalPrice
    df["TotalPrice"] = round(df["UnitPrice"] * df["Quantity"].astype(float),2)

    # Pipeline Transforms Phases
    df = standardization_text(df)
    df = fix_data_type(df)
    df = handling_missing_values(df)
    df = handling_duplicates(df)
    
    # Saves file data after transforms to CSV
    logger.info(f"Save Data Clean to Folders Processed")
    df.to_csv(
        project_root/"data"/"processed"/"data_clean.csv", index=False,
        encoding="utf-8"
    )
    logger.info("Saves Is Successfully.....")

    print("="*60)
    print("TRANSFORMATION IS SUCCESSFULLY")
    print("="*60)
    
    return df

if __name__ == "__main__":
    raw_data_csv = project_root / "data" / "raw"
    df = transform_all(raw_data_csv)
