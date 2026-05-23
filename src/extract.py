import pandas as pd
from pathlib import Path
import logging
import kagglehub

# Get the logger, for checking process on terminal
logger = logging.getLogger(__name__)

# basic config for logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s"
)

def download_dataset(dataset:str, output_dir: Path | None = None, force_download: bool = False):
    """Download Dataset From Kaggle to target directory"""
    logger.info(f"🔄 Download Dataset From Kaggle")
    
    # Check path directory "data". If there is any, the data will be saved there.
    if output_dir:
        path_dataset = Path(kagglehub.dataset_download(dataset, output_dir=str(output_dir), force_download=force_download))

    # Else, it will be saved in the default directory.
    else:
        path_dataset = Path(kagglehub.dataset_download(dataset, force_download=force_download))
    logger.info(f"✔ Download Dataset Kaggle is successful")
    return path_dataset

def find_first_csv(dataset_path: Path):
    """Find the first CSV file inside the downloaded dataset directory."""
    csv_files = list(dataset_path.glob("*.csv"))
    if not csv_files:
        logger.error("⚠️ No CSV files found in dataset path: %s", dataset_path)
        return None
    logger.info("✔ Found CSV file: %s", csv_files[0])
    return csv_files[0]

def extract_data_csv(file_path: Path):
    """Extract data from CSV file."""

    # Step 1 : Check if the file path is valid and exists
    # Check if the file path is valid
    if not file_path.suffix == ('.csv'):
        logger.error("❌ File path does not end with CSV Formats")
        return None

    # Check if the file exists
    if not file_path.exists():
        logger.error("❌ File path does not exist")
        return None
        
    # Step 2 : Extract Data CSV When everything is valid
    try:
        logger.info(f"🔄 Reading CSV Files : {file_path.name}")
        try:
            # Get DataFrame from the CSV file
            df = pd.read_csv(file_path, encoding='utf-8')
        except UnicodeDecodeError:
            logger.warning(f"⚠️ UTF-8 decoding failed for {file_path.name}, retrying with ISO-8859-1")
            df = pd.read_csv(file_path, encoding='ISO-8859-1')
        logger.info(f"✔ Read Data from {file_path.name} is successfully")

        # Return DataFrame
        return df

    # Error Handling (Checking if the file is empty or not)
    except pd.errors.EmptyDataError:
        logger.error(f"❌ File is empty: {file_path.name}")
        return None
    
    # Error Handling (Checking Permission Denied)
    except PermissionError:
        logger.error(f"❌ Permission Denied for this file: {file_path.name}")
        return None
    
    # Error Handling (Checking another error to read CSV file)
    except Exception as e:
        logger.error(f"❌ Another Error to read CSV file: {str(e)}")
        return None

def get_data(project_root: Path):
    print("="*60)
    print("PHASE 1: EXTRACT CSV FORMATS")
    print("="*60)
    
    # Path to folder data
    data_dir = project_root / "data"

    # Check have a files .csv on folder data?.
    csv_path = find_first_csv(data_dir)

    # If There Isn't Any, Now Downlaod From Kaggle
    if not csv_path:
        try:
            dataset_path = download_dataset("carrie1/ecommerce-data", output_dir=data_dir)
        except FileExistsError:
            logger.warning("⚠️ Folder data is not empty. Retrying download with force_download=True")
            dataset_path = download_dataset("carrie1/ecommerce-data", output_dir=data_dir, force_download=True)
        csv_path = find_first_csv(dataset_path)

    # Check formats .csv and the data exists
    if not csv_path is None and csv_path.suffix == ".csv" and csv_path.exists():
        df = extract_data_csv(csv_path)
        logger.info("✔ EXTRACT AND READ CSV FILES IS SUCCESSFULLY")
        return df
    else:
        csv_name = csv_path.name if csv_path else "CSV File"
        logger.error(f"❌ File {csv_name} does not exist or is not valid")
        return None

def summary_data(dataframe: pd.DataFrame):
    data = dataframe

    # Check if Dataframe is None
    if data is None:
        logger.error("❌ Data is Cannot Summarize")
        return

    print("\n")
    print("="*60)
    print("SUMMARY DATASET")
    print("="*60)
    
    print(f"Check Sample Data :\n{data.head()}\n")
    logger.info(f"📊 Dataset Loaded - Rows: {dataframe.shape[0]}, Columns: {dataframe.shape[1]}")

    print(f"\n👀 Check Data Types :\n{data.dtypes}")

    print("\n📊 Total Missing Values")
    total_missing_values = dataframe.isnull().sum()
    for column, count in total_missing_values.items():
        if count > 0:
            print(f"Columns '{column}' has {count} missing values.")

if __name__ == "__main__":
    # Get the project root path (one level above the ‘src’ folder)
    project_root = Path(__file__).resolve().parent.parent

    # Final Result From Extract
    result_data = get_data(project_root)

    # Function For Summary Data
    summary_data(result_data)
