import pandas as pd
import logging
import kagglehub
from pathlib import Path

# Get the logger, for checking process on terminal
logger = logging.getLogger(__name__)

# basic config for logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s"
)

def download_dataset(dataset:str, output_dir: Path, force_download: bool = False) -> pd.DataFrame:
    """Download Dataset From Kaggle to target directory
    
    Args:
        dataset (str): The name of the dataset to download.
        output_dir (Path): The path to the directory where the dataset will be downloaded.
        force_download (bool): Whether to force the download of the dataset. When the dataset exists
    
    Returns:
        Path: The path to the downloaded dataset.
    """
    logger.info(f"🔄 Download Dataset From Kaggle")
    
    # Check path directory "data". If there is any, the data will be saved there.
    if output_dir:
        path_dataset = Path(kagglehub.dataset_download(dataset, output_dir=str(output_dir), force_download=force_download))

    # Else, it will be saved in the default directory.
    else:
        path_dataset = Path(kagglehub.dataset_download(dataset, force_download=force_download))
    logger.info(f"✔ Download Dataset Kaggle is successful")
    return path_dataset

def find_first_csv(dataset_path: Path) -> pd.DataFrame:
    """Find the first CSV file inside the downloaded dataset directory.
    
    Args:
        dataset_path (Path): The path to the directory where the dataset will be downloaded.
    
    Returns:
        Path: The path to the CSV file.
        None: If the file path is invalid or does not exist.

    Raises:
        ValueError: If the file path is invalid or does not exist.
    """
    csv_files = list(dataset_path.glob("*.csv"))
    if not csv_files:
        logger.error("⚠️ No CSV files found in dataset path: %s", dataset_path)
        raise ValueError("No CSV files found in dataset path")
    logger.info("✔ Found CSV file: %s", csv_files[0])
    return csv_files[0]

def extract_data_csv(file_path: Path) -> pd.DataFrame:
    """Extract data from CSV file.
    
    Args:
        file_path (Path): The path to the CSV file.
    
    Returns:
        pd.DataFrame: DataFrame from the CSV file.
        None: If the file path is invalid or does not exist.

    Raises:
        ValueError: If the file path is invalid or does not exist.
        FileNotFoundError: If the file path is invalid or does not exist.
        PermissionError: If the file path is invalid or does not exist.
        Exception: If the file path is invalid or does not exist.
    """

    # Step 1 : Check if the file path is valid and exists
    # Check if the file path is valid
    if file_path.suffix != '.csv':
        logger.error("❌ File path does not end with CSV Formats")
        raise ValueError("Invalid File Format")

    # Check if the file exists
    if not file_path.exists():
        logger.error("❌ File path does not exist")
        raise FileNotFoundError("File Path Does Not Exist")
        
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
        raise
    
    # Error Handling (Checking Permission Denied)
    except PermissionError:
        logger.error(f"❌ Permission Denied for this file: {file_path.name}")
        raise
    
    # Error Handling (Checking another error to read CSV file)
    except Exception as e:
        logger.error(f"❌ Another Error to read CSV file: {str(e)}")
        raise

def extract_get_data(project_root: Path) -> pd.DataFrame:
    """
    Extract all data format from CSV file.
    
    Args:
        project_root (Path): The path to the root directory of the project.
    
    Returns:
        pd.DataFrame: DataFrame from the CSV file.
        None: If the file path is invalid or does not exist.

    Raises:
        FileNotFoundError: If the file path is invalid or does not exist.
    """
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

    # Check the data exists
    if csv_path:
        df = extract_data_csv(csv_path)
        logger.info("✔ EXTRACT AND READ CSV FILES IS SUCCESSFULLY")
        return df
    else:
        raise FileNotFoundError("❌ File does not exist or is not valid")

def summary_data(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Summary data from DataFrame.

    Args:
        dataframe (pd.DataFrame): DataFrame from the CSV file.
    
    Returns:
        None: If the file path is invalid or does not exist.
    """
    data = dataframe

    # Check if Dataframe is None
    if data is None:
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
    result_data = extract_get_data(project_root)
    
    # Function For Summary Data
    summary_data(result_data)
