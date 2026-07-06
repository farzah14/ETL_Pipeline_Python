import os
import logging
from src import extract, load, transform
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise EnvironmentError(
        "DATABASE_URL belum di-set. Cek file .env atau environment variables."
)

# basic config for logger(Only In Orchestrator)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s"
)

def pipeline_run():
    project_root = Path(__file__).resolve().parent

    df_extract = extract.extract_get_data(project_root)
    # Function For Summary Data
    extract.summary_data(df_extract)
    df_clean = transform.transform_all(df_extract)
    
    engine = load.db_engine(DATABASE_URL)
    
    load.init_db(engine)
    load.clear_db(engine)
    load.load_dimensions(df_clean, engine)
    load.load_fact(df_clean, engine) 

if __name__ == "__main__":
    pipeline_run()