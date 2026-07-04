import logging
from src import extract, load, transform
from pathlib import Path

connection_string = "postgresql+psycopg2://postgres:password123$@localhost:5432/ecommerce_db"

# basic config for logger(Only In Orchestrator)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s"
)

def pipeline_run():
    project_root = Path(__file__).resolve().parent.parent

    df_extract = extract.extract_get_data(project_root)
    df_clean = transform.transform_all(df_extract)
    
    engine = load.db_engine(connection_string)
    
    load.init_db(engine)
    load.clear_db(engine)
    load.load_dimensions(df_clean, engine)
    load.load_fact(df_clean, engine) 

if __name__ == "__main__":
    pipeline_run()