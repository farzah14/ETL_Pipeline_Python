import pandas as pd
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from src.transform import transform_all

logger = logging.getLogger(__name__)

# Create database engine
def db_engine(connection_str:str):
    return create_engine(connection_str)

def init_db(engine):
    with engine.connect() as conn:
        conn.begin()
        try:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_customers (
                CustomerID INT PRIMARY KEY,
                Country VARCHAR(50)
            )
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_products (
                StockCode VARCHAR(20) PRIMARY KEY,
                Description VARCHAR(100)
            )
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                InvoiceNo VARCHAR(20),
                StockCode VARCHAR(20),
                Quantity INT,
                UnitPrice FLOAT,
                TotalPrice FLOAT,
                InvoiceDate DATETIME,
                CustomerID INT,
                Country VARCHAR(50),
                PRIMARY KEY (InvoiceNo, StockCode),
                FOREIGN KEY (CustomerID) REFERENCES dim_customers(CustomerID),
                FOREIGN KEY (StockCode) REFERENCES dim_products(StockCode)
            )
            """)
            conn.commit()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            conn.rollback()
            raise

# Load Dimensions Data to Database
def load_dimensions(df: pd.DataFrame, engine):
    logger.info("Loading Dimensions Tables...")

    # Extract unique customers
    dim_customers = df[["CustomerID", "Country"]].dropna(subset=["CustomerID"]).drop_duplicates(subset=["CustomerID"])
    dim_customers["CustomerID"] = dim_customers["CustomerID"].astype(int)

    # Extract unique products
    dim_products = df[["StockCode", "Description"]].dropna(subset=["StockCode"]).drop_duplicates(subset=["StockCode"])

    with engine.begin() as conn:
        exists_customers_raw = conn.execute(text("SELECT CustomerID FROM dim_customers")).fetchall()
        exists_customers = {row[0] for row in exists_customers_raw}

        exists_products_raw = conn.execute(text("SELECT StockCode FROM dim_products")).fetchall()
        exists_products = {row[0] for row in exists_products_raw}

        new_customers = dim_customers[~dim_customers["CustomerID"].isin(exists_customers)]
        new_products = dim_products[~dim_products["StockCode"].isin(exists_products)]

        if not new_customers.empty:
            new_customers.to_sql("dim_customers", conn, if_exists="append", index=False)
            logger.info(f"Loaded {len(new_customers)} new customers")
        else:
            logger.info("No new customers to load")

        if not new_products.empty:
            new_products.to_sql("dim_products", conn, if_exists="append", index=False)
            logger.info(f"Loaded {len(new_products)} new products")
        else:
            logger.info("No new products to load")

# Load Facts Data to Database
def load_fact(df: pd.DataFrame, engine):
    logger.info("Loading Fact Table...")

    fact_sales = df[
        ["InvoiceNo", "StockCode", "Quantity", "UnitPrice", "TotalPrice", "InvoiceDate", "CustomerID", "Country"]
    ].dropna(subset=["CustomerID"])
    fact_sales["CustomerID"] = fact_sales["CustomerID"].astype(int)
    
    with engine.begin() as conn:
        exists_sales_raw = conn.execute(text("SELECT InvoiceNo, StockCode FROM fact_sales")).fetchall()
        exists_sales = {(str(row[0]), str(row[1])) for row in exists_sales_raw}
        
        sales_tuples = list(zip(fact_sales["InvoiceNo"].astype(str), fact_sales["StockCode"].astype(str)))
        new_sales = fact_sales[[t not in exists_sales for t in sales_tuples]]

        if not new_sales.empty:
            new_sales.to_sql("fact_sales", conn, if_exists="append", index=False)
            logger.info(f"Loaded {len(new_sales)} new sales")
        else:
            logger.info("No new sales to load")
    

def load_data(file_path: str, connection_str: str):
    logger.info(f"Loading data from {file_path}...")
    df = transform_all(file_path)
    engine = db_engine(connection_str)
    init_db(engine)
    load_dimensions(df, engine)
    load_fact(df, engine)
    logger.info("Data loaded successfully")