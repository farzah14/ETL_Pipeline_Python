# ETL Pipeline: Load Phase (PostgreSQL) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a robust, transactional, and idempotent Load phase that saves cleaned e-commerce data into a PostgreSQL database using a Star Schema.

**Architecture:** The load process splits data into dimension tables (`dim_customers`, `dim_products`) and a central fact table (`fact_transactions`). It uses SQLAlchemy for connection management and database transactions (ACID compliance) and delta checks (incremental insertions) for dimension idempotency.

**Tech Stack:** Python, Pandas, SQLAlchemy, Psycopg2-binary, Pytest

---

### Task 1: Environment Setup

**Files:**
- Modify: `requirements.txt` (or install directly in virtual env)
- Test: Run terminal command to verify libraries

- [ ] **Step 1: Install database drivers and test tools in the virtual environment**

Run:
```bash
.\.venv\Scripts\python.exe -m pip install pytest sqlalchemy psycopg2-binary
```
Expected output: Success message indicating installation of pytest, sqlalchemy, and psycopg2-binary.

- [ ] **Step 2: Verify psycopg2 installation**

Run:
```bash
.\.venv\Scripts\python.exe -c "import psycopg2; print(psycopg2.__version__)"
```
Expected output: Psycopg2 version info.

---

### Task 2: Schema DDL & Database Connection Setup

**Files:**
- Create: `src/load.py`
- Create: `tests/test_load.py`

- [ ] **Step 1: Write mock-based test for database schema initialization**

Create `tests/test_load.py` and write:
```python
from unittest.mock import MagicMock
from src.load import init_db

def test_init_db():
    mock_engine = MagicMock()
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    
    init_db(mock_engine)
    
    # Verify execute was called to create the tables
    assert mock_conn.execute.call_count == 3
```

- [ ] **Step 2: Run test and verify failure**

Run:
```bash
.\.venv\Scripts\python.exe -m pytest tests/test_load.py -k test_init_db
```
Expected: FAIL with `ImportError: cannot import name 'init_db' from 'src.load'`.

- [ ] **Step 3: Write minimal implementation for database initialization**

Create `src/load.py` and write:
```python
import logging
from sqlalchemy import create_engine, text
import pandas as pd

logger = logging.getLogger(__name__)

def get_db_engine(conn_string: str):
    """Create a SQLAlchemy engine."""
    return create_engine(conn_string)

def init_db(engine):
    """Initialize PostgreSQL schemas."""
    logger.info("Initializing database schemas...")
    with engine.begin() as conn:
        # Create dim_customers
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_customers (
            CustomerID INT PRIMARY KEY,
            Country VARCHAR(100),
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """))
        
        # Create dim_products
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_products (
            StockCode VARCHAR(50) PRIMARY KEY,
            Description TEXT,
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """))
        
        # Create fact_transactions
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_transactions (
            TransactionID SERIAL PRIMARY KEY,
            InvoiceNo VARCHAR(50),
            StockCode VARCHAR(50),
            CustomerID INT,
            Quantity INT,
            InvoiceDate TIMESTAMP,
            UnitPrice NUMERIC(10,2),
            TotalAmount NUMERIC(12,2),
            IsCancellation SMALLINT,
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (CustomerID) REFERENCES dim_customers(CustomerID),
            FOREIGN KEY (StockCode) REFERENCES dim_products(StockCode)
        );
        """))
    logger.info("Database schemas initialized successfully.")
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
.\.venv\Scripts\python.exe -m pytest tests/test_load.py -k test_init_db
```
Expected: PASS

---

### Task 3: Load Dimension Tables (Customers & Products)

**Files:**
- Modify: `src/load.py`
- Modify: `tests/test_load.py`

- [ ] **Step 1: Write mock-based test for loading dimensions**

Modify `tests/test_load.py` and add:
```python
from unittest.mock import MagicMock, call
import pandas as pd
from src.load import load_dimensions

def test_load_dimensions():
    mock_engine = MagicMock()
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    
    # Mock database read results (no existing rows)
    mock_conn.execute.return_value.fetchall.side_effect = [[], []]
    
    # Setup test DataFrame
    data = {
        "CustomerID": [12345, 67890],
        "Country": ["United Kingdom", "France"],
        "StockCode": ["85123A", "71053"],
        "Description": ["HEART LIGHT HOLDER", "LANTERN"]
    }
    df = pd.DataFrame(data)
    
    # We patch to_sql to verify it is called on the connection mock
    with pd.option_context('mode.chained_assignment', None):
        with pd.testing.assert_produces_warning(None):
            load_dimensions(df, mock_engine)
            
    # Verify execute was called to check existing keys (twice: customers, products)
    assert mock_conn.execute.call_count == 2
```

- [ ] **Step 2: Run test and verify failure**

Run:
```bash
.\.venv\Scripts\python.exe -m pytest tests/test_load.py -k test_load_dimensions
```
Expected: FAIL with `ImportError: cannot import name 'load_dimensions' from 'src.load'`.

- [ ] **Step 3: Implement load_dimensions with Delta checks**

Modify `src/load.py` to add `load_dimensions`:
```python
def load_dimensions(df: pd.DataFrame, engine):
    """Load unique Customers and Products into dimension tables."""
    logger.info("Loading Dimension tables...")
    
    # Extract unique customers
    df_cust = df[['CustomerID', 'Country']].dropna(subset=['CustomerID']).drop_duplicates(subset=['CustomerID'])
    df_cust['CustomerID'] = df_cust['CustomerID'].astype(int)
    
    # Extract unique products
    df_prod = df[['StockCode', 'Description']].dropna(subset=['StockCode']).drop_duplicates(subset=['StockCode'])
    
    with engine.begin() as conn:
        # Check existing customers
        result_cust = conn.execute(text("SELECT CustomerID FROM dim_customers;"))
        existing_cust_ids = {row[0] for row in result_cust.fetchall()}
        
        # Insert only new customers
        new_cust = df_cust[~df_cust['CustomerID'].isin(existing_cust_ids)]
        if not new_cust.empty:
            new_cust.to_sql('dim_customers', conn, if_exists='append', index=False)
            logger.info(f"Loaded {len(new_cust)} new customers.")
        else:
            logger.info("No new customers to load.")
            
        # Check existing products
        result_prod = conn.execute(text("SELECT StockCode FROM dim_products;"))
        existing_prod_codes = {row[0] for row in result_prod.fetchall()}
        
        # Insert only new products
        new_prod = df_prod[~df_prod['StockCode'].isin(existing_prod_codes)]
        if not new_prod.empty:
            new_prod.to_sql('dim_products', conn, if_exists='append', index=False)
            logger.info(f"Loaded {len(new_prod)} new products.")
        else:
            logger.info("No new products to load.")
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
.\.venv\Scripts\python.exe -m pytest tests/test_load.py -k test_load_dimensions
```
Expected: PASS

---

### Task 4: Load Fact Table (Transactions)

**Files:**
- Modify: `src/load.py`
- Modify: `tests/test_load.py`

- [ ] **Step 1: Write mock-based test for loading fact transactions**

Modify `tests/test_load.py` and add:
```python
from src.load import load_facts

def test_load_facts():
    mock_engine = MagicMock()
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    
    # Setup test transaction
    data = {
        "InvoiceNo": ["536365"],
        "StockCode": ["85123A"],
        "CustomerID": [12345],
        "Quantity": [6],
        "InvoiceDate": ["2010-12-01 08:26:00"],
        "UnitPrice": [2.55],
        "TotalAmount": [15.3],
        "IsCancellation": [0]
    }
    df = pd.DataFrame(data)
    
    # We patch to_sql to verify it is called
    with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        load_facts(df, mock_engine)
        mock_to_sql.assert_called_once_with('fact_transactions', mock_conn, if_exists='append', index=False)
```

- [ ] **Step 2: Run test and verify failure**

Run:
```bash
.\.venv\Scripts\python.exe -m pytest tests/test_load.py -k test_load_facts
```
Expected: FAIL with `ImportError: cannot import name 'load_facts' from 'src.load'`.

- [ ] **Step 3: Implement load_facts with ACID Transactions**

Modify `src/load.py` to add `load_facts`:
```python
def load_facts(df: pd.DataFrame, engine):
    """Load transaction data into the fact table."""
    logger.info("Loading Fact table...")
    
    # Prepare fact table dataframe
    fact_cols = [
        'InvoiceNo', 'StockCode', 'CustomerID', 
        'Quantity', 'InvoiceDate', 'UnitPrice', 
        'TotalAmount', 'IsCancellation'
    ]
    df_fact = df[fact_cols].copy()
    
    # Ensure CustomerID is nullable integer representation
    df_fact['CustomerID'] = df_fact['CustomerID'].astype('Int64')
    
    with engine.begin() as conn:
        df_fact.to_sql('fact_transactions', conn, if_exists='append', index=False)
        logger.info(f"Successfully loaded {len(df_fact)} records into fact_transactions.")
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
.\.venv\Scripts\python.exe -m pytest tests/test_load.py -k test_load_facts
```
Expected: PASS

---

### Task 5: Main Orchestration & Database Configuration

**Files:**
- Create: `src/main.py`
- Create: `tests/test_main.py`
- Create: `.env`

- [ ] **Step 1: Write test for main ETL orchestrator**

Create `tests/test_main.py` and write:
```python
from unittest.mock import patch, MagicMock
from src.main import run_etl_pipeline

@patch('src.main.init_db')
@patch('src.main.extract_get_data')
@patch('src.main.transform_all')
@patch('src.main.load_dimensions')
@patch('src.main.load_facts')
@patch('src.main.get_db_engine')
def test_run_etl_pipeline(mock_get_engine, mock_load_facts, mock_load_dims, mock_transform, mock_extract, mock_init_db):
    mock_df = MagicMock()
    mock_extract.return_value = mock_df
    mock_transform.return_value = mock_df
    
    run_etl_pipeline("postgresql://user:pass@host:5432/db")
    
    mock_init_db.assert_called_once()
    mock_extract.assert_called_once()
    mock_transform.assert_called_once()
    mock_load_dims.assert_called_once()
    mock_load_facts.assert_called_once()
```

- [ ] **Step 2: Run test and verify failure**

Run:
```bash
.\.venv\Scripts\python.exe -m pytest tests/test_main.py
```
Expected: FAIL with `ModuleNotFoundError: No module named 'src.main'`.

- [ ] **Step 3: Create database environment variables file `.env`**

Create `.env` in the root folder:
```env
# Database connection configurations
DB_USER=postgres
DB_PASSWORD=yourpassword
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce
```

- [ ] **Step 4: Implement main orchestrator `run_etl_pipeline`**

Create `src/main.py` and write:
```python
import sys
import os
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import logging
from src.extract import extract_get_data
from src.transform import transform_all
from src.load import get_db_engine, init_db, load_dimensions, load_facts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def run_etl_pipeline(conn_string: str):
    """Run the complete ETL pipeline from extraction to PostgreSQL load."""
    logger.info("🚀 Starting ETL Pipeline Execution")
    
    # Create engine
    engine = get_db_engine(conn_string)
    
    # Step 1: Initialize Database
    init_db(engine)
    
    # Step 2: Extract Data
    df_raw = extract_get_data(project_root)
    
    # Step 3: Transform Data
    df_transformed = transform_all(df_raw)
    df_transformed['IsCancellation'] = df_transformed['InvoiceNo'].astype(str).str.startswith('C').astype(int)
    
    # Step 4: Load Data
    load_dimensions(df_transformed, engine)
    load_facts(df_transformed, engine)
    
    logger.info("🏁 ETL Pipeline Executed Successfully!")

if __name__ == "__main__":
    # Standard PostgreSQL URI format
    # In production, load these from environment variables or .env file
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME", "ecommerce")
    
    db_uri = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    run_etl_pipeline(db_uri)
```

- [ ] **Step 5: Run tests to verify all passes**

Run:
```bash
.\.venv\Scripts\python.exe -m pytest tests/
```
Expected: PASS for all unit and integration tests.
