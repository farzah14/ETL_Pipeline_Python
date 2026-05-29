# ETL Pipeline: Transform and Load Phases Implementation Plan

This plan details the design and implementation of the **Transform** and **Load** phases of the ETL pipeline, building upon the existing **Extract** phase in `src/extract.py`. We will also introduce an orchestrator script `src/main.py` to run the full pipeline sequentially.

## User Review Required

> [!IMPORTANT]
> **Data Quality & Validation Decisions**:
> 1. We propose dropping rows with missing `CustomerID` or flagging them as `Unknown`.
> 2. Cancellations (Invoice numbers starting with 'C') and transactions with negative quantities/prices will be handled. We propose storing cancellations in a separate table/file or flagging them.
> 3. For the **Load** phase, we propose loading the transformed data into a local **SQLite database** (`data/ecommerce.db`) with relational tables (`transactions`, `customers`, `products`), as well as exporting a clean **Parquet** or **CSV** file. Please let us know if you prefer a different target database (e.g. PostgreSQL, MySQL).

## Open Questions

> [!WARNING]
> - Do you have a preferred database target for the Load phase (e.g., SQLite, PostgreSQL)? We default to SQLite for self-containment.
> - Would you like us to run any specific data quality checks during the Transform phase?

## Proposed Changes

---

### 1. Extract Phase Enhancements

#### [MODIFY] `src/extract.py`
- Fix the `UnicodeEncodeError` in Windows console environments when printing unicode icons (e.g. `👀`) by avoiding direct terminal output of unsupported emojis or configuring basic stdout encoding.
- Ensure the extraction returns the DataFrame cleanly for downstream consumption.

---

### 2. Transform Phase

#### [NEW] `src/transform.py`
Implement `transform_data(df: pd.DataFrame) -> pd.DataFrame` which will perform:
- **Data Type Conversions**:
  - Parse `InvoiceDate` into a standard Pandas datetime object (`YYYY-MM-DD HH:MM:SS`).
  - Convert `CustomerID` to integer/string representation and handle `NaN` values.
- **Data Cleaning**:
  - Trim whitespace and standardize casing on `Description`, `StockCode`, and `Country`.
  - Handle missing data: drop or fill empty descriptions and customer IDs.
  - Filter or flag negative quantities and prices (e.g. cancellations vs. standard sales).
- **Feature Engineering**:
  - Add `TotalAmount` (`Quantity` * `UnitPrice`).
  - Add time-based features: `Year`, `Month`, `Day`, `Hour`, `DayOfWeek` for analytics readiness.
- Provide a summary function to log distribution/anomalies post-transformation.

---

### 3. Load Phase

#### [NEW] `src/load.py`
Implement database insertion and file serialization:
- **SQLite Loading (`load_to_sqlite`)**:
  - Connect to `data/ecommerce.db` (auto-created if not exists).
  - Create three optimized tables with appropriate keys:
    - `dim_customers`: Unique customer mapping (`CustomerID`, `Country`).
    - `dim_products`: Unique product catalog (`StockCode`, `Description`).
    - `fact_transactions`: Transaction facts (`InvoiceNo`, `StockCode`, `CustomerID`, `Quantity`, `InvoiceDate`, `UnitPrice`, `TotalAmount`, `IsCancellation`).
  - Save records using transactional batch inserts (`pandas.DataFrame.to_sql` or direct `sqlite3` execution for control).
- **File Loading (`load_to_csv`)**:
  - Save the unified flat table to a clean CSV format under `data/processed_ecommerce.csv` (or Parquet if pandas/pyarrow is installed).

---

### 4. Orchestration

#### [NEW] `src/main.py`
Create a pipeline orchestrator to run the ETL steps in sequence:
```python
def run_pipeline():
    # 1. Extract
    raw_df = extract_get_data(project_root)
    
    # 2. Transform
    clean_df = transform_data(raw_df)
    
    # 3. Load
    load_data(clean_df, project_root)
```
- Add logging to track duration and status of each phase.
- Support execution via command line: `python src/main.py`.

---

## Verification Plan

### Automated Tests
- We will add standard validation checks inside the runner to verify:
  1. No missing values in critical fields (`InvoiceNo`, `StockCode`, `Quantity`, `InvoiceDate`, `UnitPrice`).
  2. The database schema in SQLite matches the specifications.
  3. Row counts match after mapping.

### Manual Verification
- Execute `python src/main.py` and inspect console output.
- Query the SQLite database database tables using a Python script or sqlite CLI to verify data is correctly structured and loaded.
- Inspect the output files (`data/processed_ecommerce.csv`).
