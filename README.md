# Python E-Commerce ETL Pipeline

A robust, production-grade **Extract, Transform, Load (ETL) pipeline** built in Python. This pipeline automates the retrieval of e-commerce transactional data from Kaggle, cleans and transforms it using Pandas, and loads it into a PostgreSQL database utilizing a clean, relational **Star Schema** with SQLAlchemy 2.0.

---

## 1. Pipeline Architecture & Data Flow

The pipeline is organized into three distinct, modular components coordinated by a central orchestrator: [main.py](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/main.py). 

```mermaid
graph TD
    %% Extract Phase
    subgraph Extract [Phase 1: Extract]
        A[Kaggle API / Local CSV] -->|Download / Read| B(extract_get_data)
        B -->|Pandas DataFrame Raw| C[df_extract]
    end

    %% Transform Phase
    subgraph Transform [Phase 2: Transform]
        C --> D(transform_all)
        D --> D1[Filter UnitPrice & Quantity >= 0]
        D1 --> D2[Calculate TotalPrice]
        D2 --> D3[Trim Whitespaces]
        D3 --> D4[Cast Date & CustomerID]
        D4 --> D5[Drop Nulls & Duplicates]
        D5 -->|Pandas DataFrame Clean| E[df_clean]
    end

    %% Load Phase
    subgraph Load [Phase 3: Load]
        E --> F[load.db_engine]
        F --> G(init_db)
        G --> H(clear_db)
        H --> I(load_dimensions)
        I --> J(load_fact)
        J -->|PostgreSQL Database| K[(ecommerce_db)]
    end

    style Extract fill:#f9f,stroke:#333,stroke-width:2px
    style Transform fill:#bbf,stroke:#333,stroke-width:2px
    style Load fill:#dfd,stroke:#333,stroke-width:2px
```

1. **Extract** ([src/extract.py](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/extract.py)): Downloads the raw dataset directly from Kaggle and ingests it into memory.
2. **Transform** ([src/transform.py](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/transform.py)): Cleans inconsistencies, handles missing values, removes duplicates, and calculates financial metrics.
3. **Load** ([src/load.py](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/load.py)): Establishes PostgreSQL connections, defines the schema, clears old state, and inserts dimensional and fact tables safely.

---

## 2. Database Star Schema

The database relies on an analytical **Star Schema** architecture to facilitate rapid querying and reporting.

```mermaid
erDiagram
    dim_customers ||--o{ fact_transactions : references
    dim_products ||--o{ fact_transactions : references

    dim_customers {
        int customerid PK "Unique Customer ID"
        varchar country "Country of origin"
    }

    dim_products {
        varchar stockcode PK "Unique Product/Stock Code"
        varchar description "Product description"
    }

    fact_transactions {
        bigint id PK "Auto-generated Transaction ID"
        varchar invoiceno "Invoice identifier"
        varchar stockcode FK "References dim_products"
        int quantity "Item quantity purchased"
        float unitprice "Price per individual item"
        float totalprice "Calculated total cost (qty * price)"
        timestamp invoicedate "Timestamp of transaction"
        int customerid FK "References dim_customers"
        varchar country "Country where invoice occurred"
    }
```

### Table Definitions
- **`dim_customers`** ([load.py:L20-24](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/load.py#L20-L24)): Stores deduplicated Customer IDs and their primary countries.
- **`dim_products`** ([load.py:L26-31](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/load.py#L26-L31)): Stores deduplicated stock codes and their text descriptions.
- **`fact_transactions`** ([load.py:L33-47](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/load.py#L33-L47)): Relates the customer and product dimensions with transactional variables such as prices, quantities, dates, and calculated totals.

---

## 3. Production Data Engineering Patterns

This codebase incorporates several industry best practices to ensure high reliability, data integrity, and pipeline robustness:

### A. Transaction Isolation & Failure Recovery
All load phases ([init_db](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/load.py#L15-L53), [load_dimensions](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/load.py#L56-L98), and [load_fact](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/load.py#L100-L130)) execute inside distinct SQLAlchemy database transaction blocks:
* SQL operations begin with `conn.begin()`.
* If all writes succeed, mutations are committed using `conn.commit()`.
* If a driver error or network exception occurs, changes are immediately rolled back with `conn.rollback()`, ensuring the database never enters a partially written state.

### B. Idempotency & Duplicate Prevention
To prevent primary key conflicts and duplicate records during incremental uploads:
* The loader retrieves active sets of primary keys (`customerid` and `stockcode`) directly from the target database in $O(1)$ search time.
* It compares incoming DataFrame records against these sets.
* Only new, unregistered entities are appended via `to_sql`.

### C. Failsafe CSV Extraction
When reading the incoming CSV data:
* [extract_data_csv](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/extract.py#L49-L104) attempts to read using standard `utf-8` encoding.
* If a `UnicodeDecodeError` occurs (common with special currency characters or vendor labels), it falls back gracefully to `ISO-8859-1` encoding to guarantee process continuity.

### D. Full Refresh Capability
To support clean testing and state synchronization:
* The function [clear_db](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/load.py#L132-L144) triggers `TRUNCATE TABLE ... CASCADE;` on the target relations to empty prior tables while preserving database constraints.

---

## 4. Getting Started

### Prerequisites
* **Python 3.10+**
* **PostgreSQL** instance running locally or remotely.

### Installation & Environment Setup

1. **Clone & Navigate** to the project directory:
   ```bash
   cd ETL_Pipeline_Python
   ```

2. **Create a Virtual Environment**:
   ```bash
   python -m venv .venv
   ```

3. **Activate the Virtual Environment**:
   * **Windows (PowerShell)**:
     ```powershell
     .venv\Scripts\Activate.ps1
     ```
   * **Windows (CMD)**:
     ```cmd
     .venv\Scripts\activate.bat
     ```
   * **Linux/macOS**:
     ```bash
     source .venv/bin/activate
     ```

4. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Configuration

Create a file named `.env` in the root directory (based on the sample format) to configure the target database URL:

```env
DATABASE_URL=postgresql+psycopg2://<username>:<password>@<host>:<port>/<dbname>
```

*Example Configuration:*
```env
DATABASE_URL=postgresql+psycopg2://postgres:password123$@localhost:5432/ecommerce_db
```

### Running the Pipeline

Before running, ensure your target PostgreSQL database is active and matches the connection details specified in `.env`. Run the main orchestrator script:

```bash
python main.py
```

#### Expected Output Logs:
```text
2026-07-06 12:00:00,000 - src.extract - extract.py - 120 - INFO - PHASE 1: EXTRACT CSV FORMATS
2026-07-06 12:00:00,100 - src.extract - extract.py - 20 - INFO - 🔄 Download Dataset From Kaggle
2026-07-06 12:00:02,400 - src.extract - extract.py - 29 - INFO - ✔ Download Dataset Kaggle is successful
2026-07-06 12:00:02,450 - src.extract - extract.py - 79 - INFO - 🔄 Reading CSV Files : data.csv
2026-07-06 12:00:03,800 - src.extract - extract.py - 86 - INFO - ✔ Read Data from data.csv is successfully
2026-07-06 12:00:03,900 - src.extract - extract.py - 168 - INFO - 📊 Dataset Loaded - Rows: 541909, Columns: 8
...
2026-07-06 12:00:05,000 - src.load - load.py - 49 - INFO - Database initialized successfully
2026-07-06 12:00:05,300 - src.load - load.py - 136 - INFO - Refresh Data POSTGREESQL
2026-07-06 12:00:07,000 - src.load - load.py - 84 - INFO - Loaded 4372 new customers
2026-07-06 12:00:08,200 - src.load - load.py - 90 - INFO - Loaded 3684 new products
2026-07-06 12:00:15,600 - src.load - load.py - 123 - INFO - Loaded 397884 new sales
```

---

## 5. Repository Directory Layout

```text
ETL_Pipeline_Python/
│
├── src/                            # Source code directory
│   ├── __init__.py                 # Packaged module definitions
│   ├── extract.py                  # Kaggle ingestion & CSV parsing logic
│   ├── transform.py                # Data type alignment, cleaning, and text normalization
│   └── load.py                     # PostgreSQL Schema design and database ingestion
│
├── main.py                         # Orchestrator & CLI entry point
├── requirements.txt                # Python package list
├── .gitignore                      # Git ignored files & environments
└── README.md                       # Repository overview documentation (this file)
```

---

## 6. References & Additional Documentation

For deeper investigations into the architecture designs, refer to the documents in the `/documents/` subdirectory:
* **Detailed Technical Documentation (Indonesian):** [ETL_Pipeline_Documentation.md](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/documents/ETL_Pipeline_Documentation.md)
* **Architectural Decisions & Future Work:** [conclusions.md](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/documents/conclusions.md)
* **Load Constraints Specification:** [Requirement_Of_Load.md](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/documents/requirements/Requirement_Of_Load.md)
* **Transformation Specification:** [Requirement_Of_Transform.md](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/documents/requirements/Requirement_Of_Transform.md)
