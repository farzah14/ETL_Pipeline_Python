# Extract Phase Implementation Plan

Provide a design and step-by-step instructions for refactoring and enhancing the data extraction phase of the Python ETL pipeline in `src/extract.py`. This plan is based on the findings in the code review and focuses on reliability, logging standards, memory safety, and API resilience, using explanation of instructions ("what" and "why") without source code.

## User Review Required

> [!IMPORTANT]
> **API Credentials & Network Dependencies**:
> 1. **Kaggle API Credentials**: The extraction relies on `kagglehub` which expects Kaggle API credentials (`kaggle.json` with username and key). We assume these are configured in the environment.
> 2. **Retry Strategy**: We propose an exponential backoff retry mechanism (3 attempts, starting with a 2-second delay) for dataset downloads. Let us know if you prefer a different limit.
> 3. **Memory safety**: For this size (~45MB), a single pandas load is acceptable. However, for scalability, we plan to configure the reader to enforce column types to reduce memory usage during load.

## Open Questions

> [!WARNING]
> - Should we implement a strict timeout (e.g. 5 minutes) on the Kaggle download to prevent the pipeline from hanging indefinitely in case of connection dropouts?
> - Is there any requirement to verify the file checksum after downloading to guarantee file integrity?

---

## Proposed Changes

### ETL Pipeline - Extract Phase

The extraction phase is the gateway of the ETL pipeline. It must be resilient to network failures, handle local cache directories cleanly, avoid locking/blocking resources, and fail gracefully with informative logs.

#### [MODIFY] [extract.py](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/src/extract.py)

**Instructions for `src/extract.py`**:

##### 1. Clean Up Logging Configuration (Module-level Isolation)
* **What**: Remove `logging.basicConfig(...)` from the global namespace in the module. Keep only `logger = logging.getLogger(__name__)`. Move the configuration of `logging.basicConfig` to the `if __name__ == "__main__":` block.
* **Why**: Mutating the global logging configuration at the module import level is an anti-pattern. If another developer or orchestrator imports `extract.py`, it will overwrite their logging configurations. Placing basic config inside the main block ensures it only executes when the script is run directly.

##### 2. Fix Emojis in Logging (Unicode Compatibility)
* **What**: Replace terminal emojis (e.g. `🔄`, `✔`, `👀`, `❌`) with standard ASCII equivalents (e.g. `[RUN]`, `[SUCCESS]`, `[ERROR]`, `[WARN]`).
* **Why**: On Windows environments, stdout encoding defaults to `cp1252` instead of `UTF-8`. Outputting non-ASCII characters directly to the console or logs can cause `UnicodeEncodeError` and crash the pipeline.

##### 3. Correct the Downstream Control-Flow Bug
* **What**: Modify the `find_first_csv` function to take an optional boolean flag `raise_on_empty` which defaults to `True`. When scanning for files in the dataset directory:
  - If no files are found and `raise_on_empty` is `False`, return `None` instead of raising a `ValueError`.
  - In `extract_get_data`, call `find_first_csv` with `raise_on_empty=False` during the initial check. If it returns `None`, download the dataset and then call `find_first_csv` with `raise_on_empty=True` to confirm the download contains the file.
* **Why**: The current code raises a `ValueError` in `find_first_csv` if the local data directory is empty. This prevents the conditional `if not csv_path:` in the orchestrator from ever executing, rendering the download step unreachable and crashing the pipeline before it attempts to fetch the file.

##### 4. Add Resilience & Exponential Backoff for API Downloads
* **What**: Wrap the `kagglehub.dataset_download` call in a loop with retry logic (e.g., up to 3 attempts). If a connection error occurs, catch the exception, log a warning, calculate a delay using exponential backoff (e.g., `delay = factor * attempt`), sleep for that duration, and retry. If all attempts fail, raise a `RuntimeError`.
* **Why**: Network calls to external APIs are unreliable. Implementing exponential backoff retries prevents transient errors (e.g., DNS timeouts, brief rate limiting) from immediately crashing the pipeline, improving overall pipeline stability.

##### 5. Optimize Memory Utilization with Schema Enforcement
* **What**: In `extract_data_csv`, define a strict dictionary mapping columns to their appropriate pandas datatypes (e.g. strings for `InvoiceNo`, `StockCode`, `Country`, and `CustomerID`, and numeric for `Quantity` and `UnitPrice`). Pass this dictionary to `pd.read_csv` using the `dtype` parameter.
* **Why**: By default, pandas infers data types by reading chunks of data, which is slow and memory-intensive (often defaulting integers to 64-bit and strings to raw objects). Enforcing data types at reading time significantly reduces memory overhead and prevents downstream type mismatches.

##### 6. Standardize Exception Catching
* **What**: In `extract_data_csv`, replace the catch-all `except Exception as e:` block with specific checks (e.g., catch `pd.errors.EmptyDataError` for empty files, `FileNotFoundError` for missing paths). Avoid capturing and re-raising general exceptions without adding logging context or wrapping them in custom exceptions, as it obscures stack traces.
* **Why**: Catch-all blocks that simply re-raise do not add value and can hide unexpected bugs. Specifying exceptions makes debugging simpler and allows targeted recovery logic.

---

## Verification Plan

### Automated Tests
We will write a test suite using pytest to mock network calls and verify local file check logic:
```powershell
# Run the extraction unit tests to verify error recovery
.venv\Scripts\pytest tests/test_extract.py -v
```

### Manual Verification
* Delete the `data` directory (or rename it) to verify that:
  - Running the script downloads the data from Kaggle automatically.
  - No control-flow crash occurs when the folder is missing or empty.
* Verify log outputs in Windows PowerShell:
  - No encoding errors are thrown during log printing.
  - Logs clearly show search, download, and file read actions in sequence.
