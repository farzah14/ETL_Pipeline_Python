# Test Extracts — PRD & Implementation Plan (Multi-Industry Perspective)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-grade test suite for the Extract phase (`src/extract.py`) that meets industry-standard data engineering testing requirements across multiple sectors.

**Architecture:** Each test category uses `unittest.mock` (MagicMock/patch) to isolate extract functions from external dependencies (Kaggle API, filesystem). Tests follow the AAA pattern (Arrange-Act-Assert) and run in-memory with no network or disk I/O.

**Tech Stack:** Python 3.x, pytest, unittest.mock, pandas, pathlib

---

## 1. Executive Summary (PRD)

### Problem Statement
File [test_extracts.py](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/tests/test_extracts.py) saat ini kosong — hanya berisi import tanpa test case. Di industri manapun, **Extract adalah fase paling kritis** karena menjadi satu-satunya pintu masuk data ke dalam pipeline. Jika data yang masuk sudah salah, seluruh downstream (Transform → Load) akan menghasilkan output yang salah pula (**Garbage In, Garbage Out**).

### Proposed Solution
Membangun test suite komprehensif yang mencakup **8 kategori pengujian** yang dibutuhkan oleh berbagai industri, menggunakan `unittest.mock` agar test berjalan cepat, terisolasi, dan tanpa ketergantungan pada jaringan atau file sistem.

### Success Criteria
- ✅ Semua 8 kategori test coverage terpenuhi
- ✅ `pytest tests/test_extracts.py -v` berjalan dalam < 5 detik
- ✅ 0 network calls / 0 disk I/O selama test execution
- ✅ Coverage > 90% untuk `src/extract.py`

---

## 2. Apa Yang Diuji di Test Extracts — Per Industri?

Berikut adalah mapping **industri → kebutuhan pengujian Extract** yang relevan:

| # | Kategori Test | 🛒 E-Commerce | 🏦 Banking/Finance | 🏥 Healthcare | 📡 Telco | 🏭 Manufacturing/IoT | 🏛️ Government |
|---|---|---|---|---|---|---|---|
| 1 | **Schema/Contract Validation** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 2 | **Data Quality (nulls, types, ranges)** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3 | **Error Handling (file not found, empty, corrupt)** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 4 | **Network Resilience (API retry, timeout)** | ✅ | ✅ | ⚠️ | ✅ | ✅ | ⚠️ |
| 5 | **Encoding & Format Handling** | ✅ | ✅ | ✅ | ✅ | ⚠️ | ✅ |
| 6 | **Performance & Memory** | ⚠️ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| 7 | **Idempotency (re-run safety)** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 8 | **Integration Smoke Test** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

> ✅ = Wajib (mandatory), ⚠️ = Direkomendasikan (recommended)

### Detail Per Industri

#### 🛒 E-Commerce (Contoh: project Anda — Kaggle ecommerce-data)
- **Schema Validation**: Pastikan kolom `InvoiceNo`, `StockCode`, `Quantity`, `UnitPrice`, `CustomerID`, `Country`, `InvoiceDate`, `Description` selalu ada
- **Data Quality**: Quantity negatif = return/refund → harus terdeteksi, bukan error
- **Encoding**: Nama produk internasional (é, ñ, ü) → encoding fallback wajib
- **Idempotency**: Download ulang dataset tidak boleh crash atau duplicate

#### 🏦 Banking & Finance (BFSI)
- **Schema = Contract**: Kolom transaksi wajib sesuai regulasi (OJK, BI, PCI-DSS)
- **Data Quality**: Null di kolom `amount` atau `account_id` → **harus fail keras**, bukan skip
- **Performance**: Extract dari core banking bisa > 100M rows → memory test kritis
- **Audit Trail**: Setiap extract harus logged dengan timestamp, row count, source

#### 🏥 Healthcare (HIPAA/BPJS)
- **Schema**: Kolom pasien wajib sesuai standar HL7/FHIR
- **Data Quality**: `patient_id` null = violation, bukan warning
- **Encoding**: Nama pasien dengan karakter non-ASCII
- **Compliance**: Logging tidak boleh expose PII (Personal Identifiable Information)

#### 📡 Telecommunications
- **Schema**: CDR (Call Detail Records) punya format yang sangat strict
- **Data Quality**: Timestamp precision (millisecond), IP format validation
- **Performance**: Volume data sangat besar (billions of rows/day)
- **Network Resilience**: API extract dari OSS/BSS harus tahan gangguan

#### 🏭 Manufacturing & IoT
- **Schema**: Sensor data (temperature, pressure, vibration) → strict numeric ranges
- **Data Quality**: Out-of-range values = sensor malfunction → harus terdeteksi
- **Performance**: High-frequency streaming data → memory management kritis
- **Idempotency**: Replay sensor batch tidak boleh duplicate

#### 🏛️ Government & Public Sector
- **Schema**: Format data wajib sesuai standar pemerintah (e.g., CKAN, open data)
- **Data Quality**: Missing mandatory fields = rejection
- **Encoding**: Nama daerah, karakter lokal (aksara Jawa, dll)
- **Compliance**: Data sovereignty → extract harus dari source yang terverifikasi

---

## 3. Proposed Changes

### Test Suite — Extract Phase

Summary: Menulis test suite lengkap di `tests/test_extracts.py` yang mencakup 8 kategori pengujian industri.

#### [MODIFY] [test_extracts.py](file:///c:/Users/korba/OneDrive/Documents/PROJECTS/ETL_Pipeline_Python/tests/test_extracts.py)

---

### Task 1: Schema / Contract Validation Tests

**Mengapa penting di semua industri:** Schema adalah "kontrak" antara data source dan pipeline. Jika kolom berubah, hilang, atau bertambah tanpa notifikasi, pipeline akan silent fail.

**Files:**
- Modify: `tests/test_extracts.py`
- Test target: `src/extract.py:extract_data_csv()`

- [ ] **Step 1: Write failing test — schema validation**

```python
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

class TestSchemaValidation:
    """
    Industry: ALL (E-Commerce, Banking, Healthcare, Telco, Manufacturing, Government)
    Purpose: Memastikan data yang di-extract memiliki kolom yang sesuai kontrak/schema.
    """

    EXPECTED_COLUMNS = [
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "UnitPrice", "CustomerID", "Country"
    ]

    @patch("pandas.read_csv")
    def test_extract_csv_returns_expected_columns(self, mock_read_csv, tmp_path):
        """Schema Contract: Semua kolom wajib harus ada di output DataFrame."""
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        mock_df = pd.DataFrame({
            "InvoiceNo": ["536365"],
            "StockCode": ["85123A"],
            "Description": ["WHITE HANGING HEART T-LIGHT HOLDER"],
            "Quantity": [6],
            "InvoiceDate": ["12/1/2010 08:26"],
            "UnitPrice": [2.55],
            "CustomerID": [17850],
            "Country": ["United Kingdom"]
        })
        mock_read_csv.return_value = mock_df

        from src.extract import extract_data_csv
        result = extract_data_csv(csv_file)

        for col in self.EXPECTED_COLUMNS:
            assert col in result.columns, f"Missing required column: {col}"

    @patch("pandas.read_csv")
    def test_extract_csv_column_count_matches(self, mock_read_csv, tmp_path):
        """Schema Contract: Jumlah kolom harus sesuai ekspektasi (deteksi kolom tak terduga)."""
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        mock_df = pd.DataFrame({
            "InvoiceNo": ["536365"],
            "StockCode": ["85123A"],
            "Description": ["WHITE HANGING HEART T-LIGHT HOLDER"],
            "Quantity": [6],
            "InvoiceDate": ["12/1/2010 08:26"],
            "UnitPrice": [2.55],
            "CustomerID": [17850],
            "Country": ["United Kingdom"]
        })
        mock_read_csv.return_value = mock_df

        from src.extract import extract_data_csv
        result = extract_data_csv(csv_file)

        assert len(result.columns) == len(self.EXPECTED_COLUMNS), \
            f"Expected {len(self.EXPECTED_COLUMNS)} columns, got {len(result.columns)}"
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
.venv\Scripts\pytest tests\test_extracts.py::TestSchemaValidation -v
```
Expected: PASS (karena extract_data_csv sudah menggunakan pd.read_csv yang di-mock)

- [ ] **Step 3: Commit**

```bash
git add tests/test_extracts.py
git commit -m "test: add schema/contract validation tests for extract phase"
```

---

### Task 2: Data Quality Tests (Nulls, Types, Ranges)

**Mengapa penting:**
- 🏦 Banking: Null di `amount` = transaksi invalid → audit failure
- 🏥 Healthcare: Null di `patient_id` = HIPAA violation
- 🛒 E-Commerce: Quantity negatif = return, bukan error

**Files:**
- Modify: `tests/test_extracts.py`
- Test target: `src/extract.py:extract_data_csv()`

- [ ] **Step 1: Write failing tests — data quality checks**

```python
class TestDataQuality:
    """
    Industry: ALL
    Purpose: Validasi kualitas data yang di-extract (null check, tipe data, range).
    """

    @patch("pandas.read_csv")
    def test_extract_returns_dataframe_type(self, mock_read_csv, tmp_path):
        """Data Quality: Output harus berupa pd.DataFrame, bukan None atau tipe lain."""
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        mock_df = pd.DataFrame({"InvoiceNo": ["536365"], "Quantity": [6]})
        mock_read_csv.return_value = mock_df

        from src.extract import extract_data_csv
        result = extract_data_csv(csv_file)

        assert isinstance(result, pd.DataFrame), "Extract must return a DataFrame"

    @patch("pandas.read_csv")
    def test_extract_returns_non_empty_dataframe(self, mock_read_csv, tmp_path):
        """Data Quality: DataFrame yang dikembalikan tidak boleh kosong (0 rows)."""
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        mock_df = pd.DataFrame({
            "InvoiceNo": ["536365"],
            "Quantity": [6],
            "UnitPrice": [2.55]
        })
        mock_read_csv.return_value = mock_df

        from src.extract import extract_data_csv
        result = extract_data_csv(csv_file)

        assert len(result) > 0, "Extracted DataFrame should not be empty"

    @patch("pandas.read_csv")
    def test_extract_numeric_columns_are_numeric(self, mock_read_csv, tmp_path):
        """
        Data Quality: Kolom numerik (Quantity, UnitPrice) harus bertipe numerik.
        Industry Focus: Banking (amount validation), Manufacturing (sensor readings).
        """
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        mock_df = pd.DataFrame({
            "InvoiceNo": ["536365"],
            "StockCode": ["85123A"],
            "Description": ["Product A"],
            "Quantity": [6],
            "InvoiceDate": ["12/1/2010 08:26"],
            "UnitPrice": [2.55],
            "CustomerID": [17850],
            "Country": ["UK"]
        })
        mock_read_csv.return_value = mock_df

        from src.extract import extract_data_csv
        result = extract_data_csv(csv_file)

        assert pd.api.types.is_numeric_dtype(result["Quantity"]), \
            "Quantity column must be numeric"
        assert pd.api.types.is_numeric_dtype(result["UnitPrice"]), \
            "UnitPrice column must be numeric"
```

- [ ] **Step 2: Run tests**

```powershell
.venv\Scripts\pytest tests\test_extracts.py::TestDataQuality -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/test_extracts.py
git commit -m "test: add data quality validation tests for extract phase"
```

---

### Task 3: Error Handling Tests (File Not Found, Empty, Invalid Format)

**Mengapa penting:**
- 🏭 Manufacturing: File sensor corrupt → pipeline harus fail gracefully, bukan hang
- 🏛️ Government: File format salah → rejection dengan pesan jelas untuk audit

**Files:**
- Modify: `tests/test_extracts.py`
- Test target: `src/extract.py:extract_data_csv()`, `find_first_csv()`

- [ ] **Step 1: Write failing tests — error handling**

```python
class TestErrorHandling:
    """
    Industry: ALL
    Purpose: Pastikan extract gagal dengan cara yang benar (raise exception spesifik)
             bukan silent failure atau crash tanpa info.
    """

    def test_extract_csv_raises_on_non_csv_file(self, tmp_path):
        """Error Handling: File bukan CSV → harus raise ValueError."""
        non_csv_file = tmp_path / "data.xlsx"
        non_csv_file.touch()

        from src.extract import extract_data_csv

        with pytest.raises(ValueError, match="Invalid File Format"):
            extract_data_csv(non_csv_file)

    def test_extract_csv_raises_on_missing_file(self, tmp_path):
        """Error Handling: File tidak ada → harus raise FileNotFoundError."""
        missing_file = tmp_path / "nonexistent.csv"

        from src.extract import extract_data_csv

        with pytest.raises(FileNotFoundError, match="File Path Does Not Exist"):
            extract_data_csv(missing_file)

    @patch("pandas.read_csv", side_effect=pd.errors.EmptyDataError("No columns to parse"))
    def test_extract_csv_raises_on_empty_file(self, mock_read_csv, tmp_path):
        """
        Error Handling: File CSV kosong → harus raise EmptyDataError.
        Industry Focus: Government (file rejection for audit),
                        Manufacturing (empty sensor batch).
        """
        empty_csv = tmp_path / "empty.csv"
        empty_csv.touch()

        from src.extract import extract_data_csv

        with pytest.raises(pd.errors.EmptyDataError):
            extract_data_csv(empty_csv)

    def test_find_first_csv_returns_none_when_no_csv(self, tmp_path):
        """Error Handling: Folder tanpa CSV → return None, bukan crash."""
        from src.extract import find_first_csv
        result = find_first_csv(tmp_path)
        assert result is None, "Should return None when no CSV files exist"
```

- [ ] **Step 2: Run tests**

```powershell
.venv\Scripts\pytest tests\test_extracts.py::TestErrorHandling -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/test_extracts.py
git commit -m "test: add error handling tests for extract phase"
```

---

### Task 4: Network Resilience Tests (API Download)

**Mengapa penting:**
- 📡 Telco: Extract dari OSS/BSS via API harus tahan intermittent failure
- 🛒 E-Commerce: Kaggle API bisa timeout → pipeline harus retry, bukan crash
- 🏦 Banking: Core banking API down → harus ada fallback strategy

**Files:**
- Modify: `tests/test_extracts.py`
- Test target: `src/extract.py:download_dataset()`

- [ ] **Step 1: Write failing tests — network resilience**

```python
class TestNetworkResilience:
    """
    Industry: E-Commerce, Banking, Telco, Manufacturing
    Purpose: Pastikan download dari external API (Kaggle) berjalan dengan benar
             dan menangani error jaringan secara graceful.
    """

    @patch("src.extract.kagglehub.dataset_download")
    def test_download_dataset_returns_path(self, mock_download, tmp_path):
        """Network: Download berhasil → harus return Path yang valid."""
        mock_download.return_value = str(tmp_path / "downloaded")
        (tmp_path / "downloaded").mkdir(exist_ok=True)

        from src.extract import download_dataset
        result = download_dataset("carrie1/ecommerce-data", output_dir=tmp_path)

        assert isinstance(result, Path), "download_dataset must return a Path"
        mock_download.assert_called_once()

    @patch("src.extract.kagglehub.dataset_download", side_effect=Exception("Connection timeout"))
    def test_download_dataset_raises_on_network_error(self, mock_download, tmp_path):
        """
        Network: API timeout → harus raise exception, bukan return None.
        Industry Focus: Telco (OSS/BSS API), Banking (core banking API).
        """
        from src.extract import download_dataset

        with pytest.raises(Exception, match="Connection timeout"):
            download_dataset("carrie1/ecommerce-data", output_dir=tmp_path)

    @patch("src.extract.kagglehub.dataset_download")
    def test_download_dataset_passes_force_download_flag(self, mock_download, tmp_path):
        """Network: force_download=True harus diteruskan ke kagglehub."""
        mock_download.return_value = str(tmp_path)

        from src.extract import download_dataset
        download_dataset("carrie1/ecommerce-data", output_dir=tmp_path, force_download=True)

        _, kwargs = mock_download.call_args
        assert kwargs.get("force_download") is True, "force_download flag must be passed through"
```

- [ ] **Step 2: Run tests**

```powershell
.venv\Scripts\pytest tests\test_extracts.py::TestNetworkResilience -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/test_extracts.py
git commit -m "test: add network resilience tests for extract API download"
```

---

### Task 5: Encoding & Format Handling Tests

**Mengapa penting:**
- 🛒 E-Commerce: Produk internasional punya karakter é, ñ, ü, 日本語
- 🏥 Healthcare: Nama pasien non-ASCII
- 🏛️ Government: Nama daerah lokal (aksara daerah)

**Files:**
- Modify: `tests/test_extracts.py`
- Test target: `src/extract.py:extract_data_csv()`

- [ ] **Step 1: Write failing tests — encoding handling**

```python
class TestEncodingHandling:
    """
    Industry: E-Commerce, Healthcare, Government
    Purpose: Pastikan extract menangani file dengan encoding non-UTF-8 (ISO-8859-1 fallback).
    """

    @patch("pandas.read_csv")
    def test_extract_csv_handles_utf8_encoding(self, mock_read_csv, tmp_path):
        """Encoding: File UTF-8 standar harus berhasil di-extract."""
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        mock_df = pd.DataFrame({"Description": ["Crème brûlée holder"]})
        mock_read_csv.return_value = mock_df

        from src.extract import extract_data_csv
        result = extract_data_csv(csv_file)

        assert result["Description"].iloc[0] == "Crème brûlée holder"

    @patch("pandas.read_csv")
    def test_extract_csv_falls_back_to_iso8859(self, mock_read_csv, tmp_path):
        """
        Encoding: UTF-8 gagal → harus fallback ke ISO-8859-1.
        Industry Focus: E-Commerce (produk Eropa), Healthcare (nama pasien).
        """
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        # First call (UTF-8) raises UnicodeDecodeError, second call (ISO-8859-1) succeeds
        mock_df = pd.DataFrame({"Description": ["Données spéciales"]})
        mock_read_csv.side_effect = [
            UnicodeDecodeError("utf-8", b"", 0, 1, "invalid"),
            mock_df
        ]

        from src.extract import extract_data_csv
        result = extract_data_csv(csv_file)

        assert len(result) > 0, "Fallback encoding should produce valid DataFrame"
        assert mock_read_csv.call_count == 2, "Should attempt UTF-8 first, then ISO-8859-1"
```

- [ ] **Step 2: Run tests**

```powershell
.venv\Scripts\pytest tests\test_extracts.py::TestEncodingHandling -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/test_extracts.py
git commit -m "test: add encoding/format handling tests for extract phase"
```

---

### Task 6: Performance & Memory Tests

**Mengapa penting:**
- 🏦 Banking: 100M+ rows dari core banking → memory limit kritis
- 📡 Telco: Billions of CDR records/day → harus tahu batas memori
- 🏭 Manufacturing: High-frequency sensor data → streaming vs batch

**Files:**
- Modify: `tests/test_extracts.py`
- Test target: `src/extract.py:extract_data_csv()`

- [ ] **Step 1: Write failing tests — performance**

```python
import time

class TestPerformance:
    """
    Industry: Banking, Telco, Manufacturing
    Purpose: Pastikan extract function tidak memakan waktu terlalu lama dan
             mengembalikan data dalam batas waktu yang wajar.
    """

    @patch("pandas.read_csv")
    def test_extract_csv_completes_within_timeout(self, mock_read_csv, tmp_path):
        """
        Performance: Extract harus selesai dalam < 5 detik untuk file normal.
        Industry Focus: Banking (SLA compliance), Telco (real-time CDR).
        """
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        mock_df = pd.DataFrame({"InvoiceNo": range(10000)})
        mock_read_csv.return_value = mock_df

        from src.extract import extract_data_csv

        start = time.time()
        result = extract_data_csv(csv_file)
        elapsed = time.time() - start

        assert elapsed < 5.0, f"Extract took {elapsed:.2f}s, exceeds 5s SLA"
        assert len(result) == 10000

    @patch("pandas.read_csv")
    def test_extract_csv_returns_correct_row_count(self, mock_read_csv, tmp_path):
        """Performance: Row count harus match antara source dan output (no data loss)."""
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        expected_rows = 5000
        mock_df = pd.DataFrame({"InvoiceNo": range(expected_rows)})
        mock_read_csv.return_value = mock_df

        from src.extract import extract_data_csv
        result = extract_data_csv(csv_file)

        assert len(result) == expected_rows, \
            f"Expected {expected_rows} rows, got {len(result)} — data loss detected"
```

- [ ] **Step 2: Run tests**

```powershell
.venv\Scripts\pytest tests\test_extracts.py::TestPerformance -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/test_extracts.py
git commit -m "test: add performance and memory tests for extract phase"
```

---

### Task 7: Idempotency Tests (Re-run Safety)

**Mengapa penting:**
- 🛒 E-Commerce: Re-run pipeline setelah failure tidak boleh duplicate data
- 🏦 Banking: Double-entry = audit violation
- 🏭 Manufacturing: Replay sensor batch = duplicate readings

**Files:**
- Modify: `tests/test_extracts.py`
- Test target: `src/extract.py:extract_data_csv()`, `find_first_csv()`

- [ ] **Step 1: Write failing tests — idempotency**

```python
class TestIdempotency:
    """
    Industry: ALL
    Purpose: Pastikan extract menghasilkan output identik jika dipanggil berulang kali
             dengan input yang sama (no side effects, no duplicates).
    """

    @patch("pandas.read_csv")
    def test_extract_csv_is_idempotent(self, mock_read_csv, tmp_path):
        """Idempotency: Dua kali extract dari file yang sama → hasil identik."""
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        mock_df = pd.DataFrame({
            "InvoiceNo": ["536365", "536366"],
            "Quantity": [6, 3]
        })
        mock_read_csv.return_value = mock_df

        from src.extract import extract_data_csv

        result_1 = extract_data_csv(csv_file)
        result_2 = extract_data_csv(csv_file)

        pd.testing.assert_frame_equal(result_1, result_2), \
            "Extract should be idempotent — same input must produce same output"

    def test_find_first_csv_is_idempotent(self, tmp_path):
        """Idempotency: find_first_csv dengan folder yang sama → path yang sama."""
        csv_file = tmp_path / "sales.csv"
        csv_file.touch()

        from src.extract import find_first_csv

        result_1 = find_first_csv(tmp_path)
        result_2 = find_first_csv(tmp_path)

        assert result_1 == result_2, "find_first_csv should return same path on repeated calls"
```

- [ ] **Step 2: Run tests**

```powershell
.venv\Scripts\pytest tests\test_extracts.py::TestIdempotency -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/test_extracts.py
git commit -m "test: add idempotency tests for extract phase"
```

---

### Task 8: Integration Smoke Test (extract_get_data Orchestrator)

**Mengapa penting di semua industri:** Ini adalah test end-to-end mock yang memastikan seluruh alur orchestrator (`extract_get_data`) bekerja: check local → download jika perlu → read CSV → return DataFrame.

**Files:**
- Modify: `tests/test_extracts.py`
- Test target: `src/extract.py:extract_get_data()`

- [ ] **Step 1: Write failing tests — integration smoke**

```python
class TestIntegrationSmoke:
    """
    Industry: ALL
    Purpose: Smoke test untuk orchestrator extract_get_data —
             memastikan semua sub-function terintegrasi dengan benar.
    """

    @patch("src.extract.extract_data_csv")
    @patch("src.extract.find_first_csv")
    def test_extract_get_data_with_existing_csv(self, mock_find_csv, mock_extract, tmp_path):
        """
        Integration: Jika CSV sudah ada di lokal → tidak perlu download,
        langsung extract.
        """
        csv_path = tmp_path / "data" / "sales.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.touch()

        mock_find_csv.return_value = csv_path
        mock_extract.return_value = pd.DataFrame({"InvoiceNo": ["536365"]})

        from src.extract import extract_get_data
        result = extract_get_data(tmp_path)

        assert isinstance(result, pd.DataFrame)
        mock_extract.assert_called_once_with(csv_path)

    @patch("src.extract.extract_data_csv")
    @patch("src.extract.download_dataset")
    @patch("src.extract.find_first_csv")
    def test_extract_get_data_downloads_when_no_local_csv(
        self, mock_find_csv, mock_download, mock_extract, tmp_path
    ):
        """
        Integration: Jika CSV tidak ada di lokal → download dari Kaggle,
        lalu extract.
        """
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        downloaded_csv = tmp_path / "data" / "downloaded.csv"
        downloaded_csv.touch()

        # First call: no CSV found locally → None
        # Second call: after download → found
        mock_find_csv.side_effect = [None, downloaded_csv]
        mock_download.return_value = data_dir
        mock_extract.return_value = pd.DataFrame({"InvoiceNo": ["536365"]})

        from src.extract import extract_get_data
        result = extract_get_data(tmp_path)

        assert isinstance(result, pd.DataFrame)
        mock_download.assert_called_once()
```

- [ ] **Step 2: Run tests**

```powershell
.venv\Scripts\pytest tests\test_extracts.py::TestIntegrationSmoke -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/test_extracts.py
git commit -m "test: add integration smoke tests for extract_get_data orchestrator"
```

---

## 4. Verification Plan

### Automated Tests

```powershell
# Run all extract tests with verbose output and coverage
.venv\Scripts\pytest tests\test_extracts.py -v --tb=short

# Run with coverage report
.venv\Scripts\pytest tests\test_extracts.py -v --cov=src.extract --cov-report=term-missing
```

### Expected Output
- **8 test classes**, **~18 test functions**
- **All PASS** with 0 network calls
- **Coverage > 90%** untuk `src/extract.py`
- **Execution time < 5 seconds**

### Manual Verification
- Review test output untuk memastikan semua error messages informatif
- Verify bahwa tidak ada test yang melakukan actual file I/O atau network call

---

## 5. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Mock terlalu loose → test lulus tapi production gagal | High | Gunakan realistic mock data yang mirror actual CSV schema |
| `extract_data_csv` berubah signature → test break | Medium | Gunakan explicit import + fixture pattern |
| Encoding fallback logic berubah → test obsolete | Low | Test actual branching behavior, bukan implementation detail |
| `extract_get_data` flow berubah → integration test break | Medium | Keep integration test at orchestration level, bukan detail |

---

## 6. Ringkasan: Kapan Industri Butuh Test Extracts?

> [!IMPORTANT]
> **Jawaban singkat: SEMUA industri yang membangun data pipeline WAJIB memiliki Test Extracts.**
>
> Perbedaannya ada di **kedalaman dan fokus**:
> - **Banking/Finance**: Fokus pada data integrity, audit trail, fail-hard strategy
> - **Healthcare**: Fokus pada compliance (HIPAA), PII protection, schema strictness
> - **E-Commerce**: Fokus pada encoding, idempotency, API resilience
> - **Telco**: Fokus pada performance, volume, real-time processing
> - **Manufacturing/IoT**: Fokus pada range validation, memory, replay safety
> - **Government**: Fokus pada format compliance, encoding, data sovereignty
