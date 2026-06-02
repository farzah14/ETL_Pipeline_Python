# Best Practice & Requirements Fase Load (ETL Pipeline - PostgreSQL)

Dokumen ini menjelaskan spesifikasi, arsitektur, dan urutan yang direkomendasikan untuk melakukan fase **Load** pada pipeline data ETL ke database **PostgreSQL** sesuai dengan standar industri.

---

## 🏗️ 1. Arsitektur Database: Star Schema (Skema Bintang)

Dalam industri data engineering, data transaksional mentah (*flat table*) yang telah dibersihkan pada fase **Transform** harus dimodelkan kembali sebelum dimuat ke database analitik. Desain yang paling umum dan efisien adalah **Star Schema** yang membagi data menjadi dua jenis tabel:

### A. Tabel Dimensi (Dimension Tables - `dim_*`)
Menyimpan data deskriptif atau atribut dari suatu entitas.
* **`dim_customers`**: Menyimpan informasi unik pelanggan.
  * *Kolom*: `CustomerID` (INT, PRIMARY KEY), `Country` (VARCHAR(100)), `load_timestamp` (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
* **`dim_products`**: Menyimpan katalog unik produk.
  * *Kolom*: `StockCode` (VARCHAR(50), PRIMARY KEY), `Description` (TEXT), `load_timestamp` (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)

### B. Tabel Fakta (Fact Table - `fact_*`)
Menyimpan metrik kuantitatif atau fakta bisnis yang merujuk pada tabel dimensi.
* **`fact_transactions`**: Menyimpan transaksi penjualan individu.
  * *Kolom*: `TransactionID` (SERIAL, PRIMARY KEY), `InvoiceNo` (VARCHAR(50)), `StockCode` (VARCHAR(50), FOREIGN KEY), `CustomerID` (INT, FOREIGN KEY), `Quantity` (INT), `InvoiceDate` (TIMESTAMP), `UnitPrice` (NUMERIC(10,2)), `TotalAmount` (NUMERIC(12,2)), `IsCancellation` (SMALLINT), `load_timestamp` (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)

---

## 🔄 2. Strategi Loading Data & Koneksi (PostgreSQL)

Fase loading harus menentukan strategi penulisan data agar tidak memicu duplikasi data jika ETL dijalankan lebih dari sekali (*Idempotency*).

### A. Koneksi Database (SQLAlchemy Engine)
Di industri, koneksi ke PostgreSQL dikelola menggunakan **SQLAlchemy Engine** karena Pandas memiliki integrasi asli dengannya melalui metode `.to_sql()`. Parameter koneksi disimpan dalam format Connection URI:
```text
postgresql://username:password@host:port/database_name
```
*Sangat disarankan menyimpan informasi kredensial ini di file `.env` (Environment Variables) agar aman.*

### B. Strategi untuk Tabel Dimensi (UPSERT)
Karena pelanggan atau deskripsi produk bisa berubah, kita menggunakan metode **UPSERT**:
* Jika data key baru belum ada di database $\rightarrow$ lakukan **Insert**.
* Jika data key sudah ada di database $\rightarrow$ lakukan **Update** (atau abaikan konflik menggunakan `ON CONFLICT DO NOTHING`).
* Pada implementasi Pandas, kita membandingkan data baru dengan data yang sudah ada di database (*Delta Load*), lalu hanya melakukan append data baru ke database.

### C. Strategi untuk Tabel Fakta (Append Only)
Tabel fakta merekam kejadian sejarah (transaksi) sehingga kita menggunakan metode **Append Only**:
* Data transaksi baru langsung ditambahkan ke tabel menggunakan `to_sql` dengan `if_exists='append'`.
* **Pencegahan Duplikat**: Sebelum menulis data transaksi, pipeline harus memastikan data transaksi unik, atau membersihkan data transaksi lama pada batch tanggal yang sama untuk menjaga idempotensi.

---

## 🔒 3. Transaksi Database (ACID - Atomicity)

Fase Load wajib menerapkan prinsip **Atomicity** (All-or-Nothing) menggunakan mekanisme **Database Transaction**:
* Seluruh tabel dimensi (`dim_customers`, `dim_products`) dan tabel fakta (`fact_transactions`) harus dimuat di dalam **satu blok transaksi yang sama**.
* Jika salah satu tabel gagal dimuat (misal karena error koneksi atau pelanggaran constraint), database harus melakukan **ROLLBACK** untuk mengembalikan database ke keadaan sebelum ETL dimulai.
* Database hanya melakukan **COMMIT** jika seluruh proses pemuatan data ke semua tabel berhasil tanpa error sama sekali. Ini mencegah terjadinya kondisi data yang hanya terisi sebagian (*half-loaded* / *corrupted database*).

---

## 📊 4. Audit & Metadata Lineage

Setiap baris data yang dimuat ke dalam database harus dilengkapi dengan kolom metadata audit untuk mempermudah pelacakan jika terjadi masalah kualitas data (*data lineage*):
1. **`load_timestamp`**: Waktu (timestamp UTC) kapan baris data tersebut dimasukkan ke database.
2. **`source_file`**: Nama file sumber asal data tersebut diekstrak (misal: `data.csv`).

---

## 🔍 5. Urutan Eksekusi Fase Load (Sequential Loading)

Karena adanya dependensi relasional (*Foreign Key*), pemuatan data ke database harus mengikuti urutan sekuensial yang ketat:

```mermaid
sequenceDiagram
    participant ETL as ETL Pipeline
    participant DB as PostgreSQL Database
    
    ETL->>DB: 1. Mulai Transaksi (BEGIN TRANSACTION)
    ETL->>DB: 2. Load Tabel Dimensi: dim_customers (UPSERT)
    ETL->>DB: 3. Load Tabel Dimensi: dim_products (UPSERT)
    Note over ETL,DB: Dimensi harus selesai di-load sebelum Fakta
    ETL->>DB: 4. Load Tabel Fakta: fact_transactions (APPEND / FK Verified)
    alt Sukses
        ETL->>DB: 5. Simpan Perubahan (COMMIT)
        Note over ETL: Log: Load Berhasil
    else Error / Gagal
        ETL->>DB: 5. Batalkan Perubahan (ROLLBACK)
        Note over ETL: Log: Error & Rollback
    end
```
