# Companies House Data Pipeline

A comprehensive Python toolset to automate the download, parsing, enrichment, and merging of Companies House bulk data.  
Ideal for analysts, data engineers, and anyone who needs up-to-date company metadata and financial metrics in a unified CSV.

---

## 🚀 Features

- **Automated Download**  
  - Fetch the latest “basic” company snapshot (CSV)  
  - Fetch the last 12 months of accounts data (ZIP archives)  
  - Retries on failure, with exponential back-off  
  - Progress displayed via `tqdm` bars  
  - Skips already-downloaded files

- **Financials Parsing**  
  - Unzip and inspect each filing (HTML/XHTML)  
  - Extract inline XBRL facts and/or parse HTML tables  
  - Normalize metric names → `metric_tag`  
  - Output one row per company/period/metric in `all_metrics.csv`

- **Metadata Enrichment**  
  - Load Companies House “BasicCompanyDataAsOneFile-*.csv”  
  - Strip whitespace, select key columns:  
    - `CompanyNumber` → `company_number`  
    - `CompanyName`  
    - `RegAddress.PostCode`  
    - `CompanyCategory`  
    - `CompanyStatus`

- **Chunked Merge**  
  - Join `all_metrics.csv` (potentially multi-GB) with enrichment data  
  - Stream in adjustable chunks to control RAM usage  
  - Produce `merged_metrics.csv`  
  - Report count of unmatched `company_number` values

---

## 📂 Repository Layout

    .
    ├── scraper.py            # (1) Bulk-download latest basic snapshot & last 12 monthly archives
    ├── parse_financials.py   # (2) Unzip & parse each accounts archive → all_metrics.csv
    ├── enrichment_loader.py  # (3) Clean & select key columns from company_attributes.csv
    ├── join_metrics.py       # (4) Chunked left-join of metrics + enrichment → merged_metrics.csv
    ├── requirements.txt      # Python dependencies
    └── README.md             # This document

---

## ⚙️ Prerequisites

- **Python** ≥ 3.8  
- Install dependencies:
  ```bash
  pip install -r requirements.txt
