# Financial Data Transformation & ETL Pipeline

---

## Overview

A Python-based ETL (Extract, Transform, Load) pipeline that parses Tally ERP financial data from XML format, transforms it into structured tabular datasets, and exports it to Excel — enabling clean, analysis-ready financial reporting.

The pipeline automates the extraction of monthly debit, credit, and closing balance records from raw Tally ERP XML output, applies validation and transformation logic, and produces a structured Excel output with a computed summary row.

---

## ETL Pipeline Architecture

```
Source: Tally ERP → Financial Data.xml
            ↓
    Extract (xml.etree.ElementTree)
    - Parse DSPPERIOD (month labels)
    - Parse DSPACCINFO (financial entries)
    - Extract: Debit (DSPDRAMTA), Credit (DSPCRAMTA), Closing Balance (DSPCLAMTA)
            ↓
    Transform (Pandas)
    - Safe type conversion (safe_float)
    - Handle missing / null / empty values
    - Structure into tabular rows
    - Append computed summary (Totals) row
            ↓
    Load (openpyxl / pandas)
    - Export to financial_data.xlsx
    - Analysis-ready structured output
```

---

## Project Structure

```
├── Data.py                      # ETL pipeline script (main)
├── Financial Data.xml           # Source: Tally ERP XML financial data
├── Sample Financial Data.xlsx   # Sample structured output for reference
├── Data.xlsx                    # Generated output after running pipeline
├── images/                      # Visualizations and output screenshots
└── README.md                    # Project documentation
```

---

## How It Works — Code Walkthrough

### 1. XML Parsing (`parse_tally_xml`)

Reads Tally ERP XML and extracts monthly financial records:

```python
import xml.etree.ElementTree as ET

periods   = root.findall("DSPPERIOD")     # Month labels
accinfos  = root.findall("DSPACCINFO")    # Financial entries per month

# Per entry:
credit  = accinfos[i].find(".//DSPCRAMTA")   # Credit Amount
closing = accinfos[i].find(".//DSPCLAMTA")   # Closing Balance
debit   = accinfos[i].find(".//DSPDRAMTA")   # Debit Amount
```

### 2. Safe Type Conversion (`safe_float`)

Handles empty strings, `None`, and invalid values gracefully — returns `0.0` instead of raising errors:

```python
def safe_float(value):
    if value is None or str(value).strip() == "":
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0
```

### 3. Output Structure

Each row in the output Excel contains:

| Column | Description |
|--------|-------------|
| Month | Period label from Tally ERP |
| Debit Amount | Total debits for the period |
| Credit Amount | Total credits for the period |
| Closing Balance | End-of-period closing balance |
| **Total** (last row) | Sum of Debit, Credit; final Closing Balance |

### 4. Export

```python
df.to_excel("financial_data.xlsx", index=False)
```

---

## How to Run

```bash
# 1. Clone the repository
git clone https://github.com/ramubattu321/Financial-Data-Transformation-ETL-Analysis.git
cd Financial-Data-Transformation-ETL-Analysis

# 2. Install dependencies
pip install pandas openpyxl

# 3. Run the ETL pipeline
python Data.py
```

**Expected output:**
```
XML successfully converted to Excel!
Output file created: financial_data.xlsx
```

---

## Data Validation & Error Handling

| Scenario | Handling |
|----------|----------|
| Missing XML field | Returns `0.0` via `safe_float` |
| Empty string value | Returns `0.0` via `safe_float` |
| XML file not found | Prints error message and exits gracefully |
| No data extracted | Prints warning and exits gracefully |
| Mismatched period/entry counts | Uses `min(len(periods), len(accinfos))` to align safely |

---

## Technologies Used

| Tool | Purpose |
|------|---------|
| Python | Core scripting language |
| `xml.etree.ElementTree` | XML parsing — Tally ERP data extraction |
| Pandas | Data transformation and structuring |
| openpyxl | Excel export |
| Pathlib | File path handling |

---

## Applications

- Financial reporting from Tally ERP exports
- Audit preparation — clean, structured transaction records
- Data preparation for BI tools (Power BI, Excel dashboards)
- Transaction trend analysis across periods
- Automated monthly financial reconciliation

---

## Author
Ramu Battu
**Ramu Battu**
MS in Data Analytics — California State University, Fresno
📧 ramuusa61@gmail.com
