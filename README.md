# Financial Data Transformation & ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-SQLite-003B57?style=flat&logo=sqlite&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Processing-150458?style=flat&logo=pandas&logoColor=white)
![ETL](https://img.shields.io/badge/ETL-Tally%20ERP%20XML-orange?style=flat)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen?style=flat)

---

## Overview

A Python-based ETL pipeline that extracts financial data from Tally ERP (XML format), transforms and validates it into structured tabular datasets, loads it into a relational SQL database, and performs 16 production financial analysis queries covering P&L, expense tracking, anomaly detection, and audit reporting.

---

## ETL Pipeline Architecture

```
Source: Tally ERP → Financial Data.xml
            ↓
    EXTRACT (xml.etree.ElementTree)
    - Parse DSPPERIOD  → Month labels
    - Parse DSPACCINFO → Financial entries
    - Extract: Debit (DSPDRAMTA), Credit (DSPCRAMTA), Closing Balance (DSPCLAMTA)
            ↓
    TRANSFORM (Python + Pandas)
    - safe_float() → handle nulls, empty strings, invalid values
    - Standardize date formats and currency fields
    - Parse nested XML into flat tabular structure
    - Compute totals row (SUM of debit, credit, closing balance)
    - Validate: cross-check debits = credits + balance reconciliation
            ↓
    LOAD (SQLite + Excel)
    - Load to SQL database → 3 tables (accounts, transactions, monthly_summary)
    - Export to Excel for stakeholder reporting (Data.xlsx)
            ↓
    ANALYZE (16 SQL Queries)
    - P&L, expense analysis, anomaly detection, balance sheet, tax
```

---

## Dataset — 2023 Annual Financial Records

| Table | Rows | Description |
|-------|------|-------------|
| accounts | 19 | Chart of accounts — Assets, Liabilities, Equity, Income, Expenses |
| transactions | 144 | Monthly journal entries extracted from Tally ERP XML |
| monthly_summary | 12 | Pre-aggregated monthly P&L summary (Jan–Dec 2023) |

**Chart of Accounts:**

| Code | Account | Type | Category |
|------|---------|------|---------|
| 4001 | Sales Revenue | Income | Revenue |
| 4002 | Service Revenue | Income | Revenue |
| 5001 | Cost of Goods Sold | Expense | Direct Cost |
| 5002 | Salaries & Wages | Expense | Operating Expense |
| 5003 | Rent & Utilities | Expense | Operating Expense |
| 5004 | Marketing & Advertising | Expense | Operating Expense |
| 1002 | Accounts Receivable | Asset | Current Asset |
| 2001 | Accounts Payable | Liability | Current Liability |
| 2003 | Tax Payable | Liability | Current Liability |

---

## Project Structure

```
Financial-Data-Transformation-ETL-Analysis/
│
├── Data.py                      # ETL pipeline — XML extract + transform + Excel load
├── Financial Data.xml           # Source: Tally ERP XML financial data
├── Data.xlsx                    # Output: structured financial dataset
├── Sample Financial Data.xlsx   # Sample output for reference
│
├── sql/
│   ├── schema.sql               # Database schema — 3 tables + indexes
│   ├── sample_data.sql          # 144 transactions + 12 monthly summaries
│   ├── financial_queries.sql    # 16 production SQL financial analysis queries
│   └── setup_and_run.py         # Creates SQLite DB + runs all queries
│
├── images/                      # Visualizations and output screenshots
└── README.md
```

---

## ETL Code — Key Functions

```python
import xml.etree.ElementTree as ET
import pandas as pd

def safe_float(value):
    """Handle null, empty, or invalid values — return 0.0 instead of crashing."""
    if value is None or str(value).strip() == "":
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0

def parse_tally_xml(filepath):
    """Extract monthly financial records from Tally ERP XML."""
    tree = ET.parse(filepath)
    root = tree.getroot()

    periods  = root.findall("DSPPERIOD")     # Month labels
    accinfos = root.findall("DSPACCINFO")    # Financial entries per month

    records = []
    for i in range(min(len(periods), len(accinfos))):
        month   = periods[i].text.strip() if periods[i].text else f"Month_{i+1}"
        credit  = safe_float(accinfos[i].find(".//DSPCRAMTA"))    # Credit amount
        closing = safe_float(accinfos[i].find(".//DSPCLAMTA"))    # Closing balance
        debit   = safe_float(accinfos[i].find(".//DSPDRAMTA"))    # Debit amount
        records.append({"Month": month, "Debit": debit,
                        "Credit": credit, "Closing Balance": closing})

    df = pd.DataFrame(records)
    # Append totals row
    df.loc[len(df)] = ["Total", df["Debit"].sum(),
                       df["Credit"].sum(), df["Closing Balance"].iloc[-1]]
    return df
```

---

## SQL Queries — 16 Production Financial Analysis Queries

### Core P&L Analysis

```sql
-- Monthly P&L summary — revenue, expenses, net profit, closing balance
SELECT month, year,
    ROUND(total_credit, 2) AS revenue,
    ROUND(total_debit, 2)  AS expenses,
    ROUND(net_profit_loss, 2) AS net_profit,
    ROUND(closing_balance, 2) AS balance,
    CASE WHEN net_profit_loss >= 0 THEN 'Profitable' ELSE 'Loss' END AS status
FROM monthly_summary ORDER BY summary_id;

-- Net profit margin % by month
SELECT month,
    ROUND(100.0 * net_profit_loss / NULLIF(total_credit, 0), 2) AS profit_margin_pct,
    ROUND(100.0 * total_debit / NULLIF(total_credit, 0), 2)     AS expense_ratio_pct
FROM monthly_summary ORDER BY summary_id;
```

### Window Functions

```sql
-- Cumulative profit over the year (SUM OVER window)
SELECT month,
    ROUND(net_profit_loss, 2) AS monthly_net,
    ROUND(SUM(net_profit_loss) OVER (ORDER BY summary_id), 2) AS cumulative_profit
FROM monthly_summary;

-- Month-over-month revenue growth (LAG window)
SELECT month, ROUND(total_credit, 2) AS revenue,
    ROUND(100.0 * (total_credit - LAG(total_credit) OVER (ORDER BY summary_id))
          / NULLIF(LAG(total_credit) OVER (ORDER BY summary_id), 0), 1) AS mom_growth_pct
FROM monthly_summary;

-- Best months ranked by profitability (RANK + NTILE)
SELECT month, ROUND(net_profit_loss, 2) AS net_profit,
    RANK() OVER (ORDER BY net_profit_loss DESC) AS profit_rank,
    CASE NTILE(3) OVER (ORDER BY net_profit_loss DESC)
        WHEN 1 THEN 'High' WHEN 2 THEN 'Average' WHEN 3 THEN 'Low'
    END AS performance_tier
FROM monthly_summary ORDER BY profit_rank;
```

### CTE & Advanced Analysis

```sql
-- Anomaly detection — transactions exceeding 1.5x account average
WITH account_stats AS (
    SELECT account_name, AVG(CASE WHEN debit > 0 THEN debit END) AS avg_debit
    FROM transactions GROUP BY account_name
)
SELECT t.txn_date, t.month, t.account_name,
    ROUND(t.debit, 2) AS debit, ROUND(s.avg_debit, 2) AS avg_debit,
    'HIGH DEBIT ANOMALY' AS flag
FROM transactions t
JOIN account_stats s ON t.account_name = s.account_name
WHERE t.debit > s.avg_debit * 1.5;

-- Annual financial health scorecard (nested CTE)
WITH annual AS (
    SELECT SUM(total_credit) AS revenue, SUM(total_debit) AS expenses,
           SUM(net_profit_loss) AS profit, MAX(closing_balance) AS year_end_balance
    FROM monthly_summary
)
SELECT ROUND(revenue,2) AS revenue, ROUND(profit,2) AS profit,
    ROUND(100.0*profit/revenue,2) AS net_margin_pct,
    ROUND(year_end_balance,2) AS year_end_balance,
    CASE WHEN profit > 0 THEN 'PROFITABLE YEAR' ELSE 'LOSS YEAR' END AS status
FROM annual;
```

### All 16 Queries Summary

| # | Query | SQL Technique |
|---|-------|--------------|
| 1 | Monthly P&L summary | GROUP BY, CASE WHEN |
| 2 | Profit margin % by month | Division, ROUND |
| 3 | Revenue vs expense by account type | JOIN + GROUP BY |
| 4 | Top expense accounts | SUM OVER window |
| 5 | Revenue source breakdown | SUM OVER window |
| 6 | Cumulative profit over year | SUM OVER window |
| 7 | Month-over-month growth | LAG window function |
| 8 | Best months ranked | RANK + NTILE window |
| 9 | Voucher type analysis | SUM OVER window |
| 10 | Quarterly performance | CASE WHEN + GROUP BY |
| 11 | Monthly expense pivot | CASE WHEN pivot |
| 12 | AR vs AP working capital | CASE WHEN pivot |
| 13 | Anomaly detection | CTE + threshold comparison |
| 14 | Tax provision analysis | CTE + effective rate |
| 15 | Balance sheet snapshot | JOIN + aggregation |
| 16 | Annual financial scorecard | Nested CTE + ratios |

---

## Key Results (2023)

| Metric | Value |
|--------|-------|
| Annual Revenue | $4,972,810 |
| Annual Expenses | $3,911,161 |
| Annual Net Profit | $1,060,648 |
| Net Profit Margin | ~21.3% |
| Best Month | August ($126,601 profit) |
| Worst Month | February ($57,067 profit) |
| Year-End Balance | $1,560,650 |
| All 12 Months | ✅ Profitable |

---

## How to Run

```bash
# 1. Clone the repository
git clone https://github.com/ramubattu321/Financial-Data-Transformation-ETL-Analysis.git
cd Financial-Data-Transformation-ETL-Analysis

# 2. Install dependencies
pip install pandas openpyxl

# 3. Run ETL pipeline — XML → Excel
python Data.py

# 4. Create SQLite DB and run all 16 SQL queries
python sql/setup_and_run.py

# 5. Run SQL directly (SQLite CLI)
sqlite3 sql/financial.db < sql/financial_queries.sql
```

---

## Business Impact

- **Audit readiness** — structured, validated financial records replace raw XML
- **Anomaly detection** — SQL queries flag transactions exceeding normal thresholds
- **P&L visibility** — monthly profit margin, expense ratios, and trend analysis
- **Tax compliance** — effective tax rate tracked monthly against revenue
- **Balance sheet** — assets, liabilities, and equity summarized for year-end reporting
- **Decision support** — quarterly and annual scorecards for management reporting

---

## Tools & Technologies

| Tool | Purpose |
|------|---------|
| Python | ETL pipeline — XML parsing, transformation, validation |
| xml.etree.ElementTree | Tally ERP XML extraction |
| Pandas | Data cleaning, structuring, validation |
| openpyxl | Excel output generation |
| SQL (SQLite) | 16 financial analysis queries |
| Microsoft Excel | Stakeholder reporting output |

---

## Author

**Ramu Battu**
MS in Data Analytics — California State University, Fresno
📧 ramuusa61@gmail.com
