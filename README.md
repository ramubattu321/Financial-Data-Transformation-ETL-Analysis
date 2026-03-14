# Financial Data Analysis using Tally ERP XML, Python and Excel

## Overview

This project demonstrates how financial data exported from **Tally ERP 9** can be stored, organized, and documented using structured formats such as XML and Excel. The repository contains monthly financial records including credit amounts and cumulative closing balances.

The goal of this project is to show how accounting data can be maintained in a structured format and managed using **GitHub version control**.

---

## Repository Structure

SS-Project
│
├── financial_data.xml     # Financial data exported from Tally ERP 9
├── financial_data.xlsx    # Excel spreadsheet containing the financial records
└── README.md              # Project documentation

---

## Data Source

The dataset in this repository was exported from **Tally ERP 9**, a widely used accounting and financial management software for managing business transactions, ledgers, and reports.

The XML export contains monthly financial information including credits and closing balances.

---

## Technologies Used

* Tally ERP 9
* XML (Extensible Markup Language)
* Microsoft Excel
* Git
* GitHub

---

## XML Data Structure

The XML file stores financial records by month. Each record contains information about credit transactions and the closing balance.

| Field     | Description                   |
| --------- | ----------------------------- |
| DSPPERIOD | Month of the financial record |
| DSPCRAMTA | Credit amount for the period  |
| DSPCLAMTA | Closing balance               |
| DSPDRAMTA | Debit amount                  |

---

## Example XML Record

```xml
<DSPPERIOD>April</DSPPERIOD>
<DSPACCINFO>
  <DSPDRAMT>
    <DSPDRAMTA></DSPDRAMTA>
  </DSPDRAMT>
  <DSPCRAMT>
    <DSPCRAMTA>29731538.00</DSPCRAMTA>
  </DSPCRAMT>
  <DSPCLAMT>
    <DSPCLAMTA>29731538.00</DSPCLAMTA>
  </DSPCLAMT>
</DSPACCINFO>
```

---

## Example Data

| Month | Credit Amount | Closing Balance |
| ----- | ------------- | --------------- |
| April | 29,731,538.00 | 29,731,538.00   |
| May   | 16,531,511.00 | 46,263,049.00   |
| June  | 5,520,157.00  | 51,783,206.00   |

---

## Purpose of the Project

This project demonstrates:

* Exporting financial data from **Tally ERP 9**
* Working with **structured XML financial datasets**
* Maintaining datasets using **GitHub version control**
* Documenting accounting data in a clear and organized format

---
## Python Script

This repository includes a Python script to convert the Tally ERP 9 XML export into an Excel file.

### Run the script

```bash
pip install pandas openpyxl
python convert_xml_to_excel.py

## How to Use

Clone the repository:

```
git clone https://github.com/ramubattu321/SS-Project.git
```

Open the XML file to view the raw financial data or open the Excel file for a tabular representation.

---

## Author

Ramu Battu
California State University, Fresno
