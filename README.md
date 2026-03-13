# Financial Data Processing from Tally ERP 9

## Overview
This repository contains financial data exported from Tally ERP 9 in XML format and maintained in Excel for analysis and reporting. The project demonstrates handling structured accounting data, organizing files in a version-controlled repository, and documenting financial records in a professional format.

## Repository Structure

.
├── Financial Data.xml   # XML export from Tally ERP 9
├── ss.xlsx              # Excel version of the financial data
└── README.md            # Project documentation

## Data Source
The XML file in this repository was generated from **Tally ERP 9**, an accounting and financial management software widely used for bookkeeping, reporting, and ledger management.

## Files

### 1. Financial Data.xml
Contains monthly financial information exported from Tally ERP 9, including:
- Reporting period
- Credit amount
- Closing balance
- Debit amount

### 2. ss.xlsx
Contains the same or related financial data in spreadsheet format for easier review and analysis.

## XML Fields

| Field      | Description |
|------------|-------------|
| DSPPERIOD  | Month or reporting period |
| DSPCRAMTA  | Credit amount |
| DSPCLAMTA  | Closing balance |
| DSPDRAMTA  | Debit amount |

## Example Record

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
