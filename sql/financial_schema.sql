-- ============================================================
-- Financial Data ETL Pipeline — Database Schema
-- Tally ERP XML → SQLite for analysis and reporting
-- Compatible with: SQLite, MySQL, PostgreSQL
-- Author: Ramu Battu — MS Data Analytics, CSU Fresno
-- ============================================================

DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS monthly_summary;
DROP TABLE IF EXISTS accounts;

-- ── TABLE 1: CHART OF ACCOUNTS ────────────────────────────────────────────────
CREATE TABLE accounts (
    account_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    account_code TEXT NOT NULL UNIQUE,    -- e.g. 4001
    account_name TEXT NOT NULL,           -- e.g. Sales Revenue
    account_type TEXT NOT NULL,           -- Asset / Liability / Equity / Income / Expense
    category     TEXT NOT NULL            -- Current Asset / Revenue / Operating Expense etc.
);

-- ── TABLE 2: TRANSACTION LEDGER ───────────────────────────────────────────────
-- One row per journal entry extracted from Tally ERP XML
CREATE TABLE transactions (
    txn_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    txn_date     DATE    NOT NULL,        -- Transaction date
    month        TEXT    NOT NULL,        -- Month name (January–December)
    year         INTEGER NOT NULL,        -- Fiscal year
    account_code TEXT    NOT NULL,        -- Account reference
    account_name TEXT    NOT NULL,        -- Account description
    txn_type     TEXT    NOT NULL,        -- Debit / Credit
    debit        REAL    DEFAULT 0,       -- Debit amount (0 if credit entry)
    credit       REAL    DEFAULT 0,       -- Credit amount (0 if debit entry)
    description  TEXT,                    -- Transaction description
    voucher_type TEXT    NOT NULL,        -- Sales / Purchase / Payment / Receipt / Journal
    FOREIGN KEY (account_code) REFERENCES accounts(account_code)
);

-- ── TABLE 3: MONTHLY SUMMARY ──────────────────────────────────────────────────
-- Pre-aggregated monthly P&L (matches Tally ERP monthly report output)
CREATE TABLE monthly_summary (
    summary_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    month           TEXT    NOT NULL,     -- Month name
    year            INTEGER NOT NULL,     -- Year
    total_debit     REAL    NOT NULL,     -- Total debits for the month
    total_credit    REAL    NOT NULL,     -- Total credits for the month
    closing_balance REAL    NOT NULL,     -- Cumulative closing balance
    net_profit_loss REAL    NOT NULL      -- Net (credit - debit) for the month
);

-- ── INDEXES ───────────────────────────────────────────────────────────────────
CREATE INDEX idx_txn_date     ON transactions(txn_date);
CREATE INDEX idx_txn_month    ON transactions(month, year);
CREATE INDEX idx_account_code ON transactions(account_code);
CREATE INDEX idx_account_type ON accounts(account_type);
CREATE INDEX idx_voucher_type ON transactions(voucher_type);
CREATE INDEX idx_txn_type     ON transactions(txn_type);
