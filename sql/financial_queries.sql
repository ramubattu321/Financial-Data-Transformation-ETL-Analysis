-- ============================================================
-- Financial Data ETL Pipeline — Analysis Queries (16 Queries)
-- Post-ETL financial analysis on structured Tally ERP data
-- Run in: SQLite / MySQL / PostgreSQL
-- Author: Ramu Battu — MS Data Analytics, CSU Fresno
-- ============================================================


-- ── QUERY 1: MONTHLY P&L SUMMARY ─────────────────────────────────────────────
-- Full income statement view — revenue, costs, net profit per month
SELECT
    month,
    year,
    ROUND(total_credit, 2)                      AS total_revenue,
    ROUND(total_debit, 2)                        AS total_expenses,
    ROUND(net_profit_loss, 2)                    AS net_profit,
    ROUND(closing_balance, 2)                    AS closing_balance,
    CASE WHEN net_profit_loss >= 0
         THEN 'Profitable' ELSE 'Loss' END       AS month_status
FROM monthly_summary
ORDER BY summary_id;


-- ── QUERY 2: PROFIT MARGIN BY MONTH ──────────────────────────────────────────
-- Net profit margin % each month
SELECT
    month,
    ROUND(total_credit, 2)                                        AS revenue,
    ROUND(net_profit_loss, 2)                                     AS net_profit,
    ROUND(100.0 * net_profit_loss / NULLIF(total_credit, 0), 2)  AS profit_margin_pct,
    ROUND(100.0 * total_debit / NULLIF(total_credit, 0), 2)      AS expense_ratio_pct
FROM monthly_summary
ORDER BY summary_id;


-- ── QUERY 3: REVENUE VS EXPENSE BREAKDOWN ────────────────────────────────────
-- Compare total income vs total expenditure by account type
SELECT
    a.account_type,
    a.category,
    COUNT(DISTINCT t.account_code)              AS accounts,
    ROUND(SUM(t.debit), 2)                      AS total_debit,
    ROUND(SUM(t.credit), 2)                     AS total_credit,
    ROUND(SUM(t.credit) - SUM(t.debit), 2)      AS net_amount
FROM transactions t
JOIN accounts a ON t.account_code = a.account_code
GROUP BY a.account_type, a.category
ORDER BY a.account_type, net_amount DESC;


-- ── QUERY 4: TOP EXPENSE ACCOUNTS ────────────────────────────────────────────
-- Which expense accounts consumed the most budget?
SELECT
    t.account_name,
    a.category,
    ROUND(SUM(t.debit), 2)                                              AS total_expense,
    ROUND(100.0 * SUM(t.debit)
          / SUM(SUM(t.debit)) OVER (), 2)                               AS pct_of_total_expense,
    ROUND(AVG(t.debit), 2)                                              AS avg_monthly_expense
FROM transactions t
JOIN accounts a ON t.account_code = a.account_code
WHERE a.account_type = 'Expense'
  AND t.debit > 0
GROUP BY t.account_name, a.category
ORDER BY total_expense DESC;


-- ── QUERY 5: REVENUE SOURCES BREAKDOWN ───────────────────────────────────────
-- Total revenue contribution by income account
SELECT
    t.account_name,
    ROUND(SUM(t.credit), 2)                                             AS total_revenue,
    ROUND(100.0 * SUM(t.credit)
          / SUM(SUM(t.credit)) OVER (), 2)                              AS revenue_share_pct,
    ROUND(AVG(t.credit), 2)                                             AS avg_monthly_revenue,
    MIN(t.month) || ' → ' || MAX(t.month)                              AS active_period
FROM transactions t
JOIN accounts a ON t.account_code = a.account_code
WHERE a.account_type = 'Income'
  AND t.credit > 0
GROUP BY t.account_name
ORDER BY total_revenue DESC;


-- ── QUERY 6: MONTHLY CUMULATIVE BALANCE (WINDOW FUNCTION) ────────────────────
-- Running balance across all months
SELECT
    month,
    year,
    ROUND(total_credit, 2)                                              AS monthly_revenue,
    ROUND(total_debit, 2)                                               AS monthly_expense,
    ROUND(net_profit_loss, 2)                                           AS monthly_net,
    ROUND(SUM(net_profit_loss) OVER (ORDER BY summary_id), 2)          AS cumulative_profit,
    ROUND(closing_balance, 2)                                           AS closing_balance
FROM monthly_summary
ORDER BY summary_id;


-- ── QUERY 7: MONTH-OVER-MONTH GROWTH (LAG WINDOW FUNCTION) ───────────────────
-- Revenue and profit growth rate month over month
SELECT
    month,
    ROUND(total_credit, 2)                                              AS revenue,
    LAG(total_credit) OVER (ORDER BY summary_id)                       AS prev_month_revenue,
    ROUND(total_credit - LAG(total_credit) OVER (ORDER BY summary_id), 2) AS revenue_change,
    ROUND(100.0 * (total_credit - LAG(total_credit) OVER (ORDER BY summary_id))
          / NULLIF(LAG(total_credit) OVER (ORDER BY summary_id), 0), 1) AS revenue_growth_pct,
    ROUND(net_profit_loss, 2)                                           AS net_profit,
    ROUND(100.0 * (net_profit_loss - LAG(net_profit_loss) OVER (ORDER BY summary_id))
          / NULLIF(ABS(LAG(net_profit_loss) OVER (ORDER BY summary_id)), 0), 1) AS profit_growth_pct
FROM monthly_summary
ORDER BY summary_id;


-- ── QUERY 8: BEST AND WORST MONTHS (RANK WINDOW FUNCTION) ────────────────────
-- Rank months by profitability
SELECT
    month,
    ROUND(net_profit_loss, 2)                                           AS net_profit,
    ROUND(total_credit, 2)                                              AS revenue,
    RANK() OVER (ORDER BY net_profit_loss DESC)                        AS profit_rank,
    RANK() OVER (ORDER BY total_credit DESC)                           AS revenue_rank,
    NTILE(3) OVER (ORDER BY net_profit_loss DESC)                      AS performance_tier,
    CASE NTILE(3) OVER (ORDER BY net_profit_loss DESC)
        WHEN 1 THEN 'High Performance'
        WHEN 2 THEN 'Average'
        WHEN 3 THEN 'Needs Improvement'
    END                                                                 AS performance_label
FROM monthly_summary
ORDER BY profit_rank;


-- ── QUERY 9: VOUCHER TYPE ANALYSIS ───────────────────────────────────────────
-- Transaction volume and value by voucher type
SELECT
    voucher_type,
    COUNT(*)                                    AS transaction_count,
    ROUND(SUM(debit), 2)                        AS total_debit,
    ROUND(SUM(credit), 2)                       AS total_credit,
    ROUND(AVG(CASE WHEN debit > 0 THEN debit
                   WHEN credit > 0 THEN credit END), 2) AS avg_txn_value,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS pct_of_transactions
FROM transactions
GROUP BY voucher_type
ORDER BY transaction_count DESC;


-- ── QUERY 10: QUARTERLY FINANCIAL PERFORMANCE ─────────────────────────────────
-- Aggregate P&L by quarter
SELECT
    CASE
        WHEN month IN ('January','February','March')    THEN 'Q1'
        WHEN month IN ('April','May','June')            THEN 'Q2'
        WHEN month IN ('July','August','September')     THEN 'Q3'
        WHEN month IN ('October','November','December') THEN 'Q4'
    END                                                 AS quarter,
    ROUND(SUM(total_credit), 2)                         AS quarterly_revenue,
    ROUND(SUM(total_debit), 2)                          AS quarterly_expenses,
    ROUND(SUM(net_profit_loss), 2)                      AS quarterly_profit,
    ROUND(100.0 * SUM(net_profit_loss)
          / NULLIF(SUM(total_credit), 0), 2)            AS profit_margin_pct,
    COUNT(*)                                            AS months_included
FROM monthly_summary
GROUP BY quarter
ORDER BY quarter;


-- ── QUERY 11: EXPENSE TREND BY ACCOUNT (MONTHLY) ─────────────────────────────
-- Track key expense accounts month by month using CASE WHEN pivot
SELECT
    month,
    ROUND(SUM(CASE WHEN account_name='Salaries & Wages'       THEN debit ELSE 0 END), 2) AS salaries,
    ROUND(SUM(CASE WHEN account_name='Cost of Goods Sold'     THEN debit ELSE 0 END), 2) AS cogs,
    ROUND(SUM(CASE WHEN account_name='Marketing & Advertising' THEN debit ELSE 0 END), 2) AS marketing,
    ROUND(SUM(CASE WHEN account_name='Rent & Utilities'       THEN debit ELSE 0 END), 2) AS rent,
    ROUND(SUM(CASE WHEN account_name='Interest Expense'       THEN debit ELSE 0 END), 2) AS interest,
    ROUND(SUM(CASE WHEN account_name='Depreciation'           THEN debit ELSE 0 END), 2) AS depreciation
FROM transactions
WHERE txn_type = 'Debit'
GROUP BY month
ORDER BY MIN(txn_date);


-- ── QUERY 12: ACCOUNTS RECEIVABLE & PAYABLE TREND ────────────────────────────
-- Track AR and AP balance by month for cash flow insight
SELECT
    month,
    ROUND(SUM(CASE WHEN account_name='Accounts Receivable' THEN debit ELSE 0 END), 2)  AS ar_balance,
    ROUND(SUM(CASE WHEN account_name='Accounts Payable'    THEN credit ELSE 0 END), 2) AS ap_balance,
    ROUND(SUM(CASE WHEN account_name='Accounts Receivable' THEN debit ELSE 0 END)
        - SUM(CASE WHEN account_name='Accounts Payable' THEN credit ELSE 0 END), 2)    AS net_working_capital
FROM transactions
GROUP BY month
ORDER BY MIN(txn_date);


-- ── QUERY 13: ANOMALY DETECTION — UNUSUAL TRANSACTIONS ───────────────────────
-- Flag transactions that deviate more than 2x the account average
WITH account_stats AS (
    SELECT account_name, txn_type,
           AVG(CASE WHEN debit  > 0 THEN debit  END) AS avg_debit,
           AVG(CASE WHEN credit > 0 THEN credit END) AS avg_credit
    FROM transactions
    GROUP BY account_name, txn_type
)
SELECT
    t.txn_date, t.month, t.account_name, t.txn_type,
    ROUND(t.debit, 2)  AS debit,
    ROUND(t.credit, 2) AS credit,
    ROUND(s.avg_debit,  2) AS avg_debit,
    ROUND(s.avg_credit, 2) AS avg_credit,
    CASE
        WHEN t.debit  > s.avg_debit  * 2 THEN 'HIGH DEBIT ANOMALY'
        WHEN t.credit > s.avg_credit * 2 THEN 'HIGH CREDIT ANOMALY'
        ELSE 'Normal'
    END AS anomaly_flag
FROM transactions t
JOIN account_stats s
    ON t.account_name = s.account_name AND t.txn_type = s.txn_type
WHERE t.debit > s.avg_debit * 2
   OR t.credit > s.avg_credit * 2
ORDER BY t.txn_date;


-- ── QUERY 14: TAX PROVISION ANALYSIS ─────────────────────────────────────────
-- Track tax provision vs revenue each month
WITH tax AS (
    SELECT month,
           SUM(CASE WHEN account_name='Tax Payable' THEN credit ELSE 0 END) AS tax_provision,
           SUM(CASE WHEN account_name IN ('Sales Revenue','Service Revenue') THEN credit ELSE 0 END) AS revenue
    FROM transactions
    GROUP BY month
)
SELECT
    month,
    ROUND(revenue, 2)         AS total_revenue,
    ROUND(tax_provision, 2)   AS tax_provision,
    ROUND(100.0 * tax_provision / NULLIF(revenue, 0), 2) AS effective_tax_rate_pct,
    ROUND(revenue - tax_provision, 2) AS net_after_tax
FROM tax
ORDER BY MIN(ROWID);


-- ── QUERY 15: BALANCE SHEET SNAPSHOT ─────────────────────────────────────────
-- Assets vs Liabilities vs Equity summary (year-end)
SELECT
    a.account_type,
    a.category,
    t.account_name,
    ROUND(SUM(t.debit) - SUM(t.credit), 2)     AS net_balance,
    CASE WHEN SUM(t.debit) > SUM(t.credit) THEN 'Debit Balance'
         ELSE 'Credit Balance' END              AS balance_nature
FROM transactions t
JOIN accounts a ON t.account_code = a.account_code
WHERE a.account_type IN ('Asset','Liability','Equity')
GROUP BY a.account_type, a.category, t.account_name
ORDER BY a.account_type,
         CASE a.account_type WHEN 'Asset' THEN 1
                              WHEN 'Liability' THEN 2
                              ELSE 3 END;


-- ── QUERY 16: FINANCIAL HEALTH SCORECARD ─────────────────────────────────────
-- Year-end summary with key financial ratios using CTEs
WITH annual AS (
    SELECT
        SUM(total_credit)       AS total_revenue,
        SUM(total_debit)        AS total_expenses,
        SUM(net_profit_loss)    AS annual_profit,
        MAX(closing_balance)    AS year_end_balance,
        MIN(net_profit_loss)    AS worst_month_profit,
        MAX(net_profit_loss)    AS best_month_profit,
        AVG(net_profit_loss)    AS avg_monthly_profit
    FROM monthly_summary
),
asset_total AS (
    SELECT ROUND(SUM(debit) - SUM(credit), 2) AS total_assets
    FROM transactions t
    JOIN accounts a ON t.account_code = a.account_code
    WHERE a.account_type = 'Asset'
),
liability_total AS (
    SELECT ROUND(SUM(credit) - SUM(debit), 2) AS total_liabilities
    FROM transactions t
    JOIN accounts a ON t.account_code = a.account_code
    WHERE a.account_type = 'Liability'
)
SELECT
    ROUND(a.total_revenue, 2)                                               AS annual_revenue,
    ROUND(a.total_expenses, 2)                                              AS annual_expenses,
    ROUND(a.annual_profit, 2)                                               AS annual_profit,
    ROUND(100.0 * a.annual_profit / NULLIF(a.total_revenue, 0), 2)         AS net_profit_margin_pct,
    ROUND(a.avg_monthly_profit, 2)                                          AS avg_monthly_profit,
    ROUND(a.best_month_profit, 2)                                           AS best_month_profit,
    ROUND(a.worst_month_profit, 2)                                          AS worst_month_profit,
    ROUND(a.year_end_balance, 2)                                            AS year_end_balance,
    ROUND(ast.total_assets, 2)                                              AS total_assets,
    ROUND(lib.total_liabilities, 2)                                         AS total_liabilities,
    ROUND(ast.total_assets / NULLIF(lib.total_liabilities, 0), 2)          AS debt_to_asset_ratio,
    CASE WHEN a.annual_profit > 0 THEN 'PROFITABLE YEAR'
         ELSE 'LOSS YEAR' END                                               AS year_status
FROM annual a, asset_total ast, liability_total lib;
