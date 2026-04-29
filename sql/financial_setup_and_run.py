"""
Financial Data ETL Pipeline — SQL Database Setup & Query Runner
===============================================================
Creates SQLite database from ETL output and runs 16 financial analysis queries.

Run: python sql/setup_and_run.py
"""

import sqlite3, pandas as pd, random, os
from datetime import date, timedelta

DB_PATH = "sql/financial.db"
random.seed(42)


def create_schema(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS accounts (
        account_id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_code TEXT NOT NULL UNIQUE, account_name TEXT NOT NULL,
        account_type TEXT NOT NULL, category TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS transactions (
        txn_id INTEGER PRIMARY KEY AUTOINCREMENT, txn_date DATE NOT NULL,
        month TEXT NOT NULL, year INTEGER NOT NULL, account_code TEXT NOT NULL,
        account_name TEXT NOT NULL, txn_type TEXT NOT NULL,
        debit REAL DEFAULT 0, credit REAL DEFAULT 0,
        description TEXT, voucher_type TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS monthly_summary (
        summary_id INTEGER PRIMARY KEY AUTOINCREMENT, month TEXT NOT NULL,
        year INTEGER NOT NULL, total_debit REAL NOT NULL, total_credit REAL NOT NULL,
        closing_balance REAL NOT NULL, net_profit_loss REAL NOT NULL
    );
    """)


def load_data(conn):
    cur = conn.cursor()
    accounts = [
        ("1001","Cash & Bank","Asset","Current Asset"),
        ("1002","Accounts Receivable","Asset","Current Asset"),
        ("1003","Inventory","Asset","Current Asset"),
        ("1004","Fixed Assets","Asset","Non-Current Asset"),
        ("2001","Accounts Payable","Liability","Current Liability"),
        ("2002","Bank Loan","Liability","Non-Current Liability"),
        ("2003","Tax Payable","Liability","Current Liability"),
        ("3001","Owner Equity","Equity","Capital"),
        ("3002","Retained Earnings","Equity","Reserves"),
        ("4001","Sales Revenue","Income","Revenue"),
        ("4002","Service Revenue","Income","Revenue"),
        ("4003","Other Income","Income","Revenue"),
        ("5001","Cost of Goods Sold","Expense","Direct Cost"),
        ("5002","Salaries & Wages","Expense","Operating Expense"),
        ("5003","Rent & Utilities","Expense","Operating Expense"),
        ("5004","Marketing & Advertising","Expense","Operating Expense"),
        ("5005","Depreciation","Expense","Non-Cash Expense"),
        ("5006","Interest Expense","Expense","Financial Cost"),
        ("5007","Miscellaneous Expense","Expense","Operating Expense"),
    ]
    cur.executemany("INSERT INTO accounts (account_code,account_name,account_type,category) VALUES (?,?,?,?)", accounts)

    months = ["January","February","March","April","May","June",
              "July","August","September","October","November","December"]
    txn_rows, month_rows = [], []
    balance = 500000.0

    for i, month in enumerate(months):
        base = date(2023, i+1, 1)
        sales  = random.uniform(180000, 320000)
        svc    = random.uniform(40000,  80000)
        cogs   = sales * random.uniform(0.55, 0.65)
        sal    = random.uniform(45000, 65000)
        rent   = random.uniform(12000, 18000)
        mktg   = random.uniform(8000,  20000)
        depr   = random.uniform(5000,   8000)
        intr   = random.uniform(3000,   6000)
        misc   = random.uniform(2000,   6000)
        tax    = (sales+svc)*0.15
        ar     = sales*0.3
        ap     = cogs*0.4

        entries = [
            (base+timedelta(2),"4001","Sales Revenue","Credit",0,sales,"Monthly sales revenue","Sales"),
            (base+timedelta(3),"4002","Service Revenue","Credit",0,svc,"Service charges","Receipt"),
            (base+timedelta(5),"5001","Cost of Goods Sold","Debit",cogs,0,"COGS for month","Purchase"),
            (base+timedelta(7),"5002","Salaries & Wages","Debit",sal,0,"Monthly payroll","Payment"),
            (base+timedelta(10),"5003","Rent & Utilities","Debit",rent,0,"Rent + utilities","Payment"),
            (base+timedelta(12),"5004","Marketing & Advertising","Debit",mktg,0,"Ad spend","Payment"),
            (base+timedelta(15),"5005","Depreciation","Debit",depr,0,"Asset depreciation","Journal"),
            (base+timedelta(18),"5006","Interest Expense","Debit",intr,0,"Bank loan interest","Journal"),
            (base+timedelta(20),"5007","Miscellaneous Expense","Debit",misc,0,"Misc costs","Payment"),
            (base+timedelta(22),"1002","Accounts Receivable","Debit",ar,0,"AR from credit sales","Journal"),
            (base+timedelta(25),"2001","Accounts Payable","Credit",0,ap,"AP for inventory","Journal"),
            (base+timedelta(28),"2003","Tax Payable","Credit",0,tax,"Tax provision","Journal"),
        ]

        for e in entries:
            acct = next(a for a in accounts if a[0]==e[1])
            txn_rows.append((e[0].isoformat(),month,2023,e[1],acct[1],e[3],round(float(e[4]),2),round(float(e[5]),2),e[6],e[7]))

        total_debit  = round(cogs+sal+rent+mktg+depr+intr+misc+ar, 2)
        total_credit = round(sales+svc+ap+tax, 2)
        net          = round(total_credit-total_debit, 2)
        balance      = round(balance+net, 2)
        month_rows.append((month,2023,total_debit,total_credit,balance,net))

    cur.executemany("""INSERT INTO transactions
        (txn_date,month,year,account_code,account_name,txn_type,debit,credit,description,voucher_type)
        VALUES (?,?,?,?,?,?,?,?,?,?)""", txn_rows)
    cur.executemany("""INSERT INTO monthly_summary
        (month,year,total_debit,total_credit,closing_balance,net_profit_loss) VALUES (?,?,?,?,?,?)""", month_rows)
    conn.commit()


def run(conn, sql, title):
    print(f"\n{'='*65}\n  {title}\n{'='*65}")
    df = pd.read_sql_query(sql, conn)
    print(df.to_string(index=False))


if __name__ == "__main__":
    if os.path.exists(DB_PATH): os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    create_schema(conn)
    load_data(conn)

    for t in ["accounts","transactions","monthly_summary"]:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"✅ {t}: {n:,} rows")

    run(conn,"""
        SELECT month, ROUND(total_credit,2) AS revenue, ROUND(total_debit,2) AS expenses,
               ROUND(net_profit_loss,2) AS net_profit, ROUND(closing_balance,2) AS balance
        FROM monthly_summary ORDER BY summary_id
    ""","QUERY 1 — Monthly P&L Summary")

    run(conn,"""
        SELECT month, ROUND(100.0*net_profit_loss/NULLIF(total_credit,0),2) AS profit_margin_pct,
               ROUND(100.0*total_debit/NULLIF(total_credit,0),2) AS expense_ratio_pct
        FROM monthly_summary ORDER BY summary_id
    ""","QUERY 2 — Profit Margin % by Month")

    run(conn,"""
        SELECT a.account_type, a.category,
               ROUND(SUM(t.debit),2) AS total_debit, ROUND(SUM(t.credit),2) AS total_credit,
               ROUND(SUM(t.credit)-SUM(t.debit),2) AS net_amount
        FROM transactions t JOIN accounts a ON t.account_code=a.account_code
        GROUP BY a.account_type, a.category ORDER BY a.account_type
    ""","QUERY 3 — Revenue vs Expense by Account Type")

    run(conn,"""
        SELECT t.account_name, ROUND(SUM(t.debit),2) AS total_expense,
               ROUND(100.0*SUM(t.debit)/SUM(SUM(t.debit)) OVER(),2) AS pct_of_total
        FROM transactions t JOIN accounts a ON t.account_code=a.account_code
        WHERE a.account_type='Expense' AND t.debit>0
        GROUP BY t.account_name ORDER BY total_expense DESC
    ""","QUERY 4 — Top Expense Accounts (SUM OVER)")

    run(conn,"""
        SELECT t.account_name, ROUND(SUM(t.credit),2) AS total_revenue,
               ROUND(100.0*SUM(t.credit)/SUM(SUM(t.credit)) OVER(),2) AS revenue_share_pct
        FROM transactions t JOIN accounts a ON t.account_code=a.account_code
        WHERE a.account_type='Income' AND t.credit>0
        GROUP BY t.account_name ORDER BY total_revenue DESC
    ""","QUERY 5 — Revenue Source Breakdown (SUM OVER)")

    run(conn,"""
        SELECT month, ROUND(net_profit_loss,2) AS monthly_net,
               ROUND(SUM(net_profit_loss) OVER (ORDER BY summary_id),2) AS cumulative_profit,
               ROUND(closing_balance,2) AS closing_balance
        FROM monthly_summary ORDER BY summary_id
    ""","QUERY 6 — Cumulative Profit (SUM OVER Window)")

    run(conn,"""
        SELECT month, ROUND(total_credit,2) AS revenue,
               ROUND(100.0*(total_credit-LAG(total_credit) OVER (ORDER BY summary_id))
                     /NULLIF(LAG(total_credit) OVER (ORDER BY summary_id),0),1) AS revenue_mom_pct,
               ROUND(100.0*(net_profit_loss-LAG(net_profit_loss) OVER (ORDER BY summary_id))
                     /NULLIF(ABS(LAG(net_profit_loss) OVER (ORDER BY summary_id)),0),1) AS profit_mom_pct
        FROM monthly_summary ORDER BY summary_id
    ""","QUERY 7 — Month-over-Month Growth (LAG Window)")

    run(conn,"""
        SELECT month, ROUND(net_profit_loss,2) AS net_profit,
               RANK() OVER (ORDER BY net_profit_loss DESC) AS profit_rank,
               CASE NTILE(3) OVER (ORDER BY net_profit_loss DESC)
                   WHEN 1 THEN 'High' WHEN 2 THEN 'Average' WHEN 3 THEN 'Low' END AS tier
        FROM monthly_summary ORDER BY profit_rank
    ""","QUERY 8 — Best Months (RANK + NTILE Window)")

    run(conn,"""
        SELECT voucher_type, COUNT(*) AS txns,
               ROUND(SUM(debit),2) AS total_debit, ROUND(SUM(credit),2) AS total_credit,
               ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),2) AS pct_of_txns
        FROM transactions GROUP BY voucher_type ORDER BY txns DESC
    ""","QUERY 9 — Voucher Type Analysis")

    run(conn,"""
        SELECT CASE WHEN month IN ('January','February','March') THEN 'Q1'
                    WHEN month IN ('April','May','June')         THEN 'Q2'
                    WHEN month IN ('July','August','September')  THEN 'Q3'
                    ELSE 'Q4' END AS quarter,
               ROUND(SUM(total_credit),2) AS revenue, ROUND(SUM(total_debit),2) AS expenses,
               ROUND(SUM(net_profit_loss),2) AS profit,
               ROUND(100.0*SUM(net_profit_loss)/NULLIF(SUM(total_credit),0),2) AS margin_pct
        FROM monthly_summary GROUP BY quarter ORDER BY quarter
    ""","QUERY 10 — Quarterly Performance")

    run(conn,"""
        SELECT month,
               ROUND(SUM(CASE WHEN account_name='Salaries & Wages' THEN debit ELSE 0 END),2) AS salaries,
               ROUND(SUM(CASE WHEN account_name='Cost of Goods Sold' THEN debit ELSE 0 END),2) AS cogs,
               ROUND(SUM(CASE WHEN account_name='Marketing & Advertising' THEN debit ELSE 0 END),2) AS marketing,
               ROUND(SUM(CASE WHEN account_name='Rent & Utilities' THEN debit ELSE 0 END),2) AS rent
        FROM transactions WHERE txn_type='Debit'
        GROUP BY month ORDER BY MIN(txn_date)
    ""","QUERY 11 — Monthly Expense Pivot (CASE WHEN)")

    run(conn,"""
        SELECT month,
               ROUND(SUM(CASE WHEN account_name='Accounts Receivable' THEN debit ELSE 0 END),2) AS ar,
               ROUND(SUM(CASE WHEN account_name='Accounts Payable' THEN credit ELSE 0 END),2) AS ap,
               ROUND(SUM(CASE WHEN account_name='Accounts Receivable' THEN debit ELSE 0 END)
                   - SUM(CASE WHEN account_name='Accounts Payable' THEN credit ELSE 0 END),2) AS net_working_capital
        FROM transactions GROUP BY month ORDER BY MIN(txn_date)
    ""","QUERY 12 — AR vs AP Working Capital")

    run(conn,"""
        WITH stats AS (SELECT account_name, AVG(CASE WHEN debit>0 THEN debit END) AS avg_d FROM transactions GROUP BY account_name)
        SELECT t.txn_date, t.month, t.account_name, ROUND(t.debit,2) AS debit,
               ROUND(s.avg_d,2) AS avg_debit, 'HIGH DEBIT' AS flag
        FROM transactions t JOIN stats s ON t.account_name=s.account_name
        WHERE t.debit > s.avg_d*1.5 ORDER BY t.debit DESC LIMIT 10
    ""","QUERY 13 — Anomaly Detection (High Transactions)")

    run(conn,"""
        WITH tax AS (
            SELECT month,
                   SUM(CASE WHEN account_name='Tax Payable' THEN credit ELSE 0 END) AS tax_provision,
                   SUM(CASE WHEN account_name IN ('Sales Revenue','Service Revenue') THEN credit ELSE 0 END) AS revenue
            FROM transactions GROUP BY month
        )
        SELECT month, ROUND(revenue,2) AS revenue, ROUND(tax_provision,2) AS tax,
               ROUND(100.0*tax_provision/NULLIF(revenue,0),2) AS effective_tax_rate_pct
        FROM tax ORDER BY MIN(ROWID)
    ""","QUERY 14 — Tax Rate Analysis (CTE)")

    run(conn,"""
        SELECT a.account_type, a.category, t.account_name,
               ROUND(SUM(t.debit)-SUM(t.credit),2) AS net_balance
        FROM transactions t JOIN accounts a ON t.account_code=a.account_code
        WHERE a.account_type IN ('Asset','Liability','Equity')
        GROUP BY a.account_type, a.category, t.account_name
        ORDER BY a.account_type
    ""","QUERY 15 — Balance Sheet Snapshot")

    run(conn,"""
        WITH annual AS (
            SELECT ROUND(SUM(total_credit),2) AS revenue, ROUND(SUM(total_debit),2) AS expenses,
                   ROUND(SUM(net_profit_loss),2) AS profit, ROUND(MAX(closing_balance),2) AS yr_end_balance,
                   ROUND(100.0*SUM(net_profit_loss)/NULLIF(SUM(total_credit),0),2) AS margin_pct
            FROM monthly_summary
        )
        SELECT revenue, expenses, profit, margin_pct, yr_end_balance,
               CASE WHEN profit>0 THEN 'PROFITABLE YEAR' ELSE 'LOSS YEAR' END AS status
        FROM annual
    ""","QUERY 16 — Annual Financial Health Scorecard (CTE)")

    conn.close()
    print(f"\n{'='*65}\n✅ All 16 queries complete! DB: {DB_PATH}\n{'='*65}")
