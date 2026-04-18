import sqlite3
import os

DB_PATH = "trade_log.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS signal_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        ticker TEXT,
        tier INTEGER,
        universe_input TEXT,
        market_state TEXT,
        research_score REAL,
        score_band TEXT,
        l1 INTEGER,
        l2 INTEGER,
        l3 INTEGER,
        l4 INTEGER,
        l5 INTEGER,
        l6 INTEGER,
        l5_data_freshness TEXT,
        entry_type TEXT,
        vcp_detected INTEGER,
        entry REAL,
        stop REAL,
        t1 REAL,
        t2 REAL,
        shares INTEGER,
        position_value REAL,
        max_loss REAL,
        entry_timing TEXT,
        reasoning TEXT,
        skip_flags TEXT,
        sector TEXT,
        themes TEXT,
        status TEXT DEFAULT 'GENERATED',
        user_action_ts TEXT,
        user_action_note TEXT,
        outcome_checked INTEGER DEFAULT 0,
        outcome_result TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS live_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id INTEGER REFERENCES signal_log(id),
        entry_price_actual REAL,
        entry_date TEXT,
        shares_actual INTEGER,
        stop_price REAL,
        t1_price REAL,
        t2_price REAL,
        gtt_entry_id TEXT,
        gtt_stop_id TEXT,
        status TEXT DEFAULT 'OPEN',
        exit_price REAL,
        exit_date TEXT,
        exit_reason TEXT,
        pnl_abs REAL,
        pnl_pct REAL,
        days_held INTEGER,
        signal_quality_tag TEXT,
        exec_quality_tag TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS paper_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id INTEGER REFERENCES signal_log(id),
        entry_price_actual REAL,
        entry_date TEXT,
        shares_actual INTEGER,
        stop_price REAL,
        t1_price REAL,
        t2_price REAL,
        gtt_entry_id TEXT,
        gtt_stop_id TEXT,
        status TEXT DEFAULT 'OPEN',
        exit_price REAL,
        exit_date TEXT,
        exit_reason TEXT,
        pnl_abs REAL,
        pnl_pct REAL,
        days_held INTEGER,
        signal_quality_tag TEXT,
        exec_quality_tag TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS watchlist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id INTEGER REFERENCES signal_log(id),
        ticker TEXT,
        added_date TEXT,
        entry_zone_price REAL,
        expiry_date TEXT,
        notes TEXT,
        status TEXT DEFAULT 'ACTIVE'
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS regime_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        nifty_close REAL,
        ema200 REAL,
        market_state TEXT,
        vix_today REAL,
        vix_52wk_avg REAL,
        fii_flow REAL,
        fii_streak INTEGER,
        fii_direction TEXT,
        l1_score INTEGER,
        l2_score INTEGER,
        sector_leaders TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS system_learnings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_number INTEGER,
        date_run TEXT,
        trades_reviewed INTEGER,
        win_rate REAL,
        avg_rr REAL,
        rule_proposals_json TEXT,
        approved_changes_json TEXT,
        is_active INTEGER DEFAULT 1
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS run_registry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT UNIQUE,
        created_at_utc TEXT,
        finalized_at_utc TEXT,
        paper INTEGER DEFAULT 1,
        provider TEXT,
        timing_mode TEXT,
        analysis_input_path TEXT,
        analysis_output_path TEXT,
        data_quality_report_path TEXT,
        compliance_report_path TEXT,
        bias_report_path TEXT,
        analysis_input_sha256 TEXT,
        output_schema_ok INTEGER DEFAULT 0,
        compliance_pct REAL,
        recency_bias_avg REAL,
        status TEXT DEFAULT 'RUN_STARTED',
        notes TEXT
    )''')

    conn.commit()
    conn.close()
    print("Database initialised.")

def log_signal(signal: dict):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO signal_log (
        date, ticker, tier, universe_input, market_state, research_score, score_band,
        l1, l2, l3, l4, l5, l6, l5_data_freshness, entry_type, vcp_detected,
        entry, stop, t1, t2, shares, position_value, max_loss,
        entry_timing, reasoning, skip_flags, sector, themes, status
    ) VALUES (
        :date, :ticker, :tier, :universe_input, :market_state, :research_score, :score_band,
        :l1, :l2, :l3, :l4, :l5, :l6, :l5_data_freshness, :entry_type, :vcp_detected,
        :entry, :stop, :t1, :t2, :shares, :position_value, :max_loss,
        :entry_timing, :reasoning, :skip_flags, :sector, :themes, :status
    )''', signal)
    signal_id = c.lastrowid
    conn.commit()
    conn.close()
    return signal_id

def update_signal_status(signal_id: int, status: str, note: str = None):
    conn = get_connection()
    c = conn.cursor()
    import datetime
    c.execute('''UPDATE signal_log SET status = ?, user_action_ts = ?, user_action_note = ?
                 WHERE id = ?''',
              (status, datetime.datetime.now().isoformat(), note, signal_id))
    conn.commit()
    conn.close()

def log_trade(signal_id: int, trade: dict, paper: bool = False):
    table = "paper_trades" if paper else "live_trades"
    conn = get_connection()
    c = conn.cursor()
    c.execute(f'''INSERT INTO {table} (
        signal_id, entry_price_actual, entry_date, shares_actual,
        stop_price, t1_price, t2_price, status
    ) VALUES (
        :signal_id, :entry_price_actual, :entry_date, :shares_actual,
        :stop_price, :t1_price, :t2_price, :status
    )''', {**trade, "signal_id": signal_id})
    conn.commit()
    conn.close()

def close_trade(ticker: str, exit_price: float, exit_reason: str, paper: bool = False):
    table = "paper_trades" if paper else "live_trades"
    conn = get_connection()
    c = conn.cursor()
    import datetime
    c.execute(f'''SELECT t.id, t.entry_price_actual, t.shares_actual, t.entry_date
                  FROM {table} t
                  JOIN signal_log s ON t.signal_id = s.id
                  WHERE s.ticker = ? AND t.status = 'OPEN'
                  ORDER BY t.id DESC LIMIT 1''', (ticker,))
    row = c.fetchone()
    if not row:
        print(f"No open trade found for {ticker}")
        conn.close()
        return
    trade_id, entry_price, shares, entry_date = row
    pnl_abs = (exit_price - entry_price) * shares
    pnl_pct = ((exit_price - entry_price) / entry_price) * 100
    entry_dt = datetime.date.fromisoformat(entry_date)
    days_held = (datetime.date.today() - entry_dt).days
    c.execute(f'''UPDATE {table} SET status = 'CLOSED', exit_price = ?, exit_date = ?,
                  exit_reason = ?, pnl_abs = ?, pnl_pct = ?, days_held = ?
                  WHERE id = ?''',
              (exit_price, datetime.date.today().isoformat(), exit_reason,
               pnl_abs, pnl_pct, days_held, trade_id))
    conn.commit()
    conn.close()
    print(f"Trade closed. P&L: {'+' if pnl_abs >= 0 else ''}₹{pnl_abs:,.0f} ({pnl_pct:+.1f}%)")

def get_open_positions(paper: bool = False):
    table = "paper_trades" if paper else "live_trades"
    conn = get_connection()
    c = conn.cursor()
    c.execute(f'''SELECT s.ticker, s.sector, s.entry, s.t1, s.t2, s.market_state,
                         t.entry_price_actual, t.shares_actual, t.stop_price,
                         t.t1_price, t.t2_price, t.entry_date, t.status
                  FROM {table} t
                  JOIN signal_log s ON t.signal_id = s.id
                  WHERE t.status IN ('OPEN', 'PARTIALLY_CLOSED')''')
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_mtd_pnl(paper: bool = False):
    table = "paper_trades" if paper else "live_trades"
    conn = get_connection()
    c = conn.cursor()
    import datetime
    month_start = datetime.date.today().replace(day=1).isoformat()
    c.execute(f'''SELECT COALESCE(SUM(pnl_abs), 0) FROM {table}
                  WHERE status = 'CLOSED' AND exit_date >= ?''', (month_start,))
    result = c.fetchone()[0]
    conn.close()
    return result

def get_unchecked_abandoned_signals(older_than):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''SELECT id, ticker, entry, stop, t1, t2
                 FROM signal_log
                 WHERE status = 'ABANDONED'
                 AND outcome_checked = 0
                 AND date <= ?''', (older_than.isoformat(),))
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def update_signal_outcome(signal_id: int, outcome: str):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''UPDATE signal_log SET outcome_checked = 1, outcome_result = ?
                 WHERE id = ?''', (outcome, signal_id))
    conn.commit()
    conn.close()

def add_to_watchlist(signal_id: int, ticker: str, entry_zone: float, expiry_date: str):
    conn = get_connection()
    c = conn.cursor()
    import datetime
    c.execute('''INSERT INTO watchlist (signal_id, ticker, added_date, entry_zone_price, expiry_date, status)
                 VALUES (?, ?, ?, ?, ?, 'ACTIVE')''',
              (signal_id, ticker, datetime.date.today().isoformat(), entry_zone, expiry_date))
    conn.commit()
    conn.close()

def get_active_watchlist():
    conn = get_connection()
    c = conn.cursor()
    c.execute('''SELECT w.*, s.entry, s.stop, s.t1, s.t2, s.shares, s.max_loss,
                        s.entry_timing, s.sector, s.market_state
                 FROM watchlist w
                 JOIN signal_log s ON w.signal_id = s.id
                 WHERE w.status = 'ACTIVE'
                 ORDER BY w.added_date DESC''')
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def update_watchlist_status(watchlist_id: int, status: str):
    conn = get_connection()
    c = conn.cursor()
    c.execute('UPDATE watchlist SET status = ? WHERE id = ?', (status, watchlist_id))
    conn.commit()
    conn.close()

def log_regime(regime: dict):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO regime_log (
        date, nifty_close, ema200, market_state, vix_today, vix_52wk_avg,
        fii_flow, fii_streak, fii_direction, l1_score, l2_score, sector_leaders
    ) VALUES (
        :date, :nifty_close, :ema200, :market_state, :vix_today, :vix_52wk_avg,
        :fii_flow, :fii_streak, :fii_direction, :l1_score, :l2_score, :sector_leaders
    )''', regime)
    conn.commit()
    conn.close()

def get_all_closed_trades(paper: bool = False):
    table = "paper_trades" if paper else "live_trades"
    conn = get_connection()
    c = conn.cursor()
    c.execute(f'''SELECT t.*, s.ticker, s.market_state, s.research_score,
                         s.score_band, s.entry_type, s.sector
                  FROM {table} t
                  JOIN signal_log s ON t.signal_id = s.id
                  WHERE t.status = 'CLOSED'
                  ORDER BY t.exit_date DESC''')
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_signal_by_ticker(ticker: str):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''SELECT * FROM signal_log WHERE ticker = ?
                 ORDER BY id DESC LIMIT 1''', (ticker,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None

def upsert_run_registry(run: dict):
    conn = get_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO run_registry (
        run_id, created_at_utc, finalized_at_utc, paper, provider, timing_mode,
        analysis_input_path, analysis_output_path, data_quality_report_path,
        compliance_report_path, bias_report_path, analysis_input_sha256,
        output_schema_ok, compliance_pct, recency_bias_avg, status, notes
    ) VALUES (
        :run_id, :created_at_utc, :finalized_at_utc, :paper, :provider, :timing_mode,
        :analysis_input_path, :analysis_output_path, :data_quality_report_path,
        :compliance_report_path, :bias_report_path, :analysis_input_sha256,
        :output_schema_ok, :compliance_pct, :recency_bias_avg, :status, :notes
    )
    ON CONFLICT(run_id) DO UPDATE SET
        finalized_at_utc=excluded.finalized_at_utc,
        analysis_output_path=excluded.analysis_output_path,
        compliance_report_path=excluded.compliance_report_path,
        bias_report_path=excluded.bias_report_path,
        output_schema_ok=excluded.output_schema_ok,
        compliance_pct=excluded.compliance_pct,
        recency_bias_avg=excluded.recency_bias_avg,
        status=excluded.status,
        notes=excluded.notes
    ''', run)
    conn.commit()
    conn.close()
