"""
finalize.py — Run this on your LOCAL machine (needs internet for live prices).
Reads analysis_output.json, applies position sizing, checks portfolio, logs to DB.

Usage:
    python finalize.py          # paper mode (default)
    python finalize.py --live   # live mode (real trades)

Run this AFTER Codex has written analysis_output.json.
"""

import sys
import os
import json
import datetime

def _import_or_none(module_name):
    try:
        return __import__(module_name)
    except Exception:
        return None

PAPER = "--live" not in sys.argv

def load_config():
    yaml = _import_or_none("yaml")
    if yaml and os.path.exists("config.yaml"):
        with open("config.yaml", "r") as f:
            return yaml.safe_load(f)
    if os.path.exists("config.json"):
        with open("config.json", "r") as f:
            return json.load(f)
    print("Config file missing. Expected config.yaml (or config.json fallback).")
    sys.exit(1)

def prompt_capital():
    try:
        val = input("\nEnter capital to deploy today (Rs): ").replace(",", "").replace("₹", "").strip()
        capital = float(val)
        print(f"Capital: Rs {capital:,.0f}  |  Mode: {'PAPER' if PAPER else 'LIVE'}\n")
        return capital
    except:
        print("Invalid input. Exiting.")
        sys.exit(1)

def main():
    mode = "PAPER" if PAPER else "LIVE"
    print("=" * 60)
    print(f"  SWING TRADING AGENT — FINALIZE ({mode})")
    print("=" * 60)

    if not os.path.exists("analysis_output.json"):
        print("\nERROR: analysis_output.json not found.")
        print("Run Codex analysis first, then re-run this script.")
        sys.exit(1)

    config = load_config()
    capital = prompt_capital()

    from db.database import (
        init_db, get_open_positions, get_active_watchlist,
        log_signal, get_unchecked_abandoned_signals, update_signal_outcome
    )
    from engine.ranker import load_and_rank
    from portfolio.portfolio import check_portfolio
    from portfolio.watchlist import check_watchlist
    from reports.reporter import generate_report
    import yfinance as yf

    init_db()

    print("[1/5] Loading and ranking analysis output...")
    open_positions = get_open_positions(paper=PAPER)
    analysis = load_and_rank(capital, config, open_positions, paper=PAPER)
    if not analysis:
        print("Ranking failed. Check analysis_output.json.")
        sys.exit(1)

    vix_today   = analysis.get('vix_today', 18.0)
    market_state = analysis.get('market_state', 'WEAK_BULL')

    print("[2/5] Checking shadow book outcomes...")
    _check_shadow_book(get_unchecked_abandoned_signals, update_signal_outcome, config)

    print("[3/5] Checking watchlist...")
    results_blackout = _load_results_blackout()

    class DB:
        def get_active_watchlist(self): return get_active_watchlist()
        def update_watchlist_status(self, wid, status):
            from db.database import update_watchlist_status
            update_watchlist_status(wid, status)
        def get_mtd_pnl(self, paper=False):
            from db.database import get_mtd_pnl
            return get_mtd_pnl(paper=paper)
        def get_open_positions(self, paper=False):
            from db.database import get_open_positions
            return get_open_positions(paper=paper)

    db = DB()
    watchlist_hits, watchlist_updates = check_watchlist(db, results_blackout)
    if watchlist_hits:
        print(f"      {len(watchlist_hits)} watchlist hit(s)!")
    else:
        print("      No watchlist hits.")

    print("[4/5] Checking open positions (live prices)...")
    portfolio_actions = check_portfolio(db, vix_today, market_state, capital, paper=PAPER)

    print("[5/5] Logging signals to DB and generating report...")
    for c in analysis.get('candidates', []):
        if c.get('signal') == 'BUY':
            try:
                log_signal({
                    'date': datetime.date.today().isoformat(),
                    'ticker': c['ticker'],
                    'tier': c.get('tier', 3),
                    'universe_input': 'NSE',
                    'market_state': market_state,
                    'research_score': c.get('research_score'),
                    'score_band': c.get('score_band'),
                    'l1': c.get('l1'), 'l2': c.get('l2'),
                    'l3': c.get('l3'), 'l4': c.get('l4'),
                    'l5': c.get('l5'), 'l6': c.get('l6'),
                    'l5_data_freshness': c.get('l5_data_freshness'),
                    'entry_type': c.get('entry_type'),
                    'vcp_detected': 1 if c.get('vcp_detected') else 0,
                    'entry': c.get('entry'),
                    'stop': c.get('stop'),
                    't1': c.get('t1'),
                    't2': c.get('t2'),
                    'shares': c.get('shares'),
                    'position_value': c.get('position_value'),
                    'max_loss': c.get('max_loss'),
                    'entry_timing': c.get('entry_timing'),
                    'reasoning': c.get('reasoning'),
                    'skip_flags': json.dumps(c.get('skip_flags', [])),
                    'sector': c.get('sector'),
                    'themes': json.dumps(c.get('themes', [])),
                    'status': 'GENERATED'
                })
            except Exception as e:
                print(f"      Signal log error for {c['ticker']}: {e}")

    generate_report(analysis, portfolio_actions, watchlist_hits, watchlist_updates, capital, paper=PAPER)

    print()
    print("To act on a signal:")
    print("  python main.py trade TICKER place   — mark as placed")
    print("  python main.py trade TICKER watch   — add to watchlist")
    print("  python main.py trade TICKER abandon — skip this signal")
    print()
    print("To check portfolio tomorrow:")
    print("  python finalize.py")
    print("=" * 60)


def _check_shadow_book(get_unchecked_fn, update_outcome_fn, config):
    import yfinance as yf
    cutoff = datetime.date.today() - datetime.timedelta(
        days=config.get('shadow_book_tracking_days', 15)
    )
    abandoned = get_unchecked_fn(older_than=cutoff)
    checked = 0
    for signal in abandoned:
        try:
            data = yf.download(signal['ticker'] + ".NS", period="1d", progress=False)
            if hasattr(data.columns, 'nlevels') and data.columns.nlevels > 1:
                data.columns = data.columns.get_level_values(0)
            if data.empty:
                continue
            current_price = float(data['Close'].iloc[-1])
            if current_price >= signal['t1']:
                outcome = "T1_WOULD_HIT"
            elif current_price >= signal['entry']:
                outcome = "PROFITABLE_BUT_BELOW_T1"
            elif current_price < signal['stop']:
                outcome = "STOP_WOULD_HIT"
            else:
                outcome = "FLAT"
            update_outcome_fn(signal['id'], outcome)
            checked += 1
        except:
            pass
    if checked:
        print(f"      {checked} abandoned signals outcome-checked.")


def _load_results_blackout():
    try:
        from data.fetch import fetch_results_calendar
        return fetch_results_calendar()
    except:
        return set()


if __name__ == "__main__":
    main()
