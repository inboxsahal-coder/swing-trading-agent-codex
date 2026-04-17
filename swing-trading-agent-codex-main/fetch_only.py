"""
fetch_only.py — Run this on your LOCAL machine (needs internet).
Fetches all market data and writes analysis_input.json.

Usage:
    python fetch_only.py
    python fetch_only.py --skip-hours-check

After this completes, upload analysis_input.json to your Codex session
and say: Read CODEX.md and analysis_input.json, perform analysis, write analysis_output.json
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

def market_hours_warning():
    if "--skip-hours-check" in sys.argv:
        return
    pytz = _import_or_none("pytz")
    if pytz:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.datetime.now(ist)
    else:
        ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        now = datetime.datetime.now(ist)
    market_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    safe_run     = now.replace(hour=20, minute=0,  second=0, microsecond=0)
    if market_open <= now <= market_close:
        print("WARNING: Market is open (9:15 AM - 3:30 PM IST). EOD data is incomplete.")
        print("Recommended: run after 8 PM IST. Continuing anyway — data gaps will be flagged.")
        print()
    elif market_close < now < safe_run:
        print("WARNING: Market closed but EOD data may still be processing.")
        print("Recommended run time: 8 PM IST. Continuing anyway.")
        print()

def main():
    print("=" * 60)
    print("  SWING TRADING AGENT — DATA FETCH")
    print("=" * 60)

    market_hours_warning()
    config = load_config()

    import yfinance as yf
    from data.fetch import (
        fetch_universe, fetch_ohlcv_batch, fetch_fii_data,
        fetch_bhavcopy, fetch_results_calendar, fetch_sector_rs,
        get_vix_avg, fetch_fundamentals, fetch_global_macro,
    )
    from data.prefilter import run_prefilter
    from engine.formatter import build_analysis_input
    from db.database import init_db, get_open_positions, get_active_watchlist
    from universe_fallback import NIFTY50, NIFTY_NEXT50, MIDCAP_TOP100, SMALLCAP_TOP100

    init_db()

    print("\n[1/8] Fetching Nifty + VIX...")
    try:
        from data.fetch import _flatten_df
        nifty_df = _flatten_df(yf.download("^NSEI", period="12mo", auto_adjust=True, progress=False))
        print(f"      Nifty: {len(nifty_df)} days loaded")
    except Exception as e:
        print(f"      Nifty fetch failed: {e}")
        nifty_df = None

    try:
        vix_data = yf.download("^INDIAVIX", period="5d", auto_adjust=True, progress=False)
        from data.fetch import _flatten_df
        vix_data = _flatten_df(vix_data)
        vix_today = float(vix_data['Close'].iloc[-1]) if not vix_data.empty else 18.0
        print(f"      VIX today: {vix_today}")
    except:
        vix_today = 18.0
        print("      VIX fetch failed — defaulting to 18.0")

    vix_52wk_avg = get_vix_avg()
    print(f"      VIX 52wk avg: {vix_52wk_avg}")

    print("\n[2/8] Fetching global macro (S&P500, DXY, Crude, Gold, US10Y)...")
    global_macro = fetch_global_macro()
    print(f"      S&P500: {global_macro.get('sp500')}%  DXY: {global_macro.get('dxy_price')}  Crude: {global_macro.get('brent_price')}")

    print("\n[3/8] Fetching FII flow data...")
    fii_data = fetch_fii_data()
    print(f"      Source: {fii_data['source']} | Direction: {fii_data['direction']} | Flow: {fii_data.get('flow_crores')} Cr")

    print("\n[4/8] Fetching sector RS vs Nifty (12 sectors)...")
    sector_rs = fetch_sector_rs()
    leading = [s for s, v in sector_rs.items() if v.get('quadrant') == 'LEADING']
    print(f"      {len(sector_rs)}/12 sectors fetched. Leading: {leading}")

    print("\n[5/8] Fetching results calendar (next 10 days)...")
    results_blackout = fetch_results_calendar()
    print(f"      {len(results_blackout)} stocks in blackout")

    print("\n[6/8] Fetching bhavcopy (delivery %)...")
    bhavcopy = fetch_bhavcopy()
    print(f"      Bhavcopy: {'loaded' if bhavcopy is not None else 'FAILED — delivery % will be skipped'}")

    print("\n[7/8] Fetching universe and OHLCV (this takes 5-10 mins)...")
    universe_df = fetch_universe()
    all_tickers = universe_df['symbol'].tolist() if hasattr(universe_df, 'columns') else list(universe_df)

    tier_map = {}
    for t in NIFTY50:       tier_map[t] = 1
    for t in NIFTY_NEXT50:  tier_map[t] = 2
    for t in MIDCAP_TOP100: tier_map[t] = 3
    for t in SMALLCAP_TOP100: tier_map[t] = 4

    ohlcv_data = fetch_ohlcv_batch(all_tickers, period="18mo")

    active_watchlist = get_active_watchlist()
    watchlist_tickers = [w['ticker'] for w in active_watchlist]
    open_positions = get_open_positions(paper=True)

    print("\n[8/8] Running prefilter + building analysis_input.json...")
    fundamentals = fetch_fundamentals(all_tickers[:50])

    candidates = run_prefilter(
        ohlcv_data, nifty_df, results_blackout,
        bhavcopy, watchlist_tickers, config, tier_map
    )

    build_analysis_input(
        candidates, ohlcv_data, nifty_df, None,
        sector_rs, fii_data, global_macro,
        vix_today, vix_52wk_avg, fundamentals,
        bhavcopy, results_blackout,
        active_watchlist, open_positions, config
    )

    print()
    print("=" * 60)
    print("  analysis_input.json is ready.")
    print("=" * 60)
    print()
    print("NEXT STEP — Go to your Codex session and say:")
    print()
    print('  "Read CODEX.md and analysis_input.json, perform analysis,')
    print('   write analysis_output.json"')
    print()
    print("Then run:  python finalize.py")
    print("=" * 60)

if __name__ == "__main__":
    main()
