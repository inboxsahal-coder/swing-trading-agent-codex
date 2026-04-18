import sys
import os
import json
import datetime
import shutil

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
    print("Install dependencies and keep the config file in project root.")
    sys.exit(1)

def get_ist_time():
    pytz = _import_or_none("pytz")
    if pytz:
        ist = pytz.timezone('Asia/Kolkata')
        return datetime.datetime.now(ist)
    ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    return datetime.datetime.now(ist)

def market_hours_guard():
    now = get_ist_time()
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    safe_run = now.replace(hour=20, minute=0, second=0, microsecond=0)
    if market_open <= now <= market_close:
        print("Market is open (9:15 AM - 3:30 PM IST). Run after 8 PM for complete EOD data.")
        sys.exit(0)
    elif market_close < now < safe_run:
        print("Market closed but data may not be complete yet.")
        print("Recommended run time: 8 PM IST. Proceeding anyway — partial data will be flagged.")

def prompt_capital():
    try:
        capital_str = input("\nEnter capital to deploy today (Rs): ").replace(",", "").replace("₹", "").strip()
        capital = float(capital_str)
        print(f"Capital: Rs {capital:,.0f}. This is not stored anywhere.\n")
        return capital
    except:
        print("Invalid capital input. Exiting.")
        sys.exit(1)

def cmd_run(paper=False, provider="chatgpt_project"):
    market_hours_guard()
    config = load_config()
    capital = prompt_capital()

    from engine.handoff import (
        generate_run_id,
        file_sha256,
        write_run_context,
        write_analysis_request_markdown,
    )

    run_id = generate_run_id()
    schema_version = "1.1"

    from db.database import init_db, get_open_positions, get_active_watchlist
    from data.fetch import (
        fetch_universe, fetch_ohlcv_batch, fetch_fii_data,
        fetch_bhavcopy, fetch_results_calendar, fetch_sector_rs,
        get_vix_avg, fetch_fundamentals, fetch_global_macro,
        check_shadow_book_outcomes, fetch_sector_classification,
        validate_candidate_data_completeness
    )
    from data.prefilter import run_prefilter
    from engine.formatter import build_analysis_input
    from engine.ranker import load_and_rank
    from portfolio.portfolio import check_portfolio, get_portfolio_summary
    from portfolio.watchlist import check_watchlist
    from reports.reporter import generate_report

    init_db()

    print("\nFetching market data...")

    try:
        import yfinance as yf
        from data.fetch import _flatten_df
        nifty_df = _flatten_df(yf.download("^NSEI", period="12mo", auto_adjust=True, progress=False))
    except Exception as e:
        print(f"Nifty fetch failed: {e}")
        nifty_df = None

    try:
        from data.fetch import _flatten_df
        vix_data = _flatten_df(yf.download("^INDIAVIX", period="5d", auto_adjust=True, progress=False))
        vix_today = float(vix_data['Close'].iloc[-1]) if not vix_data.empty else 18.0
    except:
        vix_today = 18.0

    vix_52wk_avg = get_vix_avg()
    fii_data = fetch_fii_data()
    global_macro = fetch_global_macro()
    sector_rs = fetch_sector_rs()
    results_blackout = fetch_results_calendar()
    bhavcopy = fetch_bhavcopy()

    universe_df = fetch_universe()
    all_tickers = universe_df['symbol'].tolist() if hasattr(universe_df, 'columns') else list(universe_df)

    tier_map = {}
    from universe_fallback import NIFTY50, NIFTY_NEXT50, MIDCAP_TOP100, SMALLCAP_TOP100
    for t in NIFTY50:
        tier_map[t] = 1
    for t in NIFTY_NEXT50:
        tier_map[t] = 2
    for t in MIDCAP_TOP100:
        tier_map[t] = 3
    for t in SMALLCAP_TOP100:
        tier_map[t] = 4

    print(f"Fetching OHLCV for {len(all_tickers)} stocks...")
    ohlcv_data = fetch_ohlcv_batch(all_tickers, period="18mo")

    open_positions = get_open_positions(paper=paper)
    active_watchlist = get_active_watchlist()
    watchlist_tickers = [w['ticker'] for w in active_watchlist]

    from db.database import get_connection
    class DB:
        def get_active_watchlist(self): return get_active_watchlist()
        def update_watchlist_status(self, wid, status):
            from db.database import update_watchlist_status
            update_watchlist_status(wid, status)
        def get_unchecked_abandoned_signals(self, older_than):
            from db.database import get_unchecked_abandoned_signals
            return get_unchecked_abandoned_signals(older_than)
        def update_signal_outcome(self, sid, outcome):
            from db.database import update_signal_outcome
            update_signal_outcome(sid, outcome)
        def get_mtd_pnl(self, paper=False):
            from db.database import get_mtd_pnl
            return get_mtd_pnl(paper=paper)
        def get_open_positions(self, paper=False):
            from db.database import get_open_positions
            return get_open_positions(paper=paper)

    db = DB()
    check_shadow_book_outcomes(db, config)
    watchlist_hits, watchlist_updates = check_watchlist(db, results_blackout)

    candidates = run_prefilter(
        ohlcv_data, nifty_df, results_blackout,
        bhavcopy, watchlist_tickers, config, tier_map
    )

    candidate_tickers = sorted({c["ticker"] for c in candidates if c.get("ticker")})
    fundamentals = fetch_fundamentals(candidate_tickers)
    dynamic_sector_map, unresolved_sectors = fetch_sector_classification(candidate_tickers)
    sector_data = {k: v.get("sector") for k, v in dynamic_sector_map.items()}

    blockers = validate_candidate_data_completeness(candidates, fundamentals, dynamic_sector_map)
    blocked_tickers = {item.get("ticker") for item in blockers if item.get("ticker")}
    eligible_candidates = [c for c in candidates if c.get("ticker") not in blocked_tickers]
    if unresolved_sectors:
        print(f"Data completeness blocker: unresolved sectors for {len(unresolved_sectors)} tickers")
    if blockers:
        print("\nDATA COMPLETENESS BLOCKERS DETECTED (skipping blocked tickers)")
        for item in blockers[:20]:
            print(f"  - {item['ticker']}: missing {', '.join(item['missing'])}")
        if len(blockers) > 20:
            print(f"  ... and {len(blockers) - 20} more")
        try:
            with open("data_blockers.json", "w") as f:
                json.dump(
                    {
                        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
                        "total_candidates": len(candidates),
                        "eligible_candidates": len(eligible_candidates),
                        "blocked_candidates": len(blockers),
                        "blockers": blockers
                    },
                    f,
                    indent=2
                )
            print("Blocker report written to data_blockers.json")
        except Exception as e:
            print(f"Could not write data_blockers.json: {e}")

    if not eligible_candidates:
        print("All candidates failed data completeness checks. Paper run aborted.")
        sys.exit(1)
    if blockers:
        print(f"Proceeding with {len(eligible_candidates)} eligible candidates and skipping {len(blockers)} blocked candidates.")

    build_analysis_input(
        eligible_candidates, ohlcv_data, nifty_df, sector_data,
        sector_rs, fii_data, global_macro,
        vix_today, vix_52wk_avg, fundamentals,
        bhavcopy, results_blackout,
        active_watchlist, open_positions, config,
        capital=capital,
        run_metadata={"run_id": run_id, "schema_version": schema_version}
    )

    analysis_input_hash = file_sha256("analysis_input.json")
    expected_output = f"analysis_output_{run_id}.json" if provider == "chatgpt_project" else "analysis_output.json"
    run_context = {
        "run_id": run_id,
        "schema_version": schema_version,
        "provider": provider,
        "paper": paper,
        "analysis_input_path": "analysis_input.json",
        "analysis_input_sha256": analysis_input_hash,
        "expected_analysis_output_path": expected_output,
        "created_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
    }
    write_run_context(run_context)
    request_doc = write_analysis_request_markdown(run_context)

    print("\n" + "=" * 60)
    print(f"analysis_input.json is ready. run_id={run_id}")
    print(f"Provider mode: {provider}")
    print(f"Run context saved: run_context.json")
    print(f"Analysis request saved: {request_doc}")
    if provider == "chatgpt_project":
        print("Use your ChatGPT Project and provide CODEX.md + analysis_input.json.")
        print(f"Ask it to write: {expected_output}")
    else:
        print("Use local analysis workflow and write analysis_output.json")
    print("After analysis output file is written, run:")
    print("  python main.py finalize --paper" if paper else "  python main.py finalize")
    print("=" * 60 + "\n")

def cmd_finalize(paper=False):
    config = load_config()
    capital = prompt_capital()

    from engine.handoff import load_run_context, validate_analysis_output

    from db.database import init_db, get_open_positions, log_signal
    from engine.ranker import load_and_rank
    from portfolio.portfolio import check_portfolio, get_portfolio_summary
    from reports.reporter import generate_report

    init_db()

    run_context = load_run_context() or {}
    expected_run_id = run_context.get("run_id")
    candidate_outputs = []
    if run_context.get("expected_analysis_output_path"):
        candidate_outputs.append(run_context["expected_analysis_output_path"])
    if expected_run_id:
        candidate_outputs.append(f"analysis_output_{expected_run_id}.json")
    candidate_outputs.append("analysis_output.json")

    selected_output = next((p for p in candidate_outputs if p and os.path.exists(p)), None)
    if not selected_output:
        print("No analysis output file found.")
        print("Expected one of:")
        for p in candidate_outputs:
            if p:
                print(f"  - {p}")
        sys.exit(1)

    ok, errors, _ = validate_analysis_output(selected_output, expected_run_id=expected_run_id)
    if not ok:
        print(f"analysis output validation failed for {selected_output}")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

    if selected_output != "analysis_output.json":
        shutil.copyfile(selected_output, "analysis_output.json")
        print(f"Using {selected_output} for finalize (copied to analysis_output.json)")

    open_positions = get_open_positions(paper=paper)
    analysis = load_and_rank(capital, config, open_positions, paper=paper)

    if not analysis:
        print("Ranking failed. Check analysis_output.json.")
        sys.exit(1)

    class DB:
        def get_mtd_pnl(self, paper=False):
            from db.database import get_mtd_pnl
            return get_mtd_pnl(paper=paper)
        def get_open_positions(self, paper=False):
            from db.database import get_open_positions
            return get_open_positions(paper=paper)

    portfolio_actions = check_portfolio(
        DB(),
        vix_today=analysis.get('vix_today', 18),
        market_state=analysis.get('market_state', 'WEAK_BULL'),
        capital=capital,
        paper=paper
    )

    for c in analysis.get('candidates', []):
        if c.get('signal') == 'BUY':
            try:
                log_signal({
                    'date': datetime.date.today().isoformat(),
                    'ticker': c['ticker'],
                    'tier': c.get('tier', 3),
                    'universe_input': 'NSE',
                    'market_state': analysis.get('market_state'),
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
                print(f"Signal log error for {c['ticker']}: {e}")

    generate_report(
        analysis, portfolio_actions, [], [], capital, paper=paper
    )

def cmd_portfolio():
    config = load_config()
    capital = prompt_capital()
    from db.database import init_db, get_open_positions
    from portfolio.portfolio import check_portfolio
    init_db()

    class DB:
        def get_mtd_pnl(self, paper=False):
            from db.database import get_mtd_pnl
            return get_mtd_pnl(paper=paper)
        def get_open_positions(self, paper=False):
            from db.database import get_open_positions
            return get_open_positions(paper=paper)

    actions = check_portfolio(DB(), 18.0, "WEAK_BULL", capital)
    if not any([actions['must_exit'], actions['sell_half'], actions['trail_stop'], actions['alerts'], actions['hold']]):
        print("No open positions.")
        return
    for m in actions['must_exit']:
        print(f"MUST EXIT: {m['ticker']} — {m['reason']}")
    for s in actions['sell_half']:
        print(f"SELL HALF: {s['ticker']} — {s['reason']}")
    for t in actions['trail_stop']:
        print(f"TRAIL STOP: {t['ticker']} — {t['reason']}")
    for a in actions['alerts']:
        print(f"ALERT: {a}")
    for h in actions['hold']:
        print(f"HOLD: {h['ticker']} {'+' if h['gain_pct'] >= 0 else ''}{h['gain_pct']}% | Day {h['days_held']} | Stop Rs {h['stop_price']:,.2f}")

def cmd_trade(ticker, action, note=None):
    from db.database import init_db, get_signal_by_ticker, update_signal_status, add_to_watchlist
    config = load_config()
    init_db()
    signal = get_signal_by_ticker(ticker)
    if not signal:
        print(f"No signal found for {ticker}. Creating placeholder entry.")
        return
    if action == "place":
        update_signal_status(signal['id'], 'SIGNAL_PLACED', note)
        print(f"Signal logged as PLACED in signal_log.")
    elif action == "watch":
        expiry_days = config.get('watchlist_expiry_trading_days', 5)
        expiry = (datetime.date.today() + datetime.timedelta(days=int(expiry_days * 1.4))).isoformat()
        add_to_watchlist(signal['id'], ticker, signal.get('entry', 0), expiry)
        update_signal_status(signal['id'], 'WATCHLISTED', note)
        print(f"{ticker} added to watchlist. Entry zone: Rs {signal.get('entry', 0):,.2f}")
    elif action == "abandon":
        update_signal_status(signal['id'], 'ABANDONED', note)
        print(f"{ticker} abandoned. Will track in shadow book for {config.get('shadow_book_tracking_days', 15)} days.")
    else:
        print(f"Unknown action: {action}. Use place, watch, or abandon.")

def cmd_close(ticker, exit_price, reason):
    from db.database import init_db, close_trade
    init_db()
    close_trade(ticker, float(exit_price), reason, paper=False)

def cmd_metrics():
    from db.database import init_db, get_all_closed_trades, get_connection
    config = load_config()
    init_db()
    paper_trades = get_all_closed_trades(paper=True)
    live_trades = get_all_closed_trades(paper=False)
    all_trades = paper_trades + live_trades
    total = len(all_trades)
    sep = "=" * 60
    print(sep)
    print(f"  AGENT SCORECARD | {datetime.date.today()}")
    print(sep)
    if total == 0:
        print("No closed trades yet.")
        print(f"\nPAPER MODE GO/NO-GO: NOT YET — need 20 paper trades (have {len(paper_trades)})")
        print(sep)
        return
    wins = [t for t in all_trades if t.get('pnl_abs', 0) > 0]
    win_rate = round(len(wins) / total * 100, 1)
    avg_win = round(sum(t['pnl_abs'] for t in wins) / len(wins), 2) if wins else 0
    losses = [t for t in all_trades if t.get('pnl_abs', 0) <= 0]
    avg_loss = round(sum(t['pnl_abs'] for t in losses) / len(losses), 2) if losses else 0
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM signal_log")
    total_signals = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM signal_log WHERE status='SIGNAL_PLACED'")
    placed = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM watchlist")
    watchlisted = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM signal_log WHERE status='ABANDONED'")
    abandoned = c.fetchone()[0]
    c.execute("SELECT COUNT(*), SUM(CASE WHEN outcome_result='T1_WOULD_HIT' THEN 1 ELSE 0 END), SUM(CASE WHEN outcome_result='STOP_WOULD_HIT' THEN 1 ELSE 0 END) FROM signal_log WHERE status='ABANDONED' AND outcome_checked=1")
    shadow_row = c.fetchone()
    conn.close()
    traded_pct = round(placed / total_signals * 100, 1) if total_signals else 0
    print(f"SIGNALS: {total_signals} generated | {placed} traded ({traded_pct}%) | {watchlisted} watchlisted | {abandoned} abandoned")
    print(f"\nTRADED SIGNALS:")
    print(f"  Win rate: {win_rate}% ({len(wins)}/{total})")
    print(f"  Avg win: Rs {avg_win:,.0f} | Avg loss: Rs {avg_loss:,.0f}")
    print(f"\nSCORE ACCURACY:")
    for band in ['HIGH_CONFIDENCE', 'STANDARD', 'WEAK_SIGNAL']:
        band_trades = [t for t in all_trades if t.get('score_band') == band]
        if band_trades:
            bw = len([t for t in band_trades if t.get('pnl_abs', 0) > 0])
            bwr = round(bw / len(band_trades) * 100, 1)
            print(f"  {band}: {bwr}% WR ({len(band_trades)} trades)")
    if shadow_row and shadow_row[0]:
        print(f"\nSHADOW BOOK ({shadow_row[0]} abandoned signals tracked):")
        print(f"  Would have hit T1: {shadow_row[1] or 0}")
        print(f"  Would have stopped out: {shadow_row[2] or 0}")
    paper_count = len(paper_trades)
    threshold = config.get('paper_mode_go_nogo_threshold_pct', 50.0)
    paper_wins = [t for t in paper_trades if t.get('pnl_abs', 0) > 0]
    paper_wr = round(len(paper_wins) / paper_count * 100, 1) if paper_count else 0
    print(f"\nPAPER MODE GO/NO-GO:")
    if paper_count >= 20 and paper_wr >= threshold:
        print(f"  PASS — {paper_count} paper trades, {paper_wr}% win rate")
        print(f"  GO — ready to deploy live capital.")
        print(f"  To switch: change paper_mode to false in config.yaml")
    else:
        remaining = max(0, 20 - paper_count)
        print(f"  NOT YET — {paper_count}/20 paper trades completed ({paper_wr}% WR, need {threshold}%)")
        if remaining > 0:
            print(f"  Need {remaining} more paper trades.")
    print(sep)

def cmd_history():
    from db.database import init_db, get_all_closed_trades
    init_db()
    trades = get_all_closed_trades(paper=False) + get_all_closed_trades(paper=True)
    trades.sort(key=lambda x: x.get('exit_date', ''), reverse=True)
    trades = trades[:30]
    if not trades:
        print("No closed trades in history.")
        return
    print(f"\n{'Date':<12} {'Ticker':<12} {'Entry':<10} {'Exit':<10} {'P&L':>10} {'%':>7} {'Days':>5} {'Reason':<15}")
    print("-" * 80)
    for t in trades:
        print(f"{t.get('exit_date',''):<12} {t.get('ticker',''):<12} "
              f"{t.get('entry_price_actual',0):>9,.2f} {t.get('exit_price',0):>9,.2f} "
              f"{t.get('pnl_abs',0):>10,.0f} {t.get('pnl_pct',0):>6.1f}% "
              f"{t.get('days_held',0):>5} {t.get('exit_reason',''):.<15}")

def cmd_watchlist():
    from db.database import init_db, get_active_watchlist
    init_db()
    items = get_active_watchlist()
    if not items:
        print("No active watchlist items.")
        return
    print(f"\nACTIVE WATCHLIST ({len(items)} items):")
    print("-" * 60)
    for item in items:
        print(f"  {item['ticker']} | Entry zone: Rs {item.get('entry_zone_price', 0):,.2f} | "
              f"Added: {item.get('added_date', '')} | Expires: {item.get('expiry_date', '')}")

def cmd_learn():
    from db.database import init_db
    from learning.learning import run_learning_loop
    config = load_config()
    init_db()
    run_learning_loop(config, paper=False)

def cmd_status():
    print("\nSYSTEM STATUS CHECK")
    print("-" * 40)
    checks = {}
    missing = []
    for dep in ["yaml", "yfinance", "pandas", "requests", "bs4", "pytz"]:
        if not _import_or_none(dep):
            missing.append(dep)
    if missing:
        checks['Dependencies'] = f"WARN — missing: {', '.join(missing)}"
    else:
        checks['Dependencies'] = "OK"
    try:
        _ = load_config()
        checks['Config'] = 'OK'
    except Exception as e:
        checks['Config'] = f'FAIL — {e}'
    try:
        with open("CODEX.md") as f:
            f.read()
        checks['CODEX.md'] = 'OK'
    except:
        checks['CODEX.md'] = 'FAIL — file missing'
    try:
        from db.database import init_db
        init_db()
        checks['Database'] = 'OK'
    except Exception as e:
        checks['Database'] = f'FAIL — {e}'
    try:
        import yfinance as yf
        test = yf.download("^NSEI", period="1d", progress=False)
        checks['yfinance'] = 'OK' if not test.empty else 'WARN — empty response'
    except Exception as e:
        checks['yfinance'] = f'FAIL — {e}'
    try:
        import nsefin
        checks['nsefin'] = 'OK'
    except:
        checks['nsefin'] = 'WARN — not installed, fallback will be used'
    try:
        from data.fetch import fetch_fii_data
        fii = fetch_fii_data()
        src = fii.get('source', 'unknown')
        checks['FII data'] = f"OK — source: {src}" if src != 'FAILED' else f"WARN — FII fetch failed, will use NEUTRAL context"
    except Exception as e:
        checks['FII data'] = f'WARN — {e}'
    for name, status in checks.items():
        print(f"  {name:<20} {status}")
    print("")
    all_ok = all(not status.startswith('FAIL') for status in checks.values())
    if all_ok:
        print("System ready. Run: python main.py run --paper")
    else:
        print("Fix FAIL items before running.")

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("Usage:")
        print("  python main.py run [--provider chatgpt_project|local_file]         — full EOD run")
        print("  python main.py run --paper [--provider chatgpt_project|local_file] — paper trading mode")
        print("  python main.py finalize      — after Codex writes analysis_output.json")
        print("  python main.py finalize --paper")
        print("  python main.py portfolio     — portfolio check")
        print("  python main.py trade TICKER place|watch|abandon")
        print("  python main.py close TICKER PRICE REASON")
        print("  python main.py metrics       — scorecard")
        print("  python main.py history       — last 30 trades")
        print("  python main.py watchlist     — active watchlist")
        print("  python main.py learn         — learning loop")
        print("  python main.py status        — system health check")
        sys.exit(0)

    cmd = args[0]

    try:
        if cmd == "run":
            paper = "--paper" in args
            provider = "chatgpt_project"
            if "--provider" in args:
                i = args.index("--provider")
                if i + 1 >= len(args):
                    print("Usage: python main.py run [--paper] [--provider chatgpt_project|local_file]")
                    sys.exit(1)
                provider = args[i + 1].strip().lower()
            if provider not in ("chatgpt_project", "local_file"):
                print(f"Unsupported provider: {provider}")
                print("Allowed: chatgpt_project, local_file")
                sys.exit(1)
            cmd_run(paper=paper, provider=provider)
        elif cmd == "finalize":
            paper = "--paper" in args
            cmd_finalize(paper=paper)
        elif cmd == "portfolio":
            cmd_portfolio()
        elif cmd == "trade":
            if len(args) < 3:
                print("Usage: python main.py trade TICKER place|watch|abandon")
                sys.exit(1)
            cmd_trade(args[1], args[2], args[3] if len(args) > 3 else None)
        elif cmd == "close":
            if len(args) < 4:
                print("Usage: python main.py close TICKER PRICE REASON")
                sys.exit(1)
            cmd_close(args[1], args[2], args[3])
        elif cmd == "metrics":
            cmd_metrics()
        elif cmd == "history":
            cmd_history()
        elif cmd == "watchlist":
            cmd_watchlist()
        elif cmd == "learn":
            cmd_learn()
        elif cmd == "status":
            cmd_status()
        else:
            print(f"Unknown command: {cmd}")
            print("Run python main.py for usage.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(0)
    except Exception as e:
        error_type = type(e).__name__
        if "ConnectionError" in error_type or "Timeout" in error_type:
            print(f"Network issue. Check internet connection and try again.")
        elif "FileNotFoundError" in error_type:
            print(f"File not found: {e}")
            print("Run python main.py status to check system health.")
        else:
            print(f"Error: {str(e)[:200]}")
            print("Run python main.py status if this keeps happening.")
        sys.exit(1)
