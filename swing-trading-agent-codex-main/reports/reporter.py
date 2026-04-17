import json
import os
import datetime

def _fmt_metric(value, suffix=""):
    if value is None or value == "":
        return "NA"
    return f"{value}{suffix}"

def generate_report(analysis, portfolio_actions, watchlist_hits, watchlist_updates, capital, paper=False):
    lines = []
    mode = "PAPER" if paper else "LIVE"
    today = datetime.date.today()
    day_name = today.strftime("%A").upper()
    run_time = datetime.datetime.now().strftime("%I:%M %p")

    market_state = analysis.get('market_state', 'UNKNOWN')
    vix = analysis.get('vix_today', 0)
    entry_mode = analysis.get('entry_mode', 'UNKNOWN')
    l1 = analysis.get('l1_score', 0)
    l2 = analysis.get('l2_score', 0)
    dq = analysis.get('data_quality', {})
    data_status = "COMPLETE" if all(v == "COMPLETE" for k, v in dq.items() if isinstance(v, str)) else "PARTIAL"

    sectors = analysis.get('market_context', {}).get('sectors', {})
    leading = [s for s, v in sectors.items() if v.get('quadrant') == 'LEADING']
    improving = [s for s, v in sectors.items() if v.get('quadrant') == 'IMPROVING']
    lagging = [s for s, v in sectors.items() if v.get('quadrant') == 'LAGGING']

    sep = "=" * 60

    lines.append(sep)
    lines.append(f"  TRADING SIGNAL REPORT — {today} {day_name} | Run: {run_time} | {mode}")
    lines.append(sep)
    lines.append(f"DATA: {data_status}")
    lines.append(f"STATE: {market_state} | VIX: {vix} | Mode: {entry_mode}")
    lines.append(f"MACRO  [{'+' if l1 >= 0 else ''}{l1}]")
    lines.append(f"INDIA  [{'+' if l2 >= 0 else ''}{l2}]")
    lines.append(f"SECTORS  Leading: {', '.join(leading) if leading else 'None'}")
    lines.append(f"         Improving: {', '.join(improving) if improving else 'None'}")
    lines.append(f"         Avoid (Lagging): {', '.join(lagging) if lagging else 'None'}")

    if watchlist_hits:
        lines.append("")
        lines.append(sep)
        lines.append("  WATCHLIST HITS — act on these first")
        lines.append(sep)
        for hit in watchlist_hits:
            lines.append(f"[WATCHLIST HIT] {hit['ticker']} — added {hit['days_since_added']} days ago, entry zone ₹{hit['entry_zone']:,.2f} reached")
            lines.append(f"  Entry: ₹{hit.get('entry_zone', 0):,.2f} | Stop: ₹{hit.get('stop', 0):,.2f} | T1: ₹{hit.get('t1', 0):,.2f} | T2: ₹{hit.get('t2', 0):,.2f}")
            if hit.get('shares'):
                lines.append(f"  Shares: {hit['shares']} | Max loss: ₹{hit.get('max_loss', 0):,.2f}")
            if hit.get('entry_timing'):
                lines.append(f"  Timing: {hit['entry_timing']}")
            lines.append("")

    if watchlist_updates:
        for upd in watchlist_updates:
            lines.append(f"  {upd['message']}")

    lines.append("")
    lines.append(sep)
    lines.append("  NEW SIGNALS TODAY")
    lines.append(sep)

    candidates = analysis.get('candidates', [])
    buy_signals = [c for c in candidates if c.get('signal') == 'BUY' and not c.get('position_blocked')]
    blocked = [c for c in candidates if c.get('signal') == 'BUY' and c.get('position_blocked')]

    if not buy_signals:
        lines.append("No buy signals today.")
    else:
        for rank, c in enumerate(buy_signals, 1):
            score = c.get('research_score', 0)
            band = c.get('score_band', '')
            entry_type = c.get('entry_type', '')
            sector = c.get('sector', '')
            sector_quadrant = sectors.get(sector, {}).get('quadrant', '')
            tier = c.get('tier', '')
            rsi = c.get('rsi14')
            adx = c.get('adx14')
            vol = c.get('volume_ratio')
            rs = c.get('rs_vs_nifty_20d')
            vcp = c.get('vcp_detected', False)

            entry = c.get('entry', 0)
            stop = c.get('stop', 0)
            t1 = c.get('t1', 0)
            t2 = c.get('t2', 0)
            shares = c.get('shares', 0)
            pos_value = c.get('position_value', 0)
            max_loss = c.get('max_loss', 0)
            risk_pct = c.get('risk_pct_of_capital', 0)
            atr = c.get('atr', 0)
            atr_mult = c.get('atr_multiplier_used', 0)
            entry_timing = c.get('entry_timing', '')
            reasoning = c.get('reasoning', '')
            l5_check = c.get('l5_manual_check_needed', False)

            rr = round((t1 - entry) / (entry - stop), 1) if entry and stop and t1 and entry != stop else 0

            next_day = _next_trading_day(today)
            time_stop_days = _get_time_stop_days(market_state)
            time_stop_date = _add_trading_days(today, time_stop_days)

            lines.append("")
            lines.append(f"RANK #{rank} — {c['ticker']} | Score: {score}/10 | {band}")
            lines.append(f"Sector: {sector} ({sector_quadrant}) | Tier: {tier} | Pattern: {entry_type}")
            rs_text = "NA" if rs is None else f"{'+' if rs >= 0 else ''}{rs}%"
            lines.append(
                f"RSI: {_fmt_metric(rsi)}  ADX: {_fmt_metric(adx)}  "
                f"Vol: {_fmt_metric(vol, 'x')}  RS vs Nifty: {rs_text}"
            )
            if vcp:
                lines.append("  [VCP CONFIRMED]")
            lines.append(f"  ENTRY:     ₹{entry:,.2f} (next trading day — {next_day})")
            lines.append(f"  STOP:      ₹{stop:,.2f} ({atr_mult}x ATR — {market_state} rule)")
            lines.append(f"  TARGET 1:  ₹{t1:,.2f} → sell 40% here, move stop to entry")
            lines.append(f"  TARGET 2:  ₹{t2:,.2f} → R:R {rr}:1")
            lines.append(f"  SHARES:    {shares} | Value: ₹{pos_value:,.2f} | Max loss: ₹{max_loss:,.2f} ({risk_pct}% of capital)")
            lines.append(f"  TIME STOP: {time_stop_date} ({time_stop_days} trading days)")
            lines.append(f"  TIMING:    {entry_timing}")
            lines.append(f"  WHY:       {reasoning}")
            if l5_check:
                lines.append("  ⚠ Manual check: Verify fundamentals on Screener.in before placing.")
            lines.append(f"  Action → [P]lace order  [W]atchlist  [X]Abandon")
            lines.append("  " + "-" * 56)

    if blocked:
        lines.append("")
        lines.append("SIGNALS BLOCKED BY POSITION LIMITS:")
        for c in blocked:
            lines.append(f"  {c['ticker']} (score {c.get('research_score', 0)}) — {c.get('position_blocked', '')}")

    mcx = analysis.get('mcx_gold', {})
    lines.append("")
    if mcx.get('signal') == 'BUY':
        lines.append(f"MCX GOLD: BUY | Entry: ₹{mcx.get('entry', 0):,.2f} | Stop: ₹{mcx.get('stop', 0):,.2f} | T1: ₹{mcx.get('t1', 0):,.2f}")
        lines.append("  Note: MCX open until 11:30 PM. Confirm price before placing.")
    else:
        reason = mcx.get('reason', 'Conditions not met')
        met = mcx.get('conditions_met', 0)
        lines.append(f"MCX GOLD: NO_TRADE — {reason} ({met}/3 conditions met)")

    lines.append("")
    lines.append(sep)
    lines.append("  PORTFOLIO STATUS")
    lines.append(sep)

    must_exit = portfolio_actions.get('must_exit', [])
    sell_half = portfolio_actions.get('sell_half', [])
    trail_stop = portfolio_actions.get('trail_stop', [])
    alerts = portfolio_actions.get('alerts', [])
    hold = portfolio_actions.get('hold', [])
    blocked_monthly = portfolio_actions.get('monthly_loss_blocked', False)

    if blocked_monthly:
        lines.append("⛔ MONTHLY LOSS LIMIT HIT — no new trades this month")

    if must_exit:
        for m in must_exit:
            lines.append(f"MUST EXIT:  {m['ticker']} — {m['reason']}")
    if sell_half:
        for s in sell_half:
            lines.append(f"SELL HALF:  {s['ticker']} — {s['reason']} | New stop: ₹{s.get('new_stop', 0):,.2f}")
    if trail_stop:
        for t in trail_stop:
            lines.append(f"TRAIL STOP: {t['ticker']} — {t['reason']}")
    if alerts:
        for a in alerts:
            lines.append(f"ALERT:      {a}")
    if hold:
        for h in hold:
            lines.append(f"HOLD:       {h['ticker']}: {'+' if h['gain_pct'] >= 0 else ''}{h['gain_pct']}% | Day {h['days_held']}/{h['time_stop_days']} | Stop ₹{h['stop_price']:,.2f}")
    if not any([must_exit, sell_half, trail_stop, alerts, hold]):
        lines.append("No open positions.")

    open_count = len(hold) + len(must_exit) + len(sell_half) + len(trail_stop)
    phase = analysis.get('phase', 1)
    max_pos = analysis.get('phase_limits', {}).get('max_positions', 2)
    mtd_pnl = portfolio_actions.get('mtd_pnl', 0)
    new_trades = "YES" if buy_signals and not blocked_monthly else "NO"

    lines.append("")
    lines.append(f"Phase {phase}: {open_count}/{max_pos} positions")
    lines.append(f"MTD P&L: ₹{mtd_pnl:,.0f} | New trades: {new_trades}")
    lines.append("")
    lines.append(sep)

    report_text = "\n".join(lines)
    print(report_text)

    os.makedirs("reports", exist_ok=True)
    report_path = f"reports/{today}_report.md"
    with open(report_path, "w") as f:
        f.write(report_text)

    lines.append(f"Report saved: {report_path}")
    lines.append(f"To act: python main.py trade TICKER place|watch|abandon")
    lines.append(sep)

    return report_text

def _next_trading_day(today):
    next_day = today + datetime.timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += datetime.timedelta(days=1)
    return next_day.strftime("%d %b %Y")

def _add_trading_days(start, days):
    current = start
    added = 0
    while added < days:
        current += datetime.timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current.strftime("%d %b %Y")

def _get_time_stop_days(market_state):
    table = {
        "STRONG_BULL": 15,
        "WEAK_BULL": 12,
        "SECTOR_ROTATION": 10,
        "BEAR_CORRECTION": 8,
        "HIGH_VOLATILITY": 5,
        "SIDEWAYS": 7,
    }
    return table.get(market_state, 12)
