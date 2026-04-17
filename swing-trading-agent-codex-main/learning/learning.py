import json
import datetime
from db.database import get_all_closed_trades, get_connection

def run_learning_loop(config, paper=False):
    trades = get_all_closed_trades(paper=paper)
    if len(trades) < 20:
        print(f"Learning loop requires 20 closed trades. You have {len(trades)}.")
        return

    print(f"\nRunning learning loop on {len(trades)} closed trades...")

    total = len(trades)
    wins = [t for t in trades if t.get('pnl_abs', 0) > 0]
    losses = [t for t in trades if t.get('pnl_abs', 0) <= 0]
    win_rate = len(wins) / total * 100

    rr_values = []
    for t in wins:
        entry = t.get('entry_price_actual', 0)
        exit_p = t.get('exit_price', 0)
        stop = t.get('stop_price', 0)
        if entry and stop and entry != stop:
            rr = (exit_p - entry) / (entry - stop)
            rr_values.append(rr)
    avg_rr = round(sum(rr_values) / len(rr_values), 2) if rr_values else 0

    avg_win = round(sum(t.get('pnl_abs', 0) for t in wins) / len(wins), 2) if wins else 0
    avg_loss = round(sum(t.get('pnl_abs', 0) for t in losses) / len(losses), 2) if losses else 0

    by_state = {}
    for t in trades:
        state = t.get('market_state', 'UNKNOWN')
        if state not in by_state:
            by_state[state] = {'wins': 0, 'total': 0}
        by_state[state]['total'] += 1
        if t.get('pnl_abs', 0) > 0:
            by_state[state]['wins'] += 1

    by_band = {}
    for t in trades:
        band = t.get('score_band', 'UNKNOWN')
        if band not in by_band:
            by_band[band] = {'wins': 0, 'total': 0}
        by_band[band]['total'] += 1
        if t.get('pnl_abs', 0) > 0:
            by_band[band]['wins'] += 1

    by_entry_type = {}
    for t in trades:
        etype = t.get('entry_type', 'UNKNOWN')
        if etype not in by_entry_type:
            by_entry_type[etype] = {'wins': 0, 'total': 0}
        by_entry_type[etype]['total'] += 1
        if t.get('pnl_abs', 0) > 0:
            by_entry_type[etype]['wins'] += 1

    time_stopped = [t for t in trades if t.get('exit_reason') == 'TIME_STOP']
    time_stop_profitable = [t for t in time_stopped if t.get('pnl_abs', 0) > 0]
    time_stop_accuracy = round(len(time_stop_profitable) / len(time_stopped) * 100, 1) if time_stopped else 0

    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT outcome_result FROM signal_log WHERE status='ABANDONED' AND outcome_checked=1")
    shadow_results = [r[0] for r in c.fetchall()]
    conn.close()

    shadow_t1 = shadow_results.count('T1_WOULD_HIT')
    shadow_stop = shadow_results.count('STOP_WOULD_HIT')
    shadow_flat = shadow_results.count('FLAT') + shadow_results.count('PROFITABLE_BUT_BELOW_T1')

    sep = "=" * 60
    print(sep)
    print(f"  LEARNING LOOP — Batch | {datetime.date.today()}")
    print(sep)
    print(f"Trades reviewed: {total}")
    print(f"Win rate: {win_rate:.1f}% ({len(wins)}/{total})")
    print(f"Avg R:R: {avg_rr}")
    print(f"Avg win: ₹{avg_win:,.0f} | Avg loss: ₹{avg_loss:,.0f}")
    print("")
    print("WIN RATE BY MARKET STATE:")
    for state, data in by_state.items():
        wr = round(data['wins'] / data['total'] * 100, 1)
        print(f"  {state}: {wr}% ({data['wins']}/{data['total']})")
    print("")
    print("WIN RATE BY SCORE BAND:")
    for band, data in by_band.items():
        wr = round(data['wins'] / data['total'] * 100, 1)
        print(f"  {band}: {wr}% ({data['wins']}/{data['total']})")
    print("")
    print("WIN RATE BY ENTRY TYPE:")
    for etype, data in by_entry_type.items():
        wr = round(data['wins'] / data['total'] * 100, 1)
        print(f"  {etype}: {wr}% ({data['wins']}/{data['total']})")
    print("")
    print(f"TIME STOP ACCURACY: {time_stop_accuracy}% of time-stopped trades were profitable")
    print("")
    print(f"SHADOW BOOK ({len(shadow_results)} abandoned signals tracked):")
    print(f"  Would have hit T1: {shadow_t1}")
    print(f"  Would have stopped out: {shadow_stop}")
    print(f"  Flat/partial: {shadow_flat}")
    print(sep)

    proposals = _generate_proposals(by_band, by_entry_type, by_state, avg_rr, win_rate, total)

    if proposals:
        print("\nPROPOSED RULE CHANGES (based on evidence):")
        for i, p in enumerate(proposals, 1):
            print(f"  {i}. {p}")
        print("")

    print("Review proposals above. Type approved changes or press Enter to skip.")
    approved = input("Approved changes (or Enter to skip): ").strip()

    if approved:
        _apply_approved_learning(approved)
        _log_learning(config, total, win_rate, avg_rr, proposals, approved, paper)
        print("CODEX.md updated. Changes active from next run.")
    else:
        _log_learning(config, total, win_rate, avg_rr, proposals, "", paper)
        print("No changes applied.")

def _generate_proposals(by_band, by_entry_type, by_state, avg_rr, win_rate, total):
    proposals = []

    for band, data in by_band.items():
        if data['total'] >= 5:
            wr = data['wins'] / data['total'] * 100
            if band == 'WEAK_SIGNAL' and wr < 40:
                proposals.append(f"WEAK_SIGNAL band win rate is {wr:.0f}% ({data['total']} trades) — consider raising minimum score threshold from 3.5 to 4.5")
            if band == 'HIGH_CONFIDENCE' and wr > 70:
                proposals.append(f"HIGH_CONFIDENCE band win rate is {wr:.0f}% ({data['total']} trades) — consider increasing position size to 1.75% risk")

    for etype, data in by_entry_type.items():
        if data['total'] >= 5:
            wr = data['wins'] / data['total'] * 100
            if etype == 'BREAKOUT' and wr < 45:
                proposals.append(f"BREAKOUT entry win rate is {wr:.0f}% ({data['total']} trades) — consider requiring ADX > 30 for breakout entries")
            if etype == 'VCP_BREAKOUT' and wr > 65:
                proposals.append(f"VCP_BREAKOUT win rate is {wr:.0f}% ({data['total']} trades) — consider increasing VCP position size by 25%")
            if etype == 'PULLBACK_TO_EMA30' and wr > 60:
                proposals.append(f"PULLBACK_TO_EMA30 win rate is {wr:.0f}% ({data['total']} trades) — strong edge, consider prioritising pullback entries")

    if avg_rr < 1.5 and total >= 10:
        proposals.append(f"Average R:R is {avg_rr} — below 1.5 target. Consider tightening ATR multiplier by 0.25 or moving T1 to 2.5x ATR")

    return proposals

def _apply_approved_learning(approved_text):
    codexmd_path = "CODEX.md"
    with open(codexmd_path, 'r') as f:
        content = f.read()
    marker = "## LEARNING LOOP CONTEXT"
    if marker not in content:
        print("Warning: LEARNING LOOP CONTEXT section not found in CODEX.md")
        return
    before = content[:content.index(marker)]
    new_section = f"{marker}\n[Updated: {datetime.date.today()}]\n\n{approved_text}\n"
    with open(codexmd_path, 'w') as f:
        f.write(before + new_section)

def _log_learning(config, trades_reviewed, win_rate, avg_rr, proposals, approved, paper):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COALESCE(MAX(batch_number), 0) FROM system_learnings")
    last_batch = c.fetchone()[0]
    c.execute('''INSERT INTO system_learnings
                 (batch_number, date_run, trades_reviewed, win_rate, avg_rr,
                  rule_proposals_json, approved_changes_json, is_active)
                 VALUES (?, ?, ?, ?, ?, ?, ?, 1)''',
              (last_batch + 1, datetime.date.today().isoformat(),
               trades_reviewed, win_rate, avg_rr,
               json.dumps(proposals), approved))
    conn.commit()
    conn.close()
