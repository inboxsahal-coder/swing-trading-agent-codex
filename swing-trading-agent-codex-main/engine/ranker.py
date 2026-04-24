import json
import datetime

RISK_TABLE = {
    "STRONG_BULL":     {"risk_pct": 0.015, "max_pos_pct": 0.20},
    "WEAK_BULL":       {"risk_pct": 0.010, "max_pos_pct": 0.15},
    "SECTOR_ROTATION": {"risk_pct": 0.0125, "max_pos_pct": 0.15},
    "BEAR_CORRECTION": {"risk_pct": 0.0075, "max_pos_pct": 0.10},
    "HIGH_VOLATILITY": {"risk_pct": 0.005,  "max_pos_pct": 0.10},
    "SIDEWAYS":        {"risk_pct": 0.010,  "max_pos_pct": 0.10},
}

def apply_position_sizing(candidates, capital, market_state, config, open_positions):
    phase = config.get('phase', 1)
    phase_config = config.get('phases', {}).get(phase, {})
    max_positions = phase_config.get('max_positions', 2)
    monthly_loss_limit = config.get('monthly_loss_limit_pct', 0.06)

    risk_params = RISK_TABLE.get(market_state, RISK_TABLE["WEAK_BULL"])
    risk_pct = risk_params['risk_pct']
    max_pos_pct = risk_params['max_pos_pct']

    per_trade_risk = capital * risk_pct
    max_position_value = capital * max_pos_pct

    current_open = len(open_positions)
    slots_available = max_positions - current_open

    sector_counts = {}
    for pos in open_positions:
        sec = pos.get('sector', 'UNKNOWN')
        sector_counts[sec] = sector_counts.get(sec, 0) + 1

    total_heat = sum(pos.get('max_loss', 0) for pos in open_positions)
    heat_limit = capital * monthly_loss_limit
    heat_remaining = heat_limit - total_heat

    storyline_weight = float(config.get("storyline_weight", 0.35))

    def _num(val, default=0.0):
        try:
            return float(val)
        except Exception:
            return float(default)

    def _composite_rank_score(c):
        research_score = _num(c.get("research_score"), 0.0)
        storyline_score = _num(c.get("storyline_score"), 0.0)
        return research_score + (storyline_weight * storyline_score)

    buy_signals = [c for c in candidates if c.get('signal') == 'BUY' and not c.get('skip_flags')]
    for c in buy_signals:
        c["composite_rank_score"] = round(_composite_rank_score(c), 2)
    buy_signals.sort(key=lambda x: (x.get("composite_rank_score", 0), x.get("research_score", 0)), reverse=True)

    ranked = []
    slots_used = 0

    for c in buy_signals:
        if slots_used >= slots_available:
            c['position_blocked'] = f"No slots available (max {max_positions}, {current_open} open)"
            ranked.append(c)
            continue

        sector = c.get('sector', 'UNKNOWN')
        if sector_counts.get(sector, 0) >= 2:
            c['position_blocked'] = f"Sector concentration limit reached for {sector}"
            ranked.append(c)
            continue

        entry = c.get('entry', 0)
        stop = c.get('stop', 0)
        if not entry or not stop or entry <= stop:
            c['position_blocked'] = "Invalid entry/stop prices"
            ranked.append(c)
            continue

        risk_per_share = entry - stop
        shares = int(per_trade_risk / risk_per_share)
        if shares < 1:
            shares = 1

        position_value = shares * entry
        if position_value > max_position_value:
            shares = int(max_position_value / entry)
            position_value = shares * entry

        max_loss = shares * risk_per_share

        if max_loss > heat_remaining:
            shares = int(heat_remaining / risk_per_share)
            if shares < 1:
                c['position_blocked'] = "Portfolio heat limit reached"
                ranked.append(c)
                continue
            position_value = shares * entry
            max_loss = shares * risk_per_share

        c['shares'] = shares
        c['position_value'] = round(position_value, 2)
        c['max_loss'] = round(max_loss, 2)
        c['risk_pct_of_capital'] = round((max_loss / capital) * 100, 2)

        heat_remaining -= max_loss
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        slots_used += 1

        ranked.append(c)

    no_trade = [c for c in candidates if c.get('signal') != 'BUY' or c.get('skip_flags')]

    return ranked, no_trade

def load_and_rank(capital, config, open_positions, paper=False):
    try:
        with open("analysis_output.json", "r") as f:
            analysis = json.load(f)
    except FileNotFoundError:
        print("analysis_output.json not found. Run analysis first.")
        return None

    market_state = analysis.get('market_state', 'WEAK_BULL')
    candidates = analysis.get('candidates', [])

    ranked, no_trade = apply_position_sizing(
        candidates, capital, market_state, config, open_positions
    )

    analysis['candidates'] = ranked
    analysis['no_trade_stocks'] = analysis.get('no_trade_stocks', [])
    analysis['capital_used'] = capital
    analysis['ranked_at'] = datetime.datetime.now().isoformat()

    with open("analysis_output_ranked.json", "w") as f:
        json.dump(analysis, f, indent=2)

    return analysis
