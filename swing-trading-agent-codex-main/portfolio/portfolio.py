import datetime
import yfinance as yf

ATR_MULTIPLIERS = {
    "STRONG_BULL":     {"vix_low": 1.5,  "vix_high": 1.5},
    "WEAK_BULL":       {"vix_low": 1.75, "vix_high": 2.0},
    "SECTOR_ROTATION": {"vix_low": 1.5,  "vix_high": 1.5},
    "BEAR_CORRECTION": {"vix_low": 2.0,  "vix_high": 2.0},
    "HIGH_VOLATILITY": {"vix_low": 2.5,  "vix_high": 2.5},
    "SIDEWAYS":        {"vix_low": None, "vix_high": None},
}

TIME_STOP_DAYS = {
    "STRONG_BULL":     15,
    "WEAK_BULL":       12,
    "SECTOR_ROTATION": 10,
    "BEAR_CORRECTION": 8,
    "HIGH_VOLATILITY": 5,
    "SIDEWAYS":        7,
}

def check_portfolio(db, vix_today, market_state, capital, paper=False):
    actions = {
        "must_exit": [],
        "sell_half": [],
        "trail_stop": [],
        "alerts": [],
        "hold": [],
        "monthly_loss_blocked": False
    }

    mtd_pnl = db.get_mtd_pnl(paper=paper)
    monthly_loss_limit = capital * 0.06
    if mtd_pnl < -monthly_loss_limit:
        actions['monthly_loss_blocked'] = True
        print(f"Monthly loss limit hit: ₹{mtd_pnl:,.0f}. No new trades this month.")
        return actions

    open_positions = db.get_open_positions(paper=paper)
    if not open_positions:
        return actions

    for pos in open_positions:
        ticker = pos['ticker']
        entry_price = pos['entry_price_actual'] or pos['entry']
        stop_price = pos['stop_price']
        t1_price = pos['t1_price']
        t2_price = pos['t2_price']
        shares = pos['shares_actual']
        entry_date_str = pos['entry_date']
        entry_market_state = pos.get('market_state', market_state)

        try:
            data = yf.download(ticker + ".NS", period="5d", auto_adjust=True, progress=False)
            if hasattr(data.columns, 'nlevels') and data.columns.nlevels > 1:
                data.columns = data.columns.get_level_values(0)
            if data.empty:
                actions['alerts'].append(f"{ticker}: Could not fetch price")
                continue
            current_price = float(data['Close'].iloc[-1])
            current_volume = float(data['Volume'].iloc[-1])
            avg_volume = float(data['Volume'].mean())
        except Exception as e:
            actions['alerts'].append(f"{ticker}: Price fetch error — {e}")
            continue

        entry_date = datetime.date.fromisoformat(entry_date_str) if entry_date_str else datetime.date.today()
        days_held = (datetime.date.today() - entry_date).days
        gain_pct = ((current_price - entry_price) / entry_price) * 100

        if current_price < stop_price:
            actions['must_exit'].append({
                "ticker": ticker,
                "reason": f"Stop hit — close ₹{current_price:,.2f} < stop ₹{stop_price:,.2f}",
                "current_price": current_price
            })
            continue

        time_stop_days = TIME_STOP_DAYS.get(entry_market_state, 12)
        if days_held >= time_stop_days and current_price < t1_price:
            actions['must_exit'].append({
                "ticker": ticker,
                "reason": f"Time stop — {days_held} days held, T1 not reached",
                "current_price": current_price
            })
            continue

        if current_price < pos.get('ema30', 0) and current_volume > avg_volume:
            actions['must_exit'].append({
                "ticker": ticker,
                "reason": f"EMA30 breakdown on volume — close ₹{current_price:,.2f}",
                "current_price": current_price
            })
            continue

        if current_price >= t1_price and pos['status'] == 'OPEN':
            actions['sell_half'].append({
                "ticker": ticker,
                "reason": f"T1 hit — close ₹{current_price:,.2f} >= T1 ₹{t1_price:,.2f}",
                "current_price": current_price,
                "new_stop": entry_price
            })
            continue

        if gain_pct >= 10 and current_price < t1_price:
            new_trail_stop = round(entry_price * 1.01, 2)
            actions['trail_stop'].append({
                "ticker": ticker,
                "reason": f"10%+ gain without T1 — trail stop to ₹{new_trail_stop:,.2f}",
                "new_stop": new_trail_stop,
                "current_price": current_price
            })
            continue

        near_stop_pct = ((current_price - stop_price) / entry_price) * 100
        if near_stop_pct <= 1.5:
            actions['alerts'].append(
                f"{ticker}: Near stop — price ₹{current_price:,.2f}, stop ₹{stop_price:,.2f} ({near_stop_pct:.1f}% buffer)"
            )

        if entry_market_state in ("STRONG_BULL", "WEAK_BULL") and market_state in ("BEAR_CORRECTION", "HIGH_VOLATILITY"):
            actions['alerts'].append(
                f"{ticker}: Regime downgrade — entered in {entry_market_state}, now {market_state}. Consider reducing 25%."
            )

        actions['hold'].append({
            "ticker": ticker,
            "current_price": current_price,
            "gain_pct": round(gain_pct, 2),
            "days_held": days_held,
            "stop_price": stop_price,
            "t1_price": t1_price,
            "time_stop_days": time_stop_days
        })

    return actions

def get_portfolio_summary(db, capital, paper=False):
    open_positions = db.get_open_positions(paper=paper)
    mtd_pnl = db.get_mtd_pnl(paper=paper)
    total_deployed = sum(p.get('position_value', 0) or 0 for p in open_positions)
    available = capital - total_deployed
    return {
        "open_count": len(open_positions),
        "total_deployed": round(total_deployed, 2),
        "available": round(available, 2),
        "mtd_pnl": round(mtd_pnl, 2),
        "positions": open_positions
    }
