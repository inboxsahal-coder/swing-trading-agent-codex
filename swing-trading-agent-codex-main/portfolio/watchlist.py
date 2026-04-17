import datetime
import yfinance as yf

def check_watchlist(db, results_blackout):
    items = db.get_active_watchlist()
    if not items:
        return [], []

    hits = []
    updates = []
    today = datetime.date.today()

    for item in items:
        ticker = item['ticker']
        added_date = datetime.date.fromisoformat(item['added_date'])
        expiry_date = datetime.date.fromisoformat(item['expiry_date'])
        entry_zone = item['entry_zone_price']
        watchlist_id = item['id']

        if ticker in results_blackout:
            db.update_watchlist_status(watchlist_id, 'SKIP_FLAGGED')
            updates.append({
                "ticker": ticker,
                "status": "SKIP_FLAGGED",
                "message": f"Watchlist removed: {ticker} — results approaching"
            })
            continue

        if today > expiry_date:
            db.update_watchlist_status(watchlist_id, 'EXPIRED')
            updates.append({
                "ticker": ticker,
                "status": "EXPIRED",
                "message": f"Watchlist expired: {ticker} — re-evaluate or abandon?"
            })
            continue

        try:
            data = yf.download(ticker + ".NS", period="2d", auto_adjust=True, progress=False)
            if hasattr(data.columns, 'nlevels') and data.columns.nlevels > 1:
                data.columns = data.columns.get_level_values(0)
            if data.empty:
                continue
            current_price = float(data['Close'].iloc[-1])
        except:
            continue

        price_diff_pct = abs(current_price - entry_zone) / entry_zone

        if price_diff_pct <= 0.02:
            db.update_watchlist_status(watchlist_id, 'HIT')
            hits.append({
                "ticker": ticker,
                "current_price": current_price,
                "entry_zone": entry_zone,
                "stop": item.get('stop'),
                "t1": item.get('t1'),
                "t2": item.get('t2'),
                "shares": item.get('shares'),
                "max_loss": item.get('max_loss'),
                "entry_timing": item.get('entry_timing'),
                "days_since_added": (today - added_date).days,
                "message": f"WATCHLIST HIT: {ticker} — added {(today - added_date).days} days ago, entry zone ₹{entry_zone:,.2f} reached"
            })
        elif current_price > entry_zone * 1.02:
            updates.append({
                "ticker": ticker,
                "status": "GAP_ABOVE",
                "current_price": current_price,
                "entry_zone": entry_zone,
                "message": f"Entry zone missed for {ticker} — price ₹{current_price:,.2f} above zone ₹{entry_zone:,.2f}. Still valid? (Y/N)"
            })

    return hits, updates

def add_to_watchlist(db, signal_id, ticker, entry_zone, config):
    expiry_days = config.get('watchlist_expiry_trading_days', 5)
    expiry_date = (datetime.date.today() + datetime.timedelta(days=expiry_days * 1.4)).isoformat()
    db.add_to_watchlist(signal_id, ticker, entry_zone, expiry_date)
    print(f"Added {ticker} to watchlist. Entry zone: ₹{entry_zone:,.2f}. Expires: {expiry_date}")
