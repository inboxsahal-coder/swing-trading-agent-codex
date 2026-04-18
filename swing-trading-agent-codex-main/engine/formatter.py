import json
import datetime
import yfinance as yf
from data.indicators import compute_rs_vs_nifty, compute_rs_vs_sector

TICKER_SECTOR_MAP = {
    # Nifty 50 / Next 50 - Banks
    "HDFCBANK": "BANK", "ICICIBANK": "BANK", "KOTAKBANK": "BANK", "AXISBANK": "BANK",
    "SBIN": "PSU_BANK", "BANKBARODA": "PSU_BANK", "CANARABANK": "PSU_BANK",
    "IDFCFIRSTB": "BANK", "INDUSINDBK": "BANK", "FEDERALBNK": "BANK",
    "BANDHANBNK": "BANK", "RBLBANK": "BANK", "AUBANK": "BANK",
    "PNB": "PSU_BANK", "UNIONBANK": "PSU_BANK", "MAHABANK": "PSU_BANK",
    # IT
    "TCS": "IT", "INFY": "IT", "WIPRO": "IT", "HCLTECH": "IT", "TECHM": "IT",
    "LTIM": "IT", "MPHASIS": "IT", "PERSISTENT": "IT", "COFORGE": "IT",
    "OFSS": "IT", "HEXAWARE": "IT", "KPITTECH": "IT",
    # Pharma / Healthcare
    "SUNPHARMA": "PHARMA", "DRREDDY": "PHARMA", "CIPLA": "PHARMA",
    "DIVISLAB": "PHARMA", "AUROPHARMA": "PHARMA", "LUPIN": "PHARMA",
    "TORNTPHARM": "PHARMA", "ALKEM": "PHARMA", "IPCALAB": "PHARMA",
    "GLENMARK": "PHARMA", "NATCOPHARMA": "PHARMA", "ABBOTINDIA": "PHARMA",
    "APOLLOHOSP": "HEALTHCARE", "MAXHEALTH": "HEALTHCARE", "FORTIS": "HEALTHCARE",
    "MEDANTA": "HEALTHCARE", "NH": "HEALTHCARE",
    # FMCG
    "HINDUNILVR": "FMCG", "ITC": "FMCG", "NESTLEIND": "FMCG", "BRITANNIA": "FMCG",
    "DABUR": "FMCG", "MARICO": "FMCG", "COLPAL": "FMCG", "GODREJCP": "FMCG",
    "EMAMILTD": "FMCG", "TATACONSUM": "FMCG", "VARUNBEV": "FMCG",
    # Auto
    "MARUTI": "AUTO", "TATAMOTORS": "AUTO", "M&M": "AUTO", "BAJAJ-AUTO": "AUTO",
    "EICHERMOT": "AUTO", "HEROMOTOCO": "AUTO", "TVSMOTORS": "AUTO",
    "ASHOKLEY": "AUTO", "MOTHERSON": "AUTO", "BOSCHLTD": "AUTO",
    "BHARATFORG": "AUTO", "MRF": "AUTO", "CEAT": "AUTO",
    # Energy / Oil & Gas
    "RELIANCE": "ENERGY", "ONGC": "ENERGY", "BPCL": "ENERGY", "IOC": "ENERGY",
    "HINDPETRO": "ENERGY", "GAIL": "ENERGY", "PETRONET": "ENERGY",
    "ADANIGREEN": "ENERGY", "TATAPOWER": "ENERGY", "NTPC": "ENERGY",
    "POWERGRID": "ENERGY", "ADANIPORTS": "INFRA",
    # Metals
    "TATASTEEL": "METAL", "JSWSTEEL": "METAL", "HINDALCO": "METAL",
    "VEDL": "METAL", "SAIL": "METAL", "NATIONALUM": "METAL", "NMDC": "METAL",
    "JINDALSTEL": "METAL", "APLAPOLLO": "METAL",
    # Infra / Capex
    "LT": "INFRA", "ULTRACEMCO": "INFRA", "SHREECEM": "INFRA", "AMBUJACEM": "INFRA",
    "ACC": "INFRA", "DALMIACEMENTBHARAT": "INFRA", "RAMCOCEM": "INFRA",
    "GRINFRA": "INFRA", "IRB": "INFRA", "KNR": "INFRA", "NCC": "INFRA",
    "GPPL": "INFRA", "CONCOR": "INFRA",
    # Realty
    "DLF": "REALTY", "GODREJPROP": "REALTY", "OBEROIRLTY": "REALTY",
    "PRESTIGE": "REALTY", "BRIGADE": "REALTY", "SOBHA": "REALTY",
    "PHOENIXLTD": "REALTY", "MAHLIFE": "REALTY",
    # Defence
    "HAL": "DEFENCE", "BEL": "DEFENCE", "COCHINSHIP": "DEFENCE",
    "MIDHANI": "DEFENCE", "DATAPATTNS": "DEFENCE", "PARAS": "DEFENCE",
    "MAZDOCK": "DEFENCE",
    # Finance / NBFC
    "BAJFINANCE": "BANK", "BAJAJFINSV": "BANK", "CHOLAFIN": "BANK",
    "MUTHOOTFIN": "BANK", "M&MFIN": "BANK", "LICSGFIN": "BANK",
    "SHRIRAMFIN": "BANK", "HDFCLIFE": "BANK", "SBILIFE": "BANK",
    "ICICIPRU": "BANK", "NIACL": "BANK", "GICRE": "BANK",
    # Telecom
    "BHARTIARTL": "IT", "IDEA": "IT",
    # Consumer / Discretionary
    "TITAN": "FMCG", "PIDILITIND": "FMCG", "HAVELLS": "INFRA",
    "VOLTAS": "INFRA", "WHIRLPOOL": "FMCG", "DIXON": "IT",
    "AMBER": "IT", "BLUESTARCO": "INFRA",
    # Chemicals
    "PIDILITIND": "FMCG", "DEEPAKNTR": "PHARMA", "AARTIIND": "PHARMA",
    "NAVINFLUOR": "PHARMA", "SRF": "PHARMA", "VINATIORGAN": "PHARMA",
}

def build_analysis_input(
    candidates,
    ohlcv_data,
    nifty_df,
    sector_data,
    sector_rs,
    fii_data,
    global_macro,
    vix_today,
    vix_52wk_avg,
    fundamentals,
    bhavcopy,
    results_blackout,
    watchlist_items,
    open_positions,
    config,
    capital=None,
    theme_map=None,
    run_metadata=None
):
    if theme_map is None:
        theme_map = {}

    if nifty_df is not None and not nifty_df.empty:
        if hasattr(nifty_df.columns, 'nlevels') and nifty_df.columns.nlevels > 1:
            nifty_df = nifty_df.copy()
            nifty_df.columns = nifty_df.columns.get_level_values(0)
    nifty_close = float(nifty_df['Close'].iloc[-1]) if nifty_df is not None and not nifty_df.empty else None
    nifty_ema200 = None
    if nifty_df is not None and not nifty_df.empty:
        from data.indicators import compute_ema
        ema200 = compute_ema(nifty_df['Close'], 200)
        nifty_ema200 = round(float(ema200.iloc[-1]), 2)

    vix_ratio = round(vix_today / vix_52wk_avg, 2) if vix_52wk_avg and vix_52wk_avg > 0 else None

    ad_ratio = 1.0
    try:
        ad_ratio = float(fii_data.get('ad_ratio', 1.0)) if fii_data else 1.0
    except:
        pass

    market_context = {
        "nifty_close": round(nifty_close, 2) if nifty_close else None,
        "nifty_ema200": nifty_ema200,
        "vix_today": round(vix_today, 2) if vix_today else None,
        "vix_52wk_avg": round(vix_52wk_avg, 2) if vix_52wk_avg else None,
        "vix_ratio": vix_ratio,
        "fii_flow_crores": fii_data.get('flow_crores') if fii_data else None,
        "fii_streak": fii_data.get('streak') if fii_data else None,
        "fii_direction": fii_data.get('direction', 'UNKNOWN') if fii_data else 'UNKNOWN',
        "advance_decline_ratio": ad_ratio,
        "global": {
            "sp500_pct": global_macro.get('sp500'),
            "nasdaq_pct": global_macro.get('nasdaq'),
            "nikkei_pct": global_macro.get('nikkei'),
            "hangseng_pct": global_macro.get('hangseng'),
            "dxy": global_macro.get('dxy_price'),
            "dxy_pct": global_macro.get('dxy'),
            "brent": global_macro.get('brent_price'),
            "brent_pct": global_macro.get('brent'),
            "gold_futures": global_macro.get('gold_price'),
            "gold_pct": global_macro.get('gold'),
            "us10y": global_macro.get('us10y_price')
        },
        "sectors": sector_rs
    }

    phase = config.get('phase', 1)
    phase_config = config.get('phases', {}).get(phase, {})

    candidate_list = []
    for c in candidates:
        ticker = c['ticker']
        ind = c.get('indicators', {})
        fund = fundamentals.get(ticker, {})
        df = ohlcv_data.get(ticker)

        rs_vs_nifty = c.get('rs_vs_nifty_20d', 0)
        rs_vs_sector = None
        ticker_sector = _get_sector(ticker, sector_data)
        if ticker_sector and ticker_sector in sector_rs:
            sector_ticker_key = _sector_to_yf(ticker_sector)
            if sector_ticker_key and sector_ticker_key in ohlcv_data:
                rs_vs_sector = compute_rs_vs_sector(df, ohlcv_data[sector_ticker_key], 20)

        results_in_days = 999
        if ticker in results_blackout:
            results_in_days = 0

        delivery_pct = c.get('delivery_pct')

        data_age_days = 999
        if fund.get('data_date'):
            try:
                data_date = datetime.date.fromisoformat(fund['data_date'])
                data_age_days = (datetime.date.today() - data_date).days
            except:
                pass

        news = _fetch_news_safe(ticker)
        bulk_deals = _fetch_bulk_deals_safe(ticker)

        skip_flags = []
        if ticker in results_blackout:
            skip_flags.append("RESULTS_BLACKOUT")

        candidate_entry = {
            "ticker": ticker,
            "tier": c.get('tier', 3),
            "sector": ticker_sector or "UNKNOWN",
            "themes": theme_map.get(ticker, []),
            "close": ind.get('close'),
            "ema30": ind.get('ema30'),
            "ema200": ind.get('ema200'),
            "ema30_slope_5d": ind.get('ema30_slope_5d'),
            "rsi14": ind.get('rsi14'),
            "adx14": ind.get('adx14'),
            "atr14": ind.get('atr14'),
            "macd_line": ind.get('macd_line'),
            "macd_signal": ind.get('macd_signal'),
            "macd_histogram": ind.get('macd_histogram'),
            "macd_histogram_prev": ind.get('macd_histogram_prev'),
            "volume_ratio": ind.get('volume_ratio'),
            "delivery_pct": delivery_pct,
            "high_52w": ind.get('high_52w'),
            "pct_of_52w_high": ind.get('pct_of_52w_high'),
            "weekly_trend_up": ind.get('weekly_trend_up'),
            "rs_vs_nifty_20d": rs_vs_nifty,
            "rs_vs_sector_1m": rs_vs_sector,
            "vcp": c.get('vcp', {"detected": False, "contractions": 0, "final_range_pct": 0.0}),
            "skip_flags": skip_flags,
            "fundamentals": {
                "pe_ratio": fund.get('pe_ratio'),
                "sector_pe_median": _get_sector_pe(ticker_sector, config),
                "debt_equity": fund.get('debt_equity'),
                "revenue_q1": fund.get('revenue_q1'),
                "revenue_q2": fund.get('revenue_q2'),
                "revenue_q3": fund.get('revenue_q3'),
                "revenue_q4": fund.get('revenue_q4'),
                "data_date": fund.get('data_date'),
                "data_age_days": data_age_days
            },
            "news_headlines": news[:5],
            "bulk_deals_7d": bulk_deals,
            "results_in_days": results_in_days
        }
        candidate_list.append(candidate_entry)

    mcx_gold = _build_mcx_gold(global_macro, vix_today, ohlcv_data)

    learning_summary = "No prior batches. First paper trade run."

    output = {
        "date": datetime.date.today().isoformat(),
        "run_time": datetime.datetime.now().strftime("%H:%M"),
        "run_id": (run_metadata or {}).get("run_id"),
        "schema_version": (run_metadata or {}).get("schema_version", "1.1"),
        "capital": capital,
        "phase": phase,
        "phase_limits": {
            "max_positions": phase_config.get('max_positions', 2),
            "allow_gold": phase_config.get('allow_gold', True)
        },
        "current_open_positions": len(open_positions),
        "market_context": market_context,
        "candidates": candidate_list,
        "watchlist_items": watchlist_items,
        "mcx_gold": mcx_gold,
        "learning_context_summary": learning_summary
    }

    with open("analysis_input.json", "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"analysis_input.json written — {len(candidate_list)} candidates")
    return output

def _get_sector(ticker, sector_data):
    if sector_data and isinstance(sector_data, dict) and ticker in sector_data:
        return sector_data[ticker]
    return TICKER_SECTOR_MAP.get(ticker)

def _sector_to_yf(sector_name):
    mapping = {
        "IT": "^CNXIT", "BANK": "^NSEBANK", "PHARMA": "^CNXPHARMA",
        "FMCG": "^CNXFMCG", "AUTO": "^CNXAUTO", "METAL": "^CNXMETAL",
        "ENERGY": "^CNXENERGY", "REALTY": "^CNXREALTY", "INFRA": "^CNXINFRA",
        "PSU_BANK": "^CNXPSUBANK", "DEFENCE": "^CNXDEFENCE", "HEALTHCARE": "^CNXHEALTH"
    }
    return mapping.get(sector_name)

def _get_sector_pe(sector, config):
    if not sector:
        return None
    return config.get('sector_pe_medians', {}).get(sector)

def _fetch_news_safe(ticker):
    try:
        from data.fetch import fetch_news
        return fetch_news(ticker)
    except:
        return []

def _fetch_bulk_deals_safe(ticker):
    try:
        from data.fetch import fetch_bulk_deals
        return fetch_bulk_deals(ticker)
    except:
        return []

def _build_mcx_gold(global_macro, vix_today, ohlcv_data):
    try:
        import yfinance as yf
        from data.indicators import compute_ema
        from data.fetch import _flatten_df
        gold_df = _flatten_df(yf.download("GOLD.MCX", period="3mo", auto_adjust=True, progress=False))
        if gold_df.empty:
            gold_df = _flatten_df(yf.download("GC=F", period="3mo", auto_adjust=True, progress=False))
        gold_price = float(gold_df['Close'].iloc[-1]) if not gold_df.empty else None
        gold_ema30 = float(compute_ema(gold_df['Close'], 30).iloc[-1]) if not gold_df.empty else None
        above_ema30 = bool(gold_price > gold_ema30) if gold_price and gold_ema30 else False
        dxy_price = global_macro.get('dxy_price')
        dxy_pct = global_macro.get('dxy')
        dxy_below_ema20 = bool(dxy_pct is not None and dxy_pct < 0)
        vix_above_18 = bool(vix_today and vix_today > 18)
        atr14 = None
        if not gold_df.empty and len(gold_df) >= 14:
            from data.indicators import compute_atr
            atr14 = round(float(compute_atr(gold_df, 14).iloc[-1]), 2)
        return {
            "price": round(gold_price, 2) if gold_price else None,
            "ema30": round(gold_ema30, 2) if gold_ema30 else None,
            "above_ema30": above_ema30,
            "dxy_below_ema20": dxy_below_ema20,
            "vix_above_18": vix_above_18,
            "atr14": atr14
        }
    except:
        return {
            "price": None, "ema30": None, "above_ema30": False,
            "dxy_below_ema20": False, "vix_above_18": False, "atr14": None
        }
