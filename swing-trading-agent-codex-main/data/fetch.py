import os
import json
import time
import datetime
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup

def _flatten_df(df):
    """Flatten yfinance 1.x MultiIndex columns to single level."""
    if df is None or df.empty:
        return df
    if hasattr(df.columns, 'nlevels') and df.columns.nlevels > 1:
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df

try:
    import nsefin
    nse = nsefin.NSEClient()
    NSE_AVAILABLE = True
except:
    NSE_AVAILABLE = False

SECTOR_TICKERS = {
    "IT": "^CNXIT",
    "BANK": "^NSEBANK",
    "PHARMA": "^CNXPHARMA",
    "FMCG": "^CNXFMCG",
    "AUTO": "^CNXAUTO",
    "METAL": "^CNXMETAL",
    "ENERGY": "^CNXENERGY",
    "REALTY": "^CNXREALTY",
    "INFRA": "^CNXINFRA",
    "PSU_BANK": "^CNXPSUBANK",
    # ^CNXDEFENCE and ^CNXHEALTH are not available on Yahoo Finance
}

os.makedirs("data/cache", exist_ok=True)

def fetch_universe():
    if NSE_AVAILABLE:
        try:
            nifty50    = nse.get_index_details("NIFTY 50")
            next50     = nse.get_index_details("NIFTY NEXT 50")
            midcap150  = nse.get_index_details("NIFTY MIDCAP 150")
            smallcap250 = nse.get_index_details("NIFTY SMALLCAP 250")

            def _extract_symbols(df, limit=None):
                # Try each column — look for one that has >10 string values
                for col in df.columns:
                    vals = df[col].dropna().astype(str)
                    # Valid symbols: uppercase letters 1-20 chars, no spaces
                    valid = vals[vals.str.match(r'^[A-Z&\-\.]{1,20}$')]
                    if len(valid) >= 10:
                        result = pd.DataFrame({'symbol': valid.tolist()})
                        return result.head(limit) if limit else result
                # Try the index itself
                idx_vals = pd.Series(df.index.astype(str))
                valid = idx_vals[idx_vals.str.match(r'^[A-Z&\-\.]{1,20}$')]
                if len(valid) >= 10:
                    return pd.DataFrame({'symbol': valid.tolist()})
                raise ValueError(f"Could not extract symbols. Columns: {list(df.columns)}, sample: {df.head(2).to_dict()}")

            parts = [
                _extract_symbols(nifty50),
                _extract_symbols(next50),
                _extract_symbols(midcap150, limit=100),
                _extract_symbols(smallcap250, limit=100),
            ]
            all_tickers = pd.concat(parts).drop_duplicates(subset='symbol')
            if len(all_tickers) < 100:
                raise ValueError(f"nsefin only returned {len(all_tickers)} valid symbols — too few")
            print(f"Universe fetched via nsefin: {len(all_tickers)} stocks")
            return all_tickers
        except Exception as e:
            print(f"nsefin universe fetch failed: {e}. Using fallback.")
    from universe_fallback import NIFTY50, NIFTY_NEXT50, MIDCAP_TOP100, SMALLCAP_TOP100
    all_symbols = list(set(NIFTY50 + NIFTY_NEXT50 + MIDCAP_TOP100 + SMALLCAP_TOP100))
    df = pd.DataFrame({'symbol': all_symbols})
    print(f"Universe loaded from fallback: {len(df)} stocks")
    return df

def fetch_ohlcv_batch(tickers, period="18mo"):
    tickers = [str(t) for t in tickers]  # guard against non-string symbols
    results = {}
    chunks = [tickers[i:i+50] for i in range(0, len(tickers), 50)]
    for idx, chunk in enumerate(chunks):
        ns_tickers = [t + ".NS" for t in chunk]
        try:
            data = yf.download(
                ns_tickers, period=period,
                auto_adjust=True, group_by='ticker',
                threads=True, progress=False
            )
            for t in chunk:
                ns_t = t + ".NS"
                try:
                    if len(chunk) == 1:
                        results[t] = data
                    elif not hasattr(data.columns, 'nlevels') or data.columns.nlevels < 2:
                        results[t] = data
                    else:
                        lvl0 = data.columns.get_level_values(0).unique()
                        lvl1 = data.columns.get_level_values(1).unique()
                        if ns_t in lvl0:
                            results[t] = data[ns_t]
                        elif ns_t in lvl1:
                            results[t] = data.xs(ns_t, level=1, axis=1)
                        elif t in lvl0:
                            results[t] = data[t]
                except:
                    pass
            time.sleep(2)
        except Exception as e:
            print(f"Chunk {idx+1} batch failed: {e}. Retrying individually.")
            for t in chunk:
                try:
                    d = yf.download(t + ".NS", period=period, auto_adjust=True, progress=False)
                    if not d.empty:
                        results[t] = d
                    time.sleep(0.5)
                except:
                    pass
    print(f"OHLCV fetched for {len(results)}/{len(tickers)} stocks")
    return results

def fetch_fii_data():
    if NSE_AVAILABLE:
        try:
            fii_fn = getattr(nse, 'fii_dii', None) or getattr(nse, 'get_fii_dii', None) or getattr(nse, 'fii_dii_activity', None)
            if fii_fn is None:
                raise AttributeError("No FII method found in nsefin")
            fii_df = fii_fn()
            date_col = next((c for c in fii_df.columns if 'date' in str(c).lower()), fii_df.columns[0])
            fii_df = fii_df.sort_values(date_col, ascending=False).head(5)
            flow_col = next((c for c in fii_df.columns if 'fii' in str(c).lower() and 'net' in str(c).lower()), None) or next((c for c in fii_df.columns if 'fii' in str(c).lower()), None)
            if flow_col is None:
                raise KeyError("FII net value column not found")
            flows = fii_df[flow_col].tolist()
            streak = 0
            direction = "neutral"
            for f in flows:
                if f > 0:
                    if direction in ("neutral", "buying"):
                        direction = "buying"
                        streak += 1
                    else:
                        break
                elif f < 0:
                    if direction in ("neutral", "selling"):
                        direction = "selling"
                        streak += 1
                    else:
                        break
                else:
                    break
            return {
                "flow_crores": round(float(flows[0]), 2) if flows else None,
                "streak": streak,
                "direction": direction,
                "source": "nsefin"
            }
        except Exception as e:
            print(f"nsefin FII failed: {e}")
    try:
        url = "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 3 and "FII" in cells[0].text:
                    flow = cells[2].text.strip().replace(",", "")
                    return {
                        "flow_crores": float(flow) if flow else None,
                        "streak": None,
                        "direction": "buying" if float(flow or 0) > 0 else "selling",
                        "source": "moneycontrol"
                    }
    except Exception as e:
        print(f"Moneycontrol FII scrape failed: {e}")
    return {
        "flow_crores": None,
        "streak": None,
        "direction": "UNKNOWN",
        "source": "FAILED",
        "note": "FII data unavailable — context set NEUTRAL"
    }

def fetch_bhavcopy():
    today = datetime.date.today()
    cache_path = f"data/cache/bhavcopy_{today}.csv"
    if os.path.exists(cache_path):
        return pd.read_csv(cache_path)
    if NSE_AVAILABLE:
        try:
            bhav = nse.get_equity_bhav_copy(datetime.datetime.now())
            bhav.to_csv(cache_path, index=False)
            print("Bhavcopy fetched via nsefin")
            return bhav
        except Exception as e:
            print(f"nsefin bhavcopy failed: {e}")
    try:
        date_str = today.strftime("%d%m%Y")
        url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
        headers = {"User-Agent": "Mozilla/5.0"}
        df = pd.read_csv(url, dtype=str, storage_options={"User-Agent": "Mozilla/5.0"})
        df.to_csv(cache_path, index=False)
        print("Bhavcopy fetched via NSE archive")
        return df
    except Exception as e:
        print(f"Bhavcopy fetch failed: {e}. Delivery % will be skipped.")
        return None

def fetch_results_calendar():
    if NSE_AVAILABLE:
        try:
            actions = nse.get_corporate_actions()
            print(f"      Corporate actions columns: {list(actions.columns)}")
            today = datetime.date.today()
            cutoff = today + datetime.timedelta(days=10)
            date_col = next((c for c in ['exDate', 'recDate', 'bcStartDate'] if c in actions.columns),
                            next((c for c in actions.columns if 'date' in str(c).lower()), None))
            sym_col = next((c for c in ['symbol', 'Symbol', 'SYMBOL'] if c in actions.columns),
                           next((c for c in actions.columns if 'symbol' in str(c).lower()), None))
            action_col = next((c for c in ['subject', 'action', 'purpose'] if c in actions.columns),
                              next((c for c in actions.columns if any(k in str(c).lower() for k in ('subject','action','purpose'))), None))
            if date_col is None or sym_col is None:
                raise KeyError(f"date/symbol columns not found. Available: {list(actions.columns)}")
            actions[date_col] = pd.to_datetime(actions[date_col], errors='coerce').dt.date
            actions = actions.dropna(subset=[date_col])
            if action_col:
                upcoming = actions[
                    (actions[date_col] >= today) &
                    (actions[date_col] <= cutoff) &
                    (actions[action_col].str.contains('Result', case=False, na=False))
                ]
            else:
                upcoming = actions[(actions[date_col] >= today) & (actions[date_col] <= cutoff)]
            tickers = set(upcoming[sym_col].tolist())
            print(f"Results calendar: {len(tickers)} stocks reporting in 10 days")
            return tickers
        except Exception as e:
            print(f"Results calendar fetch failed: {e}")
    return set()

def fetch_sector_rs():
    try:
        nifty = _flatten_df(yf.download("^NSEI", period="3mo", auto_adjust=True, progress=False))
        nifty_ret_20d = float(nifty['Close'].pct_change(20).iloc[-1])
        nifty_ret_40d = float(nifty['Close'].pct_change(40).iloc[-1])
        sectors = {}
        for name, ticker in SECTOR_TICKERS.items():
            try:
                data = _flatten_df(yf.download(ticker, period="3mo", auto_adjust=True, progress=False))
                if data.empty:
                    continue
                rs_20d = float(data['Close'].pct_change(20).iloc[-1]) - nifty_ret_20d
                rs_4w_ago = float(data['Close'].pct_change(20).iloc[-6]) - nifty_ret_20d
                rs_trend = rs_20d - rs_4w_ago
                if rs_20d > 0 and rs_trend > 0:
                    quadrant = "LEADING"
                elif rs_20d < 0 and rs_trend > 0:
                    quadrant = "IMPROVING"
                elif rs_20d > 0 and rs_trend < 0:
                    quadrant = "WEAKENING"
                else:
                    quadrant = "LAGGING"
                sectors[name] = {
                    "quadrant": quadrant,
                    "rs_20d": round(rs_20d * 100, 2),
                    "rs_trend": round(rs_trend * 100, 2)
                }
                time.sleep(0.3)
            except:
                pass
        print(f"Sector RS computed for {len(sectors)}/12 sectors")
        return sectors
    except Exception as e:
        print(f"Sector RS fetch failed: {e}")
        return {}

def get_vix_avg():
    cache = "data/cache/vix_avg.json"
    if os.path.exists(cache):
        data = json.load(open(cache))
        last_updated = datetime.date.fromisoformat(data["updated"])
        if (datetime.date.today() - last_updated).days < 7:
            return float(data["avg"])
    try:
        vix_hist = yf.download("^INDIAVIX", period="15mo", progress=False)
        vix_hist = _flatten_df(vix_hist)
        avg = float(vix_hist["Close"].tail(252).mean())
        json.dump(
            {"avg": round(avg, 2), "updated": str(datetime.date.today())},
            open(cache, "w")
        )
        return avg
    except Exception as e:
        print(f"VIX avg fetch failed: {e}")
        return 15.0

def fetch_fundamentals(tickers):
    results = {}
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker + ".NS").info
            quarterly = yf.Ticker(ticker + ".NS").quarterly_financials
            revenues = []
            if quarterly is not None and not quarterly.empty:
                rev_row = None
                for idx in quarterly.index:
                    if 'revenue' in str(idx).lower() or 'total revenue' in str(idx).lower():
                        rev_row = quarterly.loc[idx]
                        break
                if rev_row is not None:
                    revenues = [float(v) if v and str(v) != 'nan' else None
                                for v in rev_row.values[:4]]
            data_date = datetime.date.today().isoformat()
            results[ticker] = {
                "pe_ratio": info.get("trailingPE"),
                "debt_equity": info.get("debtToEquity"),
                "revenue_q1": revenues[0] if len(revenues) > 0 else None,
                "revenue_q2": revenues[1] if len(revenues) > 1 else None,
                "revenue_q3": revenues[2] if len(revenues) > 2 else None,
                "revenue_q4": revenues[3] if len(revenues) > 3 else None,
                "data_date": data_date,
                "data_age_days": 0
            }
            time.sleep(0.3)
        except Exception as e:
            results[ticker] = {
                "pe_ratio": None, "debt_equity": None,
                "revenue_q1": None, "revenue_q2": None,
                "revenue_q3": None, "revenue_q4": None,
                "data_date": None, "data_age_days": 999
            }
    return results

def fetch_news(ticker):
    try:
        import xml.etree.ElementTree as ET
        url = f"https://news.google.com/rss/search?q={ticker}+NSE+India&hl=en-IN&gl=IN&ceid=IN:en"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        root = ET.fromstring(r.text)
        headlines = []
        for item in root.findall('.//item')[:5]:
            title = item.findtext('title', '')
            pub = item.findtext('pubDate', '')[:16]
            headlines.append(f"{title} — {pub}")
        return headlines
    except:
        return []

def fetch_bulk_deals(ticker):
    if NSE_AVAILABLE:
        try:
            deals = nse.get_bulk_deals()
            today = datetime.date.today()
            week_ago = today - datetime.timedelta(days=7)
            deals['date'] = pd.to_datetime(deals['date']).dt.date
            stock_deals = deals[
                (deals['symbol'] == ticker) &
                (deals['date'] >= week_ago)
            ]
            result = []
            for _, row in stock_deals.iterrows():
                result.append(f"{row.get('client_name','Unknown')} {row.get('buy_sell','?')} "
                              f"{row.get('quantity_traded','?')} shares at "
                              f"{row.get('trade_price','?')} — {row['date']}")
            return result
        except:
            return []
    return []

def fetch_global_macro():
    tickers = {
        "sp500": "^GSPC",
        "nasdaq": "^IXIC",
        "nikkei": "^N225",
        "hangseng": "^HSI",
        "dxy": "DX-Y.NYB",
        "brent": "BZ=F",
        "gold": "GC=F",
        "us10y": "^TNX"
    }
    result = {}
    for name, ticker in tickers.items():
        try:
            data = _flatten_df(yf.download(ticker, period="5d", auto_adjust=True, progress=False))
            if not data.empty and len(data) >= 2:
                prev = float(data['Close'].iloc[-2])
                curr = float(data['Close'].iloc[-1])
                result[name] = round(((curr - prev) / prev) * 100, 2)
                result[f"{name}_price"] = round(curr, 2)
            time.sleep(0.3)
        except:
            result[name] = None
    return result

def fetch_advance_decline():
    try:
        nifty500 = yf.download(
            "^CRSLDX", period="5d", auto_adjust=True, progress=False
        )
        return 1.0
    except:
        return 1.0

def check_shadow_book_outcomes(db, config):
    import datetime
    cutoff = datetime.date.today() - datetime.timedelta(
        days=config.get('shadow_book_tracking_days', 15)
    )
    abandoned = db.get_unchecked_abandoned_signals(older_than=cutoff)
    for signal in abandoned:
        try:
            data = _flatten_df(yf.download(
                signal['ticker'] + ".NS", period="1d", progress=False
            ))
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
            db.update_signal_outcome(signal['id'], outcome)
        except:
            pass

def test_fetch():
    print("Running fetch test on 10 stocks...")
    from universe_fallback import NIFTY50
    test_tickers = NIFTY50[:10]
    print(f"Testing OHLCV for: {test_tickers}")
    ohlcv = fetch_ohlcv_batch(test_tickers, period="1mo")
    print(f"OHLCV: {len(ohlcv)}/10 fetched")
    fii = fetch_fii_data()
    print(f"FII: source={fii['source']}, direction={fii['direction']}, flow={fii['flow_crores']}")
    macro = fetch_global_macro()
    print(f"Global macro: SP500={macro.get('sp500')}%, DXY={macro.get('dxy_price')}")
    vix_avg = get_vix_avg()
    print(f"VIX 52wk avg: {vix_avg}")
    print("Fetch test complete.")
