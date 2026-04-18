import os
import json
import time
import datetime
import random
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup

NSE_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/"
}

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

def _normalize_symbol(raw):
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if not s or "NIFTY" in s or " " in s:
        return None
    # Common NSE series/suffix patterns returned by some endpoints: HDFCBANKEQN, XYZ-EQ
    for suffix in ("EQN", "-EQ", ".EQ", " EQ", "EQ"):
        if s.endswith(suffix) and len(s) > len(suffix):
            s = s[:-len(suffix)]
            break
    if s.endswith(".NS"):
        s = s[:-3]
    if s and all(ch.isalnum() or ch in {"&", "-"} for ch in s):
        return s
    return None

def _nse_json_get(url, timeout=15):
    try:
        with requests.Session() as sess:
            sess.headers.update(NSE_BROWSER_HEADERS)
            # Prime cookies/anti-bot context
            try:
                sess.get("https://www.nseindia.com", timeout=timeout)
            except Exception:
                pass
            resp = sess.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
    except Exception:
        return None

def _with_retries(fetch_fn, attempts=3, base_sleep=0.8):
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            val = fetch_fn()
            if val is not None:
                return val
        except Exception as e:
            last_err = e
        if attempt < attempts:
            time.sleep(base_sleep * attempt + random.uniform(0, 0.4))
    if last_err:
        raise last_err
    return None

def fetch_universe():
    if NSE_AVAILABLE:
        try:
            nifty50    = nse.get_index_details("NIFTY 50")
            next50     = nse.get_index_details("NIFTY NEXT 50")
            midcap150  = nse.get_index_details("NIFTY MIDCAP 150")
            smallcap250 = nse.get_index_details("NIFTY SMALLCAP 250")

            def _extract_symbols(df, limit=None):
                candidate_series = []
                # Prefer index first; for nsefin this is usually clean NSE symbols.
                candidate_series.append(pd.Series(df.index.astype(str)))
                for col in ['symbol', 'SYMBOL', 'Symbol', 'identifier', 'Identifier']:
                    if col in df.columns:
                        candidate_series.append(df[col].dropna().astype(str))
                # Then scan remaining columns
                for col in df.columns:
                    if col not in ['symbol', 'SYMBOL', 'Symbol', 'identifier', 'Identifier']:
                        candidate_series.append(df[col].dropna().astype(str))

                for vals in candidate_series:
                    normalized = vals.map(_normalize_symbol).dropna()
                    valid = normalized[normalized.str.match(r'^[A-Z0-9&\-]{1,20}$')]
                    valid = valid[~valid.str.contains("NIFTY", na=False)]
                    valid = valid.drop_duplicates()
                    if len(valid) >= 10:
                        result = pd.DataFrame({'symbol': valid.tolist()})
                        return result.head(limit) if limit else result
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
    # Guard against malformed symbols and normalize them before querying Yahoo.
    tickers = [
        _normalize_symbol(t) for t in [str(t) for t in tickers]
    ]
    tickers = [t for t in tickers if t]
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
            fii_fn = (
                getattr(nse, 'fii_dii', None)
                or getattr(nse, 'get_fii_dii', None)
                or getattr(nse, 'fii_dii_activity', None)
                or getattr(nse, 'get_fii_dii_activity', None)
            )
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
    # Direct NSE API fallback
    try:
        payload = _nse_json_get("https://www.nseindia.com/api/fiidiiTradeReact")
        if payload:
            if isinstance(payload, list):
                rows = payload
            elif isinstance(payload, dict):
                rows = payload.get("data") or payload.get("fiidiiTradeReact") or []
            else:
                rows = []
            if rows:
                df = pd.DataFrame(rows)
                # Prefer latest by date desc where available
                date_col = next((c for c in df.columns if 'date' in str(c).lower()), None)
                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                    df = df.sort_values(date_col, ascending=False)
                if 'category' in df.columns:
                    fii_rows = df[df['category'].astype(str).str.upper() == 'FII']
                    if not fii_rows.empty:
                        df = fii_rows
                flow_col = next(
                    (c for c in df.columns if 'fii' in str(c).lower() and 'net' in str(c).lower()),
                    None
                ) or next((c for c in df.columns if 'net' in str(c).lower()), None)
                if flow_col:
                    flow = pd.to_numeric(df.iloc[0][flow_col], errors='coerce')
                    if pd.notna(flow):
                        return {
                            "flow_crores": round(float(flow), 2),
                            "streak": None,
                            "direction": "buying" if float(flow) > 0 else "selling",
                            "source": "nse_api"
                        }
    except Exception as e:
        print(f"NSE API FII fallback failed: {e}")
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
        try:
            cached = pd.read_csv(cache_path)
            if cached is None or cached.empty or len(getattr(cached, "columns", [])) == 0:
                raise ValueError("cached bhavcopy is empty")
            return cached
        except Exception as e:
            print(f"Bhavcopy cache invalid ({cache_path}): {e}. Re-fetching.")
            try:
                os.remove(cache_path)
            except Exception:
                pass
    if NSE_AVAILABLE:
        try:
            bhav = nse.get_equity_bhav_copy(datetime.datetime.now())
            if bhav is None or bhav.empty:
                raise ValueError("nsefin returned empty bhavcopy")
            bhav.to_csv(cache_path, index=False)
            print("Bhavcopy fetched via nsefin")
            return bhav
        except Exception as e:
            print(f"nsefin bhavcopy failed: {e}")
    for i in range(0, 7):
        try:
            dt = today - datetime.timedelta(days=i)
            date_str = dt.strftime("%d%m%Y")
            url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
            df = pd.read_csv(url, dtype=str, storage_options={"User-Agent": "Mozilla/5.0"})
            if df is None or df.empty:
                continue
            # normalize columns by stripping spaces from legacy files
            df.columns = [str(c).strip() for c in df.columns]
            df.to_csv(cache_path, index=False)
            print(f"Bhavcopy fetched via NSE archive ({dt})")
            return df
        except Exception:
            continue
    print("Bhavcopy fetch failed for last 7 days. Delivery % will be skipped.")
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
        result = {
            "pe_ratio": None,
            "debt_equity": None,
            "revenue_q1": None,
            "revenue_q2": None,
            "revenue_q3": None,
            "revenue_q4": None,
            "data_date": None,
            "data_age_days": 999,
            "sources": [],
            "pe_candidates": [],
            "de_candidates": []
        }
        ticker_ns = ticker + ".NS"
        quarterly_values = []
        try:
            yt = yf.Ticker(ticker_ns)
            info = yt.info or {}
            if info.get("trailingPE") is not None:
                y_pe = float(info.get("trailingPE"))
                result["pe_ratio"] = y_pe
                result["pe_candidates"].append({"source": "yfinance", "value": y_pe})
            if info.get("debtToEquity") is not None:
                y_de = float(info.get("debtToEquity"))
                result["debt_equity"] = y_de
                result["de_candidates"].append({"source": "yfinance", "value": y_de})
            quarterly = yt.quarterly_financials
            if quarterly is not None and not quarterly.empty:
                rev_row = None
                for idx in quarterly.index:
                    idx_lower = str(idx).lower()
                    if 'revenue' in idx_lower or 'total revenue' in idx_lower:
                        rev_row = quarterly.loc[idx]
                        break
                if rev_row is not None:
                    quarterly_values = [
                        float(v) if pd.notna(v) else None
                        for v in rev_row.values[:4]
                    ]
            result["sources"].append("yfinance")
        except Exception:
            pass

        if len([v for v in quarterly_values if v is not None]) < 4:
            yahoo_qs = _fetch_yahoo_quote_summary(ticker_ns)
            qs_quarters = _extract_quarterly_revenues_from_qs(yahoo_qs)
            if len(qs_quarters) >= 4:
                quarterly_values = qs_quarters
            if yahoo_qs:
                stats = _get_qs_module(yahoo_qs, "defaultKeyStatistics")
                fin = _get_qs_module(yahoo_qs, "financialData")
                pe = _safe_float(_deep_get(stats, ["trailingPE", "raw"]))
                de = _safe_float(_deep_get(fin, ["debtToEquity", "raw"]))
                if pe is not None:
                    result["pe_candidates"].append({"source": "yahoo_quote_summary", "value": pe})
                if de is not None:
                    result["de_candidates"].append({"source": "yahoo_quote_summary", "value": de})
                if result["pe_ratio"] is None and pe is not None:
                    result["pe_ratio"] = pe
                if result["debt_equity"] is None and de is not None:
                    result["debt_equity"] = de
                result["sources"].append("yahoo_quote_summary")

        nse_quote = _fetch_nse_quote_equity(ticker)
        if nse_quote:
            metadata = nse_quote.get("metadata", {}) or {}
            pe = _safe_float(metadata.get("pdSectorPe"))
            if pe is not None:
                result["pe_candidates"].append({"source": "nse_quote_equity", "value": pe})
            if result["pe_ratio"] is None and pe is not None:
                result["pe_ratio"] = pe
            result["sources"].append("nse_quote_equity")

        if quarterly_values:
            result["revenue_q1"] = quarterly_values[0] if len(quarterly_values) > 0 else None
            result["revenue_q2"] = quarterly_values[1] if len(quarterly_values) > 1 else None
            result["revenue_q3"] = quarterly_values[2] if len(quarterly_values) > 2 else None
            result["revenue_q4"] = quarterly_values[3] if len(quarterly_values) > 3 else None

        required_present = (
            result["pe_ratio"] is not None and
            result["debt_equity"] is not None and
            all(result.get(k) is not None for k in ["revenue_q1", "revenue_q2", "revenue_q3", "revenue_q4"])
        )
        if required_present:
            result["data_date"] = datetime.date.today().isoformat()
            result["data_age_days"] = 0

        results[ticker] = result
        time.sleep(0.15)
    return results

def _safe_float(v):
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.replace(",", "").strip()
            if not v:
                return None
        fv = float(v)
        if pd.isna(fv):
            return None
        return fv
    except Exception:
        return None

def _fetch_yahoo_quote_summary(symbol_ns):
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol_ns}"
    params = {
        "modules": "assetProfile,financialData,defaultKeyStatistics,incomeStatementHistoryQuarterly"
    }
    try:
        def _go():
            r = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
            r.raise_for_status()
            data = r.json()
            return _deep_get(data, ["quoteSummary", "result", 0]) or {}
        return _with_retries(_go, attempts=3, base_sleep=0.7) or {}
    except Exception:
        return {}

def _get_qs_module(qs_payload, module_name):
    if not isinstance(qs_payload, dict):
        return {}
    v = qs_payload.get(module_name)
    return v if isinstance(v, dict) else {}

def _deep_get(data, path):
    cur = data
    for p in path:
        try:
            if isinstance(p, int):
                cur = cur[p]
            else:
                cur = cur.get(p)
        except Exception:
            return None
        if cur is None:
            return None
    return cur

def _extract_quarterly_revenues_from_qs(qs_payload):
    statements = _deep_get(qs_payload, ["incomeStatementHistoryQuarterly", "incomeStatementHistory"]) or []
    revs = []
    for st in statements:
        rev = _safe_float(_deep_get(st, ["totalRevenue", "raw"]))
        if rev is not None:
            revs.append(rev)
    return revs[:4]

def _fetch_nse_quote_equity(symbol):
    sym = _normalize_symbol(symbol)
    if not sym:
        return None
    url = f"https://www.nseindia.com/api/quote-equity?symbol={sym}"
    return _nse_json_get(url)

def _normalize_sector_name(sector_text):
    if not sector_text:
        return None
    s = str(sector_text).upper()
    mapping = [
        ("PSU BANK", "PSU_BANK"),
        ("BANK", "BANK"),
        ("FINANCIAL", "BANK"),
        ("NBFC", "BANK"),
        ("INSURANCE", "BANK"),
        ("INFORMATION TECHNOLOGY", "IT"),
        ("IT", "IT"),
        ("PHARMA", "PHARMA"),
        ("HEALTH", "HEALTHCARE"),
        ("FMCG", "FMCG"),
        ("CONSUMER", "FMCG"),
        ("AUTO", "AUTO"),
        ("METAL", "METAL"),
        ("STEEL", "METAL"),
        ("OIL", "ENERGY"),
        ("GAS", "ENERGY"),
        ("POWER", "ENERGY"),
        ("ENERGY", "ENERGY"),
        ("REALTY", "REALTY"),
        ("CEMENT", "INFRA"),
        ("INFRA", "INFRA"),
        ("CAPITAL GOODS", "INFRA"),
        ("DEFENCE", "DEFENCE"),
        ("AEROSPACE", "DEFENCE"),
        ("CHEMICAL", "CHEMICALS"),
        ("TEXTILE", "TEXTILES"),
        ("TELECOM", "TELECOM"),
        ("MEDIA", "MEDIA"),
        ("LOGISTICS", "LOGISTICS"),
        ("TRANSPORT", "LOGISTICS"),
        ("RETAIL", "CONSUMPTION"),
        ("CONSUMPTION", "CONSUMPTION"),
        ("CONSUMER DURABLE", "CONSUMPTION"),
        ("HOTEL", "HOSPITALITY"),
        ("TRAVEL", "HOSPITALITY"),
        ("CEMENT", "INFRA"),
        ("AGRI", "AGRI"),
        ("FERTILIZER", "AGRI"),
    ]
    for keyword, normalized in mapping:
        if keyword in s:
            return normalized
    return None

def fetch_sector_classification(tickers):
    sectors = {}
    unresolved = []
    for ticker in tickers:
        sector = None
        source = None
        nse_q = _fetch_nse_quote_equity(ticker)
        if nse_q:
            metadata = nse_q.get("metadata", {}) or {}
            info = nse_q.get("info", {}) or {}
            sector_text = metadata.get("industry") or info.get("industry") or info.get("companyName")
            sector = _normalize_sector_name(sector_text)
            if sector:
                source = "nse_quote_equity"
        if not sector:
            qs = _fetch_yahoo_quote_summary(ticker + ".NS")
            asset = _get_qs_module(qs, "assetProfile")
            sector = _normalize_sector_name(asset.get("sector"))
            if sector:
                source = "yahoo_quote_summary"
        if not sector:
            try:
                info = yf.Ticker(ticker + ".NS").info or {}
                sector = _normalize_sector_name(info.get("sector"))
                if sector:
                    source = "yfinance"
            except Exception:
                pass
        if sector:
            sectors[ticker] = {"sector": sector, "source": source}
        else:
            # Graceful fallback to avoid full-run hard failure on unknown taxonomy.
            sectors[ticker] = {"sector": "MISC", "source": "fallback_misc"}
            unresolved.append(ticker)
        time.sleep(0.1)
    return sectors, unresolved

def validate_candidate_data_completeness(
    candidates,
    fundamentals,
    dynamic_sectors,
    require_delivery=True,
    require_sector=True
):
    blockers = []
    sector_lookup = {}
    for ticker, payload in (dynamic_sectors or {}).items():
        if isinstance(payload, dict):
            sector_lookup[ticker] = payload.get("sector")
        else:
            sector_lookup[ticker] = payload
    for c in candidates:
        ticker = c.get("ticker")
        fund = fundamentals.get(ticker, {})
        missing = []
        if not ticker:
            continue
        if require_delivery and c.get("delivery_pct") is None:
            missing.append("delivery_pct")
        if require_sector and not sector_lookup.get(ticker):
            missing.append("sector")
        if fund.get("pe_ratio") is None:
            missing.append("pe_ratio")
        if fund.get("debt_equity") is None:
            missing.append("debt_equity")
        for q in ["revenue_q1", "revenue_q2", "revenue_q3", "revenue_q4"]:
            if fund.get(q) is None:
                missing.append(q)
        if missing:
            blockers.append({"ticker": ticker, "missing": missing})
    return blockers

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
