import pandas as pd
import numpy as np
from data.indicators import compute_all, compute_rs_vs_nifty

def _norm_symbol(s):
    if s is None:
        return None
    norm = str(s).strip().upper().replace(".NS", "").replace("-EQ", "")
    norm = "".join(ch for ch in norm if ch.isalnum())
    return norm or None

def _build_bhavcopy_map(bhavcopy):
    out = {}
    if bhavcopy is None or len(bhavcopy) == 0:
        return out
    cols = {str(c).strip().upper(): c for c in bhavcopy.columns}
    sym_col = cols.get("SYMBOL")
    deliv_col = cols.get("DELIV_PER") or cols.get("DELIV PER") or cols.get("DELIVERABLE %")
    if sym_col is None or deliv_col is None:
        return out
    for _, row in bhavcopy.iterrows():
        sym = _norm_symbol(row.get(sym_col))
        if not sym:
            continue
        try:
            out[sym] = float(row.get(deliv_col))
        except Exception:
            continue
    return out

def detect_vcp(df, lookback=40):
    if df is None or df.empty or len(df) < lookback:
        return {"detected": False, "contractions": 0, "final_range_pct": 0.0}
    try:
        close = df['Close'] if 'Close' in df.columns else df['close']
        volume = df['Volume'] if 'Volume' in df.columns else None
        recent = close.tail(lookback).values
        contractions = []
        i = 1
        while i < len(recent) - 1:
            if recent[i] < recent[i-1]:
                start_high = recent[i-1]
                low = recent[i]
                j = i + 1
                while j < len(recent) and recent[j] < start_high:
                    if recent[j] < low:
                        low = recent[j]
                    j += 1
                recovery = recent[j-1] if j < len(recent) else recent[-1]
                depth_pct = (start_high - low) / start_high * 100
                contractions.append({
                    "start_idx": i-1,
                    "low_idx": i,
                    "end_idx": j-1,
                    "depth_pct": depth_pct
                })
                i = j
            else:
                i += 1
        if len(contractions) < 2:
            return {"detected": False, "contractions": len(contractions), "final_range_pct": 0.0}
        depths_declining = all(
            contractions[k+1]['depth_pct'] < contractions[k]['depth_pct']
            for k in range(len(contractions)-1)
        )
        if not depths_declining:
            return {"detected": False, "contractions": len(contractions), "final_range_pct": 0.0}
        final_range_pct = 0.0
        if volume is not None:
            vol_vals = volume.tail(lookback).values
            last_contraction = contractions[-1]
            c1_vols = vol_vals[contractions[0]['start_idx']:contractions[0]['end_idx']+1]
            c2_vols = vol_vals[last_contraction['start_idx']:last_contraction['end_idx']+1]
            vol_declining = (c2_vols.mean() < c1_vols.mean()) if len(c1_vols) > 0 and len(c2_vols) > 0 else False
        else:
            vol_declining = True
        final_window = recent[-10:]
        final_range_pct = ((final_window.max() - final_window.min()) / recent[-1]) * 100
        current_price = recent[-1]
        final_range_ok = final_range_pct < 8.0
        if vol_declining and final_range_ok and len(contractions) >= 2:
            return {"detected": True, "contractions": len(contractions), "final_range_pct": round(final_range_pct, 2)}
        return {"detected": False, "contractions": len(contractions), "final_range_pct": round(final_range_pct, 2)}
    except:
        return {"detected": False, "contractions": 0, "final_range_pct": 0.0}

def run_prefilter(ohlcv_data, nifty_df, results_blackout, bhavcopy, watchlist_tickers, config, tier_map):
    print("Pre-filtering stocks...")
    universe_mode = str(config.get("universe_mode", "full")).lower()
    strict_prefilter = universe_mode == "prefiltered"
    candidates = []
    skipped = 0
    bhav_map = _build_bhavcopy_map(bhavcopy)
    fc = {
        'no_data': 0, 'bad_indicators': 0, 'below_ema30': 0,
        'ema30_not_rising': 0, 'not_stage2': 0, 'rsi': 0,
        'low_volume': 0, 'near_52w_high': 0, 'weak_rs': 0, 'low_adx': 0
    }
    for ticker, df in ohlcv_data.items():
        if df is None or df.empty or len(df) < 60:
            skipped += 1; fc['no_data'] += 1
            continue
        if ticker in results_blackout:
            skipped += 1
            continue
        indicators = compute_all(df, ticker)
        if indicators is None:
            skipped += 1; fc['bad_indicators'] += 1
            continue
        close = indicators['close']
        ema30 = indicators['ema30']
        ema200 = indicators['ema200']
        ema30_slope = indicators['ema30_slope_5d']
        rsi = indicators['rsi14']
        adx = indicators['adx14']
        volume_ratio = indicators.get('volume_ratio', 0) or 0
        high_52w = indicators['high_52w']
        tier = tier_map.get(ticker, 3)
        import math
        if any(math.isnan(x) for x in [close, ema30, ema200, rsi, adx] if x is not None):
            fc['bad_indicators'] += 1; continue
        if strict_prefilter and close <= ema30:
            fc['below_ema30'] += 1; continue
        if strict_prefilter and ema30_slope <= 0:
            fc['ema30_not_rising'] += 1; continue
        if strict_prefilter and ema30 <= ema200:
            fc['not_stage2'] += 1; continue
        if strict_prefilter and not (45 <= rsi <= 72):
            fc['rsi'] += 1; continue
        if strict_prefilter and volume_ratio < 0.8:
            fc['low_volume'] += 1; continue
        pct_from_high = (close / high_52w) * 100
        if strict_prefilter and pct_from_high >= 98:
            fc['near_52w_high'] += 1; continue
        rs_vs_nifty = compute_rs_vs_nifty(df, nifty_df, 20) or 0
        if strict_prefilter and rs_vs_nifty < -3:
            fc['weak_rs'] += 1; continue
        if strict_prefilter and adx < 15:
            fc['low_adx'] += 1; continue
        if strict_prefilter and tier == 4:
            if volume_ratio < 2.0:
                continue
            if adx < 25:
                continue
            if bhavcopy is not None:
                del_pct = bhav_map.get(_norm_symbol(ticker))
                if del_pct is not None and del_pct < 45:
                    continue
        delivery_pct = bhav_map.get(_norm_symbol(ticker))
        vcp = detect_vcp(df)
        candidates.append({
            "ticker": ticker,
            "tier": tier,
            "rs_vs_nifty_20d": rs_vs_nifty,
            "delivery_pct": delivery_pct,
            "vcp": vcp,
            "indicators": indicators
        })
    candidates.sort(key=lambda x: x['rs_vs_nifty_20d'], reverse=True)
    top_candidates = candidates if not strict_prefilter else candidates[:25]
    for wl in watchlist_tickers:
        if wl not in [c['ticker'] for c in top_candidates]:
            if wl in ohlcv_data and wl not in results_blackout:
                df = ohlcv_data[wl]
                if df is not None and not df.empty:
                    indicators = compute_all(df, wl)
                    if indicators:
                        delivery_pct = bhav_map.get(_norm_symbol(wl))
                        vcp = detect_vcp(df)
                        top_candidates.append({
                            "ticker": wl,
                            "tier": tier_map.get(wl, 3),
                            "rs_vs_nifty_20d": compute_rs_vs_nifty(df, nifty_df, 20) or 0,
                            "delivery_pct": delivery_pct,
                            "vcp": vcp,
                            "indicators": indicators,
                            "from_watchlist": True
                        })
    total_in = len(ohlcv_data)
    print(f"Pre-filter complete: {len(top_candidates)} candidates from {total_in} stocks")
    print(f"  Filter breakdown: no_data={fc['no_data']} bad_indicators={fc['bad_indicators']} "
          f"below_ema30={fc['below_ema30']} ema30_not_rising={fc['ema30_not_rising']} "
          f"not_stage2={fc['not_stage2']} rsi={fc['rsi']} low_volume={fc['low_volume']} "
          f"near_52w_high={fc['near_52w_high']} weak_rs={fc['weak_rs']} low_adx={fc['low_adx']}")
    return top_candidates
