import pandas as pd
import numpy as np

def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def compute_atr(df, period=14):
    high = df['High']
    low = df['Low']
    close = df['Close']
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def compute_adx(df, period=14):
    high = df['High']
    low = df['Low']
    close = df['Close']
    plus_dm = high.diff()
    minus_dm = low.diff().abs()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(span=period, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(span=period, adjust=False).mean() / atr)
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di))
    adx = dx.ewm(span=period, adjust=False).mean()
    return adx

def compute_macd(series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def compute_all(df, ticker=""):
    if df is None or df.empty or len(df) < 60:
        return None
    df = df.copy()
    if hasattr(df.columns, 'nlevels') and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)
    df.columns = [c.capitalize() if isinstance(c, str) else str(c).capitalize() for c in df.columns]
    if 'Close' not in df.columns:
        return None
    close = df['Close']
    result = {}
    ema30 = compute_ema(close, 30)
    ema200 = compute_ema(close, 200)
    result['close'] = round(float(close.iloc[-1]), 2)
    result['ema30'] = round(float(ema30.iloc[-1]), 2)
    result['ema200'] = round(float(ema200.iloc[-1]), 2)
    result['ema30_slope_5d'] = round(float(ema30.iloc[-1] - ema30.iloc[-6]), 2)
    ema150 = compute_ema(close, 150)
    result['weekly_trend_up'] = bool(close.iloc[-1] > ema150.iloc[-1])
    rsi = compute_rsi(close, 14)
    result['rsi14'] = round(float(rsi.iloc[-1]), 2)
    adx = compute_adx(df, 14)
    result['adx14'] = round(float(adx.iloc[-1]), 2)
    atr = compute_atr(df, 14)
    result['atr14'] = round(float(atr.iloc[-1]), 2)
    macd_line, signal_line, histogram = compute_macd(close)
    result['macd_line'] = round(float(macd_line.iloc[-1]), 2)
    result['macd_signal'] = round(float(signal_line.iloc[-1]), 2)
    result['macd_histogram'] = round(float(histogram.iloc[-1]), 2)
    result['macd_histogram_prev'] = round(float(histogram.iloc[-2]), 2)
    if 'Volume' in df.columns:
        vol_today = float(df['Volume'].iloc[-1])
        vol_avg = float(df['Volume'].tail(20).mean())
        result['volume_ratio'] = round(vol_today / vol_avg if vol_avg > 0 else 0, 2)
    else:
        result['volume_ratio'] = None
    result['high_52w'] = round(float(close.tail(252).max()), 2)
    result['pct_of_52w_high'] = round((float(close.iloc[-1]) / result['high_52w']) * 100, 2)
    return result

def compute_rs_vs_nifty(stock_df, nifty_df, period=20):
    if stock_df is None or nifty_df is None:
        return None
    if stock_df.empty or nifty_df.empty:
        return None
    try:
        def _get_close(df):
            if hasattr(df.columns, 'nlevels') and df.columns.nlevels > 1:
                df = df.copy(); df.columns = df.columns.get_level_values(0)
            if 'Close' in df.columns: return df['Close']
            if 'close' in df.columns: return df['close']
            return None
        stock_close = _get_close(stock_df)
        nifty_close = _get_close(nifty_df)
        if stock_close is None or nifty_close is None:
            return None
        stock_ret = float(stock_close.pct_change(period).iloc[-1])
        nifty_ret = float(nifty_close.pct_change(period).iloc[-1])
        return round((stock_ret - nifty_ret) * 100, 2)
    except:
        return None

def compute_rs_vs_sector(stock_df, sector_df, period=20):
    if stock_df is None or sector_df is None:
        return None
    try:
        def _get_close(df):
            if hasattr(df.columns, 'nlevels') and df.columns.nlevels > 1:
                df = df.copy(); df.columns = df.columns.get_level_values(0)
            if 'Close' in df.columns: return df['Close']
            if 'close' in df.columns: return df['close']
            return None
        stock_close = _get_close(stock_df)
        sector_close = _get_close(sector_df)
        if stock_close is None or sector_close is None:
            return None
        stock_ret = float(stock_close.pct_change(period).iloc[-1])
        sector_ret = float(sector_close.pct_change(period).iloc[-1])
        return round((stock_ret - sector_ret) * 100, 2)
    except:
        return None
