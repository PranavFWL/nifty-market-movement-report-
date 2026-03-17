"""
nifty_report.py
───────────────
1. Reads nifty_data.csv (daily bars, exported from Colab)
2. Aggregates daily → weekly and monthly OHLC
3. Computes range metrics and gap stats
4. Saves data.json
5. Starts local server and opens browser

Usage:
    python nifty_report.py
"""

import json, math, time, threading, webbrowser, http.server, socketserver, os
import pandas as pd
from pathlib import Path

CSV_FILE = "nifty_data.csv"
OUTPUT   = "data.json"
PORT     = 8000

METRICS = ['max_range_pct', 'closing_range_pct', 'upper_range_pct', 'lower_range_pct']

def clean(obj):
    if isinstance(obj, list): return [clean(i) for i in obj]
    if isinstance(obj, dict): return {k: clean(v) for k, v in obj.items()}
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)): return None
    return obj

def load_data():
    p = Path(CSV_FILE)
    if not p.exists():
        raise FileNotFoundError(f"'{CSV_FILE}' not found. Run Colab first.\n{Path.cwd()}")
    print(f"Reading {CSV_FILE}...")
    df = pd.read_csv(p)
    df['date'] = pd.to_datetime(df['date'])
    df = df.dropna(subset=['open','high','low','close'])
    df = df[(df['high'] > df['low']) & (df['open'] > 0)]
    df = df.sort_values('date').reset_index(drop=True)
    print(f"  Loaded {len(df)} daily rows  ({df['date'].min().date()} → {df['date'].max().date()})")
    return df

def aggregate(df, freq):
    df = df.set_index('date')
    agg = df.groupby(pd.Grouper(freq=freq)).agg(
        open=('open','first'), high=('high','max'),
        low=('low','min'),     close=('close','last'),
    ).dropna().reset_index()
    return agg[(agg['high'] > agg['low']) & (agg['open'] > 0)]

def compute_pct(df):
    df = df.copy()
    df['max_range_pct']     = ((df['high'] - df['low'])          / df['open'] * 100).round(3)
    df['closing_range_pct'] = ((df['open'] - df['close']).abs()  / df['open'] * 100).round(3)
    df['upper_range_pct']   = ((df['high'] - df['open'])         / df['open'] * 100).round(3)
    df['lower_range_pct']   = ((df['open'] - df['low'])          / df['open'] * 100).round(3)
    return df

def yearly_stats(df, date_col):
    df = df.copy()
    df['year'] = pd.to_datetime(df[date_col]).dt.year
    rows = []
    for year, grp in df.groupby('year'):
        row = {'year': int(year)}
        for m in METRICS:
            row[m + '_avg']  = round(float(grp[m].mean()), 3)
            row[m + '_high'] = round(float(grp[m].max()),  3)
            row[m + '_low']  = round(float(grp[m].min()),  3)
        rows.append(row)
    return rows

def period_series(df, date_col, label_fmt):
    df = df.copy()
    df['label'] = pd.to_datetime(df[date_col]).dt.strftime(label_fmt)
    df['ym']    = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m')
    df['dt']    = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
    return df[['dt','ym','label'] + METRICS].to_dict(orient='records')

def gap_stats(daily):
    """
    gap_pct = (today open - yesterday close) / yesterday close * 100
    Positive = gap up, Negative = gap down.
    Returns (series, yearly) — a clean tuple, nothing else.
    """
    df = daily.sort_values('date').copy()
    df['prev_close'] = df['close'].shift(1)
    df = df.dropna(subset=['prev_close'])
    df['gap_pct'] = ((df['open'] - df['prev_close']) / df['prev_close'] * 100).round(3)
    df['year']    = df['date'].dt.year
    df['dt']      = df['date'].dt.strftime('%Y-%m-%d')
    df['label']   = df['date'].dt.strftime('%d %b %y')
    df['ym']      = df['date'].dt.strftime('%Y-%m')

    series = df[['dt','ym','label','gap_pct']].to_dict(orient='records')

    yearly = []
    for yr, grp in df.groupby('year'):
        yearly.append({
            'year': int(yr),
            'avg': round(float(grp['gap_pct'].mean()), 3),
            'max': round(float(grp['gap_pct'].max()),  3),
            'min': round(float(grp['gap_pct'].min()),  3),
        })

    return series, yearly

def build_payload(daily):
    daily_c          = compute_pct(daily.copy())
    weekly           = compute_pct(aggregate(daily, 'W-FRI'))
    monthly          = compute_pct(aggregate(daily, 'MS'))
    gap_ser, gap_yr  = gap_stats(daily)

    return {
        'meta': {
            'symbol':      'NIFTY',
            'daily_rows':  int(len(daily)),
            'from':        str(daily['date'].min().date()),
            'to':          str(daily['date'].max().date()),
            'total_years': int((daily['date'].max() - daily['date'].min()).days // 365),
        },
        'daily_yearly':   yearly_stats(daily_c, 'date'),
        'weekly_yearly':  yearly_stats(weekly,  'date'),
        'monthly_yearly': yearly_stats(monthly, 'date'),
        'daily_series':   period_series(daily_c, 'date', '%d %b %y'),
        'weekly_series':  period_series(weekly,  'date', '%d %b %y'),
        'monthly_series': period_series(monthly, 'date', '%b %Y'),
        'gap_series':     gap_ser,
        'gap_yearly':     gap_yr,
    }

def start_server(port):
    os.chdir(Path(__file__).parent)
    handler = http.server.SimpleHTTPRequestHandler
    handler.log_message = lambda *a: None
    with socketserver.TCPServer(('', port), handler) as httpd:
        print(f"  Dashboard → http://localhost:{port}")
        print("  Press Ctrl+C to stop.\n")
        httpd.serve_forever()

if __name__ == '__main__':
    daily   = load_data()
    payload = build_payload(daily)

    out = Path(OUTPUT)
    out.write_text(json.dumps(clean(payload), indent=2), encoding='utf-8')
    print(f"\nSaved → {out.resolve()}")
    print(f"  Gap series rows  : {len(payload['gap_series'])}")
    print(f"  Gap yearly rows  : {len(payload['gap_yearly'])}")
    print(f"  Weekly yearly    : {len(payload['weekly_yearly'])}")
    print(f"  Monthly yearly   : {len(payload['monthly_yearly'])}")

    t = threading.Thread(target=start_server, args=(PORT,), daemon=True)
    t.start()
    time.sleep(1)
    webbrowser.open(f'http://localhost:{PORT}')

    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print('\nServer has been stopped.')
