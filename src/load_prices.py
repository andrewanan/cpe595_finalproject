import pandas as pd
from pathlib import Path

RAW = Path(__file__).resolve().parents[1] /"data"/"raw"


def load_btc_daily(): #1d btc daily prices
    df = pd.read_csv(RAW / "btcusd_1-min_data.csv", parse_dates=["timestamp"])
    df = df.rename(columns={"timestamp": "Date", "close": "Close"})
    df = df.set_index("Date").sort_index()
    daily = df["Close"].resample("1D").last().to_frame("Close")
    return daily

def load_eth_daily():
    df = pd.read_csv(RAW / "ETH_day.csv", parse_dates=["Date"])
    df = df.set_index("Date").sort_index()
    if "Close" not in df.columns:
        df = df.rename(columns={df.columns[1]: "Close"})  # fallback
    return df[["Close"]]

def resample_csv(path, time_col, price_col, freq="1D"):
    df = pd.read_csv(RAW / path, parse_dates=[time_col])
    df = df.rename(columns={time_col: "Date", price_col: "Close"})
    df = df.set_index("Date").sort_index()
    return df["Close"].resample(freq).last().to_frame("Close")
