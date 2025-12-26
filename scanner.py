import os
import datetime as dt
import requests
import pandas as pd

# -------- Secrets --------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
FINMIND_TOKEN = os.getenv("FINMIND_TOKEN", "")

# -------- Telegram --------
def send_telegram(msg: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=20)
    print("Telegram status:", r.status_code)
    print("Telegram response:", r.text)
    r.raise_for_status()

# -------- Time (Taipei) --------
def today_tpe() -> dt.date:
    return (dt.datetime.utcnow() + dt.timedelta(hours=8)).date()

def fmt(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

# -------- TWSE snapshot (prefilter) --------
TWSE_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"

def load_twse_snapshot() -> pd.DataFrame:
    r = requests.get(TWSE_ALL, timeout=30)
    print("TWSE status:", r.status_code)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    print("TWSE columns:", list(df.columns))

    # Columns in your log:
    # OpeningPrice, HighestPrice, LowestPrice, ClosingPrice, TradeVolume, Change
    df2 = df[["Code","Name","OpeningPrice","HighestPrice","LowestPrice","ClosingPrice","TradeVolume","Change"]].copy()
    df2.columns = ["Code","Name","Open","High","Low","Close","TradeVolume","Change"]

    for c in ["Open","High","Low","Close","TradeVolume","Change"]:
        df2[c] = pd.to_numeric(df2[c], errors="coerce")
    df2 = df2.dropna()

    df2["Code"] = df2["Code"].astype(str)
    df2 = df2[df2["Code"].str.len() == 4].copy()
    df2 = df2[(df2["High"] - df2["Low"]) > 0].copy()

    df2["body_ratio"] = (df2["Close"] - df2["Open"]) / (df2["High"] - df2["Low"])
    return df2

# -------- FinMind --------
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

def finmind_get(dataset: str, data_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    if not FINMIND_TOKEN:
        raise RuntimeError("Missing FINMIND_TOKEN (check GitHub Secrets + run.yml env).")

    headers = {"Authorization": f"Bearer {FINMIND_TOKEN}"}
    params = {"dataset": dataset, "data_id": data_id, "start_date": start_date, "end_date": end_date}
    r = requests.get(FINMIND_URL, headers=headers, params=params, timeout=30)

    print("FinMind status:", r.status_code, "dataset:", dataset, "data_id:", data_id)
    r.raise_for_status()

    j = r.json()
    if j.get("status") != 200:
        raise RuntimeError(f"FinMind API status not 200: {j}")

    df = pd.DataFrame(j.get("data", []))
    return df

def get_price_history(stock_id: str, days: int = 160) -> pd.DataFrame:
    end = today_tpe()
    start = end - dt.timedelta(days=days)
    df = finmind_get("TaiwanStockPrice", stock_id, fmt(start), fmt(end))
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    for c in ["open","max","min","close","Trading_Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["date","open","max","min","close","Trading_Volume"]).sort_values("date")
    return df

# -------- Strategy (your final agreed rules) --------
def market_ok_ma60() -> tuple[bool, float, float]:
    hist = get_price_history("^TWII", days=200)
    if hist.empty or len(hist) < 80:
        raise RuntimeError("Not enough ^TWII history for MA60")

    ma60 = hist["close"].rolling(60).mean()
    last_close = float(hist["close"].iloc[-1])
    last_ma60 = float(ma60.iloc[-1])
    return (last_close > last_ma60), last_close, last_ma60

def check_full(stock_id: str, snap_row: pd.Series) -> tuple[bool, dict]:
    hist = get_price_history(stock_id, days=200)
    if hist.empty or len(hist) < 30:
        return False, {"reason":"history too short"}

    # From snapshot (today)
    v_today = float(snap_row["TradeVolume"])   # shares
    o = float(snap_row["Open"])
    h = float(snap_row["High"])
    l = float(snap_row["Low"])
    c = float(snap_row["Close"])
    chg = float(snap_row["Change"])
    body_ratio = float(snap_row["body_ratio"])

    lots_threshold = 1500 * 1000  # 1500張 -> 股數

    # 5-day avg volume excluding today using history
    vol = hist["Trading_Volume"].astype(float)
    if len(vol) < 10:
        return False,
