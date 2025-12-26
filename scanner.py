import os
import datetime as dt
import requests
import pandas as pd

# =====================
# Secrets
# =====================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
FINMIND_TOKEN = os.getenv("FINMIND_TOKEN", "")

# =====================
# Telegram
# =====================
def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=20)

# =====================
# Time helpers (Taipei)
# =====================
def today_tpe():
    return (dt.datetime.utcnow() + dt.timedelta(hours=8)).date()

def fmt(d):
    return d.strftime("%Y-%m-%d")

# =====================
# TWSE snapshot (當日)
# =====================
TWSE_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"

def load_twse_snapshot():
    df = pd.DataFrame(requests.get(TWSE_ALL, timeout=30).json())

    df = df[[
        "Code","Name",
        "OpeningPrice","HighestPrice","LowestPrice","ClosingPrice",
        "TradeVolume","Change"
    ]].copy()

    df.columns = ["Code","Name","Open","High","Low","Close","TradeVolume","Change"]

    for c in ["Open","High","Low","Close","TradeVolume","Change"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna()
    df["Code"] = df["Code"].astype(str)
    df = df[df["Code"].str.len() == 4]
    df = df[(df["High"] - df["Low"]) > 0]

    df["body_ratio"] = (df["Close"] - df["Open"]) / (df["High"] - df["Low"])
    return df

# =====================
# FinMind API
# =====================
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

def finmind_price(stock_id, days=200):
    end = today_tpe()
    start = end - dt.timedelta(days=days)

    r = requests.get(
        FINMIND_URL,
        headers={"Authorization": f"Bearer {FINMIND_TOKEN}"},
        params={
            "dataset": "TaiwanStockPrice",
            "data_id": stock_id,
            "start_date": fmt(start),
            "end_date": fmt(end)
        },
        timeout=30
    )
    data = r.json()["data"]
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    for c in ["open","max","min","close","Trading_Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna().sort_values("date")

# =====================
# 大盤季線
# =====================
def market_above_ma60():
    df = finmind_price("^TWII", 200)
    ma60 = df["close"].rolling(60).mean()
    return df["close"].iloc[-1] > ma60.iloc[-1]

# =====================
# 個股完整條件
# =====================
def check_stock(stock_id, snap):
    hist = finmind_price(stock_id, 200)
    if len(hist) < 30:
        return None

    v_today = snap["TradeVolume"]
    o, c = snap["Open"], snap["Close"]
    body_ratio = snap["body_ratio"]
    chg = snap["Change"]

    lots_1500 = 1500 * 1000

    vol = hist["Trading_Volume"]
    ma5 = vol.iloc[-6:-1].mean()

    # 爆量長紅
    if not (
        v_today >= lots_1500 and
        chg >= 4 and
        c > o and
        body_ratio >= 0.6 and
        v_today > 3 * ma5
    ):
        return None

    # 盤整突破
    prev20
