import os
import datetime as dt
import requests
import pandas as pd

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
FINMIND_TOKEN = os.getenv("FINMIND_TOKEN", "")

TWSE_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

def send_telegram(msg: str):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    # split long message to avoid Telegram limit
    chunk_size = 3500
    parts = [msg[i:i+chunk_size] for i in range(0, len(msg), chunk_size)] or [""]

    for part in parts:
        r = requests.post(url, json={"chat_id": CHAT_ID, "text": part}, timeout=20)
        print("Telegram status:", r.status_code)
        if r.status_code != 200:
            print("Telegram response:", r.text)
        r.raise_for_status()

def today_tpe():
    return (dt.datetime.utcnow() + dt.timedelta(hours=8)).date()

def fmt(d):
    return d.strftime("%Y-%m-%d")

def load_twse_snapshot():
    r = requests.get(TWSE_ALL, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json())

    df = df[["Code","Name","OpeningPrice","HighestPrice","LowestPrice","ClosingPrice","TradeVolume","Change"]].copy()
    df.columns = ["Code","Name","Open","High","Low","Close","TradeVolume","Change"]

    for c in ["Open","High","Low","Close","TradeVolume","Change"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna()

    df["Code"] = df["Code"].astype(str)
    df = df[df["Code"].str.len() == 4]
    df = df[(df["High"] - df["Low"]) > 0]

    df["body_ratio"] = (df["Close"] - df["Open"]) / (df["High"] - df["Low"])
    return df

def finmind_price(stock_id, days=220):
    if not FINMIND_TOKEN:
        raise RuntimeError("Missing FINMIND_TOKEN")

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
    print("FinMind status:", r.status_code, "data_id:", stock_id)
    r.raise_for_status()

    j = r.json()
    if j.get("status") != 200:
        raise RuntimeError(f"FinMind not ok: {j}")

    df = pd.DataFrame(j.get("data", []))
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    for c in ["open","max","min","close","Trading_Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna().sort_values("date")

def market_above_ma60():
    df = finmind_price("^TWII", 260)
    if df.empty or len(df) < 80:
        raise RuntimeError("Not enough ^TWII history for MA60")
    ma60 = df["close"].rolling(60).mean()
    return float(df["close"].iloc[-1]) > float(ma60.iloc[-1])

def check_stock(code, snap_row):
    hist = finmind_price(code, 260)
    if hist.empty or len(hist) < 30:
        return None

    v_today = float(snap_row["TradeVolume"])
    o = float(snap_row["Open"])
    c = float(snap_row["Close"])
    chg = float(snap_row["Change"])
    body_ratio = float(snap_row["body_ratio"])

    lots1500 = 1500 * 1000

    vol = hist["Trading_Volume"].astype(float)
    ma5 = float(vol.iloc[-6:-1].mean())

    # 爆量長紅（含 3x MA5）
    if not (v_today >= lots1500 and chg >= 4 and c > o and body_ratio >= 0.6 and v_today > 3 * ma5):
        return None

    # 盤整突破（前 20 日）
    prev20 = hist.iloc[-21:-1]
    hi = float(prev20["max"].max())
    lo = float(prev20["min"].min())
    width = (hi - lo) / lo if lo > 0 else 999

    if width > 0.08:
        return None

    if not (c >= hi * 1.01 and v_today > ma5):
        return None

    return {
        "chg": chg,
        "vol_mult": (v_today / ma5) if ma5 > 0 else None,
        "break_pct": (c / hi - 1.0),
        "width": width
    }

def run():
    # 不印太多，只留最必要線索
    if not market_above_ma60():
        send_telegram("❌ 大盤未站上季線（MA60），今日不進行選股")
        return

    snap = load_twse_snapshot()

    pre = snap[
        (snap["TradeVolume"] >= 1500 * 1000) &
        (snap["Change"] >= 4) &
        (snap["Close"] > snap["Open"]) &
        (snap["body_ratio"] >= 0.6)
    ]

    hits = []
    for _, r in pre.iterrows():
        res = check_stock(r["Code"], r)
        if res:
            hits.append(
                f"{r['Code']} {r['Name']}｜{res['chg']:.1f}%｜量倍 {res['vol_mult']:.2f}｜突破 {res['break_pct']*100:.1f}%"
            )

    if not hits:
        send_telegram("✅ 大盤多頭，但今日無符合『爆量長紅＋盤整突破』個股")
    else:
        send_telegram("📈 台股突破清單\n" + "\n".join(hits))

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("Scanner error:", repr(e))
        raise
