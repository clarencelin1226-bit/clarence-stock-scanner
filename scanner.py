import os
import datetime as dt
import requests
import pandas as pd

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
FINMIND_TOKEN = os.getenv("FINMIND_TOKEN", "")

TWSE_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

# 用 0050 當作大盤 Proxy 來算 MA60（避免 ^TWII 資料不足）
MARKET_PROXY = "0050"

def send_telegram(msg: str):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    # Telegram 訊息長度限制：保守切段
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

def finmind_get(dataset: str, data_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    if not FINMIND_TOKEN:
        raise RuntimeError("Missing FINMIND_TOKEN")

    r = requests.get(
        FINMIND_URL,
        headers={"Authorization": f"Bearer {FINMIND_TOKEN}"},
        params={
            "dataset": dataset,
            "data_id": data_id,
            "start_date": start_date,
            "end_date": end_date
        },
        timeout=30
    )
    print("FinMind status:", r.status_code, "dataset:", dataset, "data_id:", data_id)
    r.raise_for_status()

    j = r.json()
    if j.get("status") != 200:
        raise RuntimeError(f"FinMind not ok: {j}")

    return pd.DataFrame(j.get("data", []))

def finmind_price(stock_id, days=500):
    end = today_tpe()
    start = end - dt.timedelta(days=days)

    df = finmind_get("TaiwanStockPrice", stock_id, fmt(start), fmt(end))
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    for c in ["open","max","min","close","Trading_Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna().sort_values("date")

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

def market_above_ma60():
    df = finmind_price(MARKET_PROXY, 700)
    if df.empty or len(df) < 80:
        send_telegram(f"⚠️ 大盤濾網資料不足：{MARKET_PROXY} 日K不足以計算 MA60")
        return False

    ma60 = df["close"].rolling(60).mean()
    last_close = float(df["close"].iloc[-1])
    last_ma60 = float(ma60.iloc[-1])

    if last_close <= last_ma60:
        send_telegram(f"❌ 大盤未站上季線：{MARKET_PROXY} 收盤 {last_close:.2f} ≤ MA60 {last_ma60:.2f}")
        return False

    send_telegram(f"✅ 大盤站上季線：{MARKET_PROXY} 收盤 {last_close:.2f} > MA60 {last_ma60:.2f}")
    return True

def load_stock_info_sector_map() -> dict:
    # 取得股票 -> 產業/族群 的對照表
    end = today_tpe()
    start = end - dt.timedelta(days=30)  # 給一點 buffer
    info = finmind_get("TaiwanStockInfo", "all", fmt(start), fmt(end))
    if info.empty or "stock_id" not in info.columns:
        return {}

    # 常見欄位：industry_category（若沒有就退而求其次）
    sector_col = None
    for c in ["industry_category", "industry", "category"]:
        if c in info.columns:
            sector_col = c
            break

    if not sector_col:
        return {}

    info = info.drop_duplicates("stock_id")
    mp = info.set_index("stock_id")[sector_col].astype(str).to_dict()
    return mp

def compute_strong_sectors(snap: pd.DataFrame, sector_map: dict) -> tuple[list, pd.DataFrame]:
    df = snap.copy()
    df["Sector"] = df["Code"].map(sector_map).fillna("Unknown")

    g = df.groupby("Sector").agg(
        n=("Code", "count"),
        avg_chg=("Change", "mean"),
        breadth=("Change", lambda s: float((s >= 2).mean()))  # 上漲廣度
    ).reset_index()

    # 避免樣本太少的族群干擾
    g = g[g["n"] >= 5].copy()

    # score：平均漲幅 + 2*廣度（你可以之後再調權重）
    g["score"] = g["avg_chg"] + 2.0 * g["breadth"]
    g = g.sort_values("score", ascending=False)

    top5 = g[g["Sector"] != "Unknown"].head(5)["Sector"].tolist()
    return top5, g.head(10)

def check_stock(code, snap_row):
    hist = finmind_price(code, 700)
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

    # 爆量長紅（含 3×5 日均量）
    if not (v_today >= lots1500 and chg >= 4 and c > o and body_ratio >= 0.6 and v_today > 3 * ma5):
        return None

    # 盤整突破（前 20 日）
    prev20 = hist.iloc[-21:-1]
    if len(prev20) < 20:
        return None

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
    }

def run():
    if not market_above_ma60():
        return

    snap = load_twse_snapshot()

    # 先做族群判定（用今日全市場快照）
    sector_map = load_stock_info_sector_map()
    strong_sectors, _ = compute_strong_sectors(snap, sector_map)

    if strong_sectors:
        send_telegram("🔥 今日強勢族群（Top5）： " + "、".join(strong_sectors))

    # 初篩（減少 FinMind 查詢量）
    pre = snap[
        (snap["TradeVolume"] >= 1500 * 1000) &
        (snap["Change"] >= 4) &
        (snap["Close"] > snap["Open"]) &
        (snap["body_ratio"] >= 0.6)
    ].copy()

    hits = []
    for _, r in pre.iterrows():
        res = check_stock(r["Code"], r)
        if res:
            sector = sector_map.get(r["Code"], "Unknown")
            is_strong = sector in strong_sectors
            hits.append({
                "Code": r["Code"],
                "Name": r["Name"],
                "Sector": sector,
                "is_strong": is_strong,
                **res
            })

    if not hits:
        send_telegram("✅ 今日無符合『爆量長紅＋盤整突破（含3×5日均量）』個股")
        return

    df = pd.DataFrame(hits)
    df = df.sort_values(["is_strong", "chg", "vol_mult"], ascending=[False, False, False]).head(30)

    lines = []
    for _, x in df.iterrows():
        tag = "🔥" if x["is_strong"] else "•"
        lines.append(
            f"{tag}{x['Code']} {x['Name']}｜{x['chg']:.1f}%｜量倍 {x['vol_mult']:.2f}｜突破 {x['break_pct']*100:.1f}%｜{x['Sector']}"
        )

    send_telegram("📈 台股突破清單（強勢族群優先）\n" + "\n".join(lines))

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("Scanner error:", repr(e))
        raise
