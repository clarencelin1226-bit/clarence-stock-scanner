# scanner.py
# 台股上市掃描（骨架版 + Debug）
# 使用 TWSE /exchangeReport/STOCK_DAY_ALL 當日快照
# 目的：先 100% 跑通 GitHub Actions + Telegram

import os
import requests
import pandas as pd

# =========================
# Telegram 設定
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def send_telegram(msg: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=20)

    print("Telegram status:", r.status_code)
    print("Telegram response:", r.text)

    r.raise_for_status()

# =========================
# TWSE API
# =========================
TWSE_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"

def pick_col(df, names):
    for n in names:
        if n in df.columns:
            return n
    return None

def load_stocks_today() -> pd.DataFrame:
    r = requests.get(TWSE_ALL, timeout=30)
    print("TWSE status:", r.status_code)
    r.raise_for_status()

    df = pd.DataFrame(r.json())
    print("TWSE columns:", list(df.columns))

    code = pick_col(df, ["Code", "證券代號", "股票代號"])
    name = pick_col(df, ["Name", "證券名稱", "股票名稱"])
    open_ = pick_col(df, ["Open", "OpeningPrice", "開盤價"])
    high = pick_col(df, ["High", "HighestPrice", "最高價"])
    low = pick_col(df, ["Low", "LowestPrice", "最低價"])
    close = pick_col(df, ["Close", "ClosingPrice", "收盤價"])
    vol = pick_col(df, ["TradeVolume", "成交股數", "成交量"])
    chg = pick_col(df, ["Change", "漲跌幅", "漲跌百分比"])

    print("Picked:", code, name, open_, high, low, close, vol, chg)

    if None in [code, name, open_, high, low, close, vol, chg]:
        raise RuntimeError("❌ 無法對齊 TWSE 欄位，請看 columns 輸出")

    df2 = df[[code, name, open_, high, low, close, vol, chg]].copy()
    df2.columns = ["Code", "Name", "Open", "High", "Low", "Close", "TradeVolume", "Change"]

    for c in ["Open", "High", "Low", "Close", "TradeVolume", "Change"]:
        df2[c] = pd.to_numeric(df2[c], errors="coerce")

    df2 = df2.dropna()
    df2["Code"] = df2["Code"].astype(str)
    df2 = df2[df2["Code"].str.len() == 4]

    df2 = df2[(df2["High"] - df2["Low"]) > 0]

    return df2

def run():
    df = load_stocks_today()

    df["body_ratio"] = (df["Close"] - df["Open"]) / (df["High"] - df["Low"])

    volume_threshold = 1500 * 1000  # 1500 張 → 股數

    hit = df[
        (df["TradeVolume"] >= volume_threshold) &
        (df["Change"] >= 4) &
        (df["Close"] > df["Open"]) &
        (df["body_ratio"] >= 0.6)
    ].copy()

    if hit.empty:
        send_telegram("✅ 今日無符合『爆量長紅（骨架版）』的上市股票")
        return

    hit = hit.sort_values(["Change", "TradeVolume"], ascending=False).head(30)

    lines = []
    for _, r in hit.iterrows():
        lines.append(
            f"{r['Code']} {r['Name']}｜{r['Change']:.2f}%｜量 {int(r['TradeVolume'])}｜實體 {r['body_ratio']:.2f}"
        )

    send_telegram("📈 台股爆量長紅清單（骨架版）\n" + "\n".join(lines))

if __name__ == "__main__":
    try:
        print("Starting scanner")
        print("BOT_TOKEN present:", bool(BOT_TOKEN))
        print("CHAT_ID present:", bool(CHAT_ID))
        run()
        print("Scanner finished")
    except Exception as e:
        print("Scanner error:", repr(e))
        raise
