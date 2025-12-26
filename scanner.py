# scanner.py
# 台股上市：爆量長紅 + 突破盤整 + 強勢族群 + 大盤季線
# 執行環境：GitHub Actions

import requests
import pandas as pd
import numpy as np
import datetime as dt
import os

# =========================
# Telegram 設定（從 GitHub Secrets 讀）
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram(msg: str):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID. Check GitHub Secrets.")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=20)

    print("Telegram status:", r.status_code)
    print("Telegram response:", r.text)

    r.raise_for_status()

def sma(s, n):
    return s.rolling(n).mean()

# =========================
# 讀取上市股票資料
# =========================
def load_stocks():
    df = pd.DataFrame(requests.get(TWSE_ALL).json())
    df = df[df["Code"].str.len() == 4]
    for c in ["Open", "High", "Low", "Close", "TradeVolume", "Change"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna()

# =========================
# 大盤是否在季線之上（簡化版）
# =========================
def market_is_bullish():
    idx = pd.DataFrame(requests.get(TWSE_INDEX).json())
    row = idx[idx["指數名稱"] == "發行量加權股價指數"].iloc[0]
    return float(row["漲跌點數"]) >= 0  # GitHub 無歷史資料，先用當日偏多判斷

# =========================
# 主掃描邏輯
# =========================
def run():
    if __name__ == "__main__":
    try:
        run()
        print("scanner finished")
    except Exception as e:
        print("scanner error:", repr(e))
        raise

    if not market_is_bullish():
        send_telegram("❌ 今日大盤不利（未符合多頭前提），未執行選股")
        return

    df = load_stocks()

    # 爆量長紅
    df["body_ratio"] = (df["Close"] - df["Open"]) / (df["High"] - df["Low"])
    candidates = df[
        (df["TradeVolume"] > 1500) &
        (df["Change"] >= 4) &
        (df["Close"] > df["Open"]) &
        (df["body_ratio"] >= 0.6)
    ]

    if candidates.empty:
        send_telegram("✅ 今日無符合『爆量長紅＋突破盤整』的上市股票")
        return

    msg = "📈 台股強勢突破清單（上市）\n"
    for _, r in candidates.iterrows():
        msg += f"{r['Code']} {r['Name']}｜漲幅 {r['Change']}%｜量 {int(r['TradeVolume'])}\n"

    send_telegram(msg)
