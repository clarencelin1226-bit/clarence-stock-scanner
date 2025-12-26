# scanner.py
# 台股上市：骨架版（先跑通）+ Debug 版（把錯誤印出來）
# 使用 TWSE 公開 API：/exchangeReport/STOCK_DAY_ALL（當日快照）
# 後續我們再升級成：MA60 / 5日均量3倍 / 20日盤整突破1% / 強勢族群排序（需歷史資料）

import os
import requests
import pandas as pd

# =========================
# Telegram 設定（從 GitHub Secrets 讀）
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def send_telegram(msg: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID. "
            "Check: GitHub Repo -> Settings -> Secrets and variables -> Actions."
        )

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": CHAT_ID, "text": msg}, timeout=20)

    # Debug prints (visible in GitHub Actions logs)
    print("Telegram status:", r.status_code)
    print("Telegram response:", r.text)

    r.raise_for_status()

# =========================
# TWSE API
# =========================
TWSE_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"

def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return first existing column name from candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    return None

def load_stocks_today() -> pd.DataFrame:
    resp = requests.get(TWSE_ALL, timeout=30)
    print("TWSE STOCK_DAY_ALL status:", resp.status_code)
    resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, list) or len(data) == 0:
        raise RuntimeError("TWSE API returned empty or non-list data.")

    df = pd.DataFrame(data)
    print("TWSE columns:", list(df.columns))

    # 股票代碼/名稱欄位（常見 Code/Name，也可能有不同命名）
    code_col = _pick_col(df, ["Code", "code", "股票代號", "證券代號", "證券代碼"])
    name_col = _pick_col(df, ["Name", "name", "股票名稱", "證券名稱", "名稱"])

    # 價格欄位：可能是 Open/High/Low/Close，也可能是 OpeningPrice/HighestPrice...
    open_col  = _pick_col(df, ["Open", "OpeningPrice", "open", "開盤價"])
    high_col  = _pick_col(df, ["High", "HighestPrice", "high", "最高價"])
    low_col   = _pick_col(df, ["Low", "LowestPrice", "low", "最低價"])
    close_col = _pick_col(df, ["Close", "ClosingPrice", "close", "收盤價"])

    # 成交量欄位：有些是 TradeVolume，有些是 Volume，有些是 成交股數
    vol_col   = _pick_col(df, ["TradeVolume", "Volume", "TradeVolumeShares", "成交股數", "成交量"])

    # 漲跌幅欄位：有些用 Change(%)，有些可能是 漲跌幅
    chg_col   = _pick_col(df, ["Change", "ChangePercent", "PctChange", "漲跌幅", "漲跌百分比"])

    missing = {
        "code_col": code_col, "name_col": name_col,
        "open_col": open_col, "high_col": high_col, "low_col": low_col, "close_col": close_col,
        "vol_col": vol_col, "chg_col": chg_col
    }
    print("Picked columns:", missing)

    required = [code_col, name_col, open_col, high_col, low_col, close_col, vol_col, chg_col]
    if any(c is None for c in required):
        raise RuntimeError(
            "Cannot find required columns from TWSE response. "
            "Please check 'TWSE columns' printed above and tell me."
        )

    # 統一欄位名
    df2 = df[[code_col, name_col, open_col, high_col, low_col, close_col, vol_col, chg_col]].copy()
    df2.columns = ["Code", "Name", "Open", "High", "Low", "Close", "TradeVolu]()
