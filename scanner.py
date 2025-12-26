# scanner.py
# 台股上市：骨架版（先跑通）+ Debug 版（把錯誤印出來）
# 目前使用 TWSE 公開 API 的「當日快照」資料
# 後續我們會再升級成：MA60 / 5日均量3倍 / 20日盤整突破1% / 強勢族群排序（需歷史資料）

import os
import requests
import pandas as pd


# =========================
# Telegram 設定（從 GitHub Secrets 讀）
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def send_telegram(msg: str) -> None:
    """Send message to Telegram and print response for debugging."""
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
# TWSE 公開 API（當日快照）
# =========================
TWSE_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"


def load_stocks_today() -> pd.DataFrame:
    """
    Load today's TWSE listed stock snapshot.
    Expected columns include: Code, Name, Open, High, Low, Close, TradeVolume, Change
    """
    resp = requests.get(TWSE_ALL, timeout=30)
    print("TWSE STOCK_DAY_ALL status:", resp.status_code)

    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list) or len(data) == 0:
        raise RuntimeError("TWSE API returned empty or non-list data.")

    df = pd.DataFrame(data)

    # Keep 4-digit stock codes only
    df = df[df["Code"].astype(str).str.len() == 4].copy()

    # Convert numeric columns
    for c in ["Open", "High", "Low", "Close", "TradeVolume", "Change"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["Open", "High", "Low", "Close", "TradeVolume", "Change"])

    # Avoid division by zero
    df = df[(df["High"] - df["Low"]) > 0]

    return df


def run() -> None:
    """
    Current skeleton logic:
    - Listed stocks only
    - Long red candle (Close > Open) with body ratio >= 0.6
    - Change >= +4%
    - TradeVolume > 1500 (TWSE data is usually shares; we treat as "張門檻" skeleton for now)
    NOTE: Full strategy will be added later with historical data.
    """
    df = load_stocks_today()

    # Candle body ratio
    df["body_ratio"] = (df["Close"] - df["Open"]) / (df["High"] - df["Low"])

    # Skeleton candidate filter (matches the "長紅+漲幅+量門檻" portion)
    candidates = df[
        (df["TradeVolume"] > 1500) &
        (df["Change"] >= 4) &
        (df["Close"] > df["Open"]) &
        (df["body_ratio"] >= 0.6)
    ].copy()

    if candidates.empty:
        send_telegram("✅ 今日掃描完成：無符合『長紅＋漲幅≥4%＋量門檻』的上市股票（骨架版）")
        return

    # Build message (limit to avoid Telegram message too long)
    candidates = candidates.sort_values(["Change", "TradeVolume"], ascending=[False, False]).head(40)

    lines = []
    for _, r in candidates.iterrows():
        lines.append(
            f"{r['Code']} {r['Name']}｜漲幅 {float(r['Change']):.2f}%｜量 {int(r['TradeVolume'])}｜長紅占比 {float(r['body_ratio']):.2f}"
        )

    msg = "📈 台股掃描清單（上市｜骨架版）\n" + "\n".join(lines)
    send_telegram(msg)


if __name__ == "__main__":
    try:
        print("Starting scanner...")
        print("BOT_TOKEN present:", bool(BOT_TOKEN))
        print("CHAT_ID present:", bool(CHAT_ID))

        run()

        print("scanner finished")
    except Exception as e:
        print("scanner error:", repr(e))
        raise
