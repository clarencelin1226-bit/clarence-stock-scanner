print("=== SCANNER VERSION: 2025-12-31 v1 ===")

import os
import datetime as dt
import requests
import pandas as pd
import time


# =======================
# ENV
# =======================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
FINMIND_TOKEN = os.getenv("FINMIND_TOKEN", "")

FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
TWSE_DAY_ALL = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"

# =======================
# STRATEGY PARAMS
# =======================
MARKET_PROXY = "0050"
MA60_WINDOW = 60

# Layer 2: 爆量長紅 (daily shape)  ✅ updated: 4% -> 3.5%
MIN_CHG_PCT = 3.5
MIN_BODY_RATIO = 0.60
MIN_LOTS = 1500          # 張
LOTS_UNIT = 1000         # FinMind Trading_Volume is shares = 張*1000

# Layer 3: 真爆量 ✅ updated: 3x -> 2x
VOL_MULT = 2.0           # today_vol > 2 * MA5 (exclude today)

# Layer 4: 盤整突破 ✅ updated: 8% -> 10%
CONSOL_DAYS = 20
MAX_RANGE_PCT = 0.10     # 20d range <= 10%
BREAKOUT_PCT = 0.01      # close >= high20*(1+1%)
BREAKOUT_VOL_GT_MA5 = True

# Sector strength: 5日主流族群
SECTOR_LOOKBACK_DAYS = 5
SECTOR_TOP_N = 5
SECTOR_MIN_COUNT = 5
SECTOR_UP_PCT = 2.0      # up >= 2% counts as "advancing"
SECTOR_SCORE_UP_WEIGHT = 2.0
SECTOR_MAIN_MIN_APPEAR = 3  # 5天內至少3天進Top5 => 主流族群


# =======================
# Utils
# =======================
def send_telegram(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram env missing; skip sending.")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(url, data={"chat_id": CHAT_ID, "text": text}, timeout=30)
    print("Telegram status:", r.status_code)
    if r.status_code != 200:
        print("Telegram response:", r.text)


def finmind_get(dataset: str, data_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    if not FINMIND_TOKEN:
        raise RuntimeError("Missing FINMIND_TOKEN")
    headers = {"Authorization": f"Bearer {FINMIND_TOKEN}"}
    params = {
        "dataset": dataset,
        "data_id": data_id,
        "start_date": start_date,
        "end_date": end_date,
    }
    r = requests.get(FINMIND_URL, headers=headers, params=params, timeout=30)
    j = r.json()
    print(f"FinMind status: {j.get('status')} dataset: {dataset} data_id: {data_id}")
    if j.get("status") != 200:
        return pd.DataFrame()
    return pd.DataFrame(j.get("data", []))


def get_price_history(stock_id: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    df = finmind_get("TaiwanStockPrice", stock_id, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    for c in ["open", "max", "min", "close", "Trading_Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["date", "open", "max", "min", "close", "Trading_Volume"]).sort_values("date")
    return df


def market_above_ma60(asof: dt.date) -> tuple[bool, str]:
    """Use 0050 close > MA60 on/asof date."""
    start = asof - dt.timedelta(days=800)
    df = get_price_history(MARKET_PROXY, start, asof)
    if df.empty or len(df) < (MA60_WINDOW + 1):
        return False, "Not enough 0050 history for MA60"
    ma60 = df["close"].rolling(MA60_WINDOW).mean().iloc[-1]
    close = float(df["close"].iloc[-1])
    if pd.isna(ma60):
        return False, "MA60 is NaN"
    ok = close > float(ma60)
    msg = f"{MARKET_PROXY} 收盤 {close:.2f} {'>' if ok else '<='} MA60 {float(ma60):.2f}"
    return ok, msg


# =======================
# TWSE fetch (daily snapshot)
# =======================
def twse_fetch_day(date_yyyymmdd: str | None = None, max_retries: int = 3) -> pd.DataFrame:
    """
    Fetch TWSE STOCK_DAY_ALL.
    - If date_yyyymmdd is None: latest available
    - If date_yyyymmdd provided (YYYYMMDD): request that date

    Robustness:
    - Retry on 5xx / transient network errors
    - Never raise; on failure returns empty DataFrame
    """
    params = {"response": "json"}
    if date_yyyymmdd:
        params["date"] = date_yyyymmdd

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(TWSE_DAY_ALL, params=params, timeout=30)
            print("TWSE status:", r.status_code, "date:", date_yyyymmdd or "latest", "attempt:", attempt)

            # 2xx OK
            if 200 <= r.status_code < 300:
                j = r.json()

                # 有時候 TWSE 回傳 stat != OK 或缺 data
                if isinstance(j, dict) and j.get("stat") not in (None, "OK"):
                    print("[TWSE] stat not OK:", j.get("stat"), "date:", date_yyyymmdd or "latest")
                    return pd.DataFrame()

                if "data" not in j:
                    return pd.DataFrame()

                if "fields" in j:
                    cols = j["fields"]
                    df = pd.DataFrame(j["data"], columns=cols)
                else:
                    df = pd.DataFrame(j["data"])

                rename_map = {
                    "日期": "Date",
                    "證券代號": "Code",
                    "證券名稱": "Name",
                    "成交股數": "TradeVolume",
                    "成交金額": "TradeValue",
                    "開盤價": "OpeningPrice",
                    "最高價": "HighestPrice",
                    "最低價": "LowestPrice",
                    "收盤價": "ClosingPrice",
                    "漲跌價差": "Change",
                    "成交筆數": "Transaction",
                }
                df = df.rename(columns=rename_map)

                keep = [c for c in ["Date", "Code", "Name", "TradeVolume", "TradeValue",
                                    "OpeningPrice", "HighestPrice", "LowestPrice", "ClosingPrice",
                                    "Change", "Transaction"] if c in df.columns]
                df = df[keep].copy()

                for c in ["TradeVolume", "TradeValue", "OpeningPrice", "HighestPrice", "LowestPrice", "ClosingPrice"]:
                    if c in df.columns:
                        df[c] = (
                            df[c].astype(str)
                            .str.replace(",", "", regex=False)
                            .replace("--", None)
                        )
                        df[c] = pd.to_numeric(df[c], errors="coerce")

                if "Code" in df.columns:
                    df["Code"] = df["Code"].astype(str).str.strip()
                    df = df[df["Code"].str.match(r"^\d{4}$", na=False)]

                # 沒資料就回空
                if df.empty:
                    return pd.DataFrame()

                # 過濾有效列
                need_cols = ["OpeningPrice", "HighestPrice", "LowestPrice", "ClosingPrice", "TradeVolume"]
                existing_need = [c for c in need_cols if c in df.columns]
                if existing_need:
                    df = df.dropna(subset=existing_need)

                return df

            # 4xx（通常是參數/日期問題）直接放棄，不重試
            if 400 <= r.status_code < 500:
                print("[TWSE] client error, skip:", r.status_code, "date:", date_yyyymmdd or "latest")
                return pd.DataFrame()

            # 5xx：重試
            last_err = RuntimeError(f"TWSE {r.status_code}")

        except Exception as e:
            last_err = e

        # 簡單 backoff
        time.sleep(0.8 * attempt)

    print("[TWSE] failed after retries:", repr(last_err), "date:", date_yyyymmdd or "latest")
    return pd.DataFrame()

def find_recent_trade_days(n: int, max_lookback_days: int = 30) -> list[str]:
    """
    Find recent TWSE trade dates (YYYYMMDD) by probing backwards.
    Returns list of date strings (most recent first).
    """
    out: list[str] = []
    d = dt.date.today()
    tries = 0

    while len(out) < n and tries < max_lookback_days:
        yyyymmdd = d.strftime("%Y%m%d")

        df = twse_fetch_day(yyyymmdd)
        if df is not None and not df.empty:
            out.append(yyyymmdd)

        d -= dt.timedelta(days=1)
        tries += 1

    return out


# =======================
# Sector mapping & 5-day main sectors
# =======================
def load_sector_map() -> dict:
    info = finmind_get("TaiwanStockInfo", "all", "2000-01-01", dt.date.today().strftime("%Y-%m-%d"))
    if info.empty:
        return {}

    candidates = ["industry_category", "industry", "category", "type"]
    pick = None
    for c in candidates:
        if c in info.columns:
            pick = c
            break

    if not pick:
        non = [c for c in info.columns if c not in ["stock_id", "date"]]
        pick = non[0] if non else None

    sector_map = {}
    if pick and "stock_id" in info.columns:
        for _, r in info.iterrows():
            sid = str(r["stock_id"]).strip()
            sec = str(r.get(pick, "Unknown")).strip()
            if sid and sid.isdigit():
                sector_map[sid] = sec if sec else "Unknown"
    return sector_map


def sector_score_for_day(df_day: pd.DataFrame, sector_map: dict) -> pd.DataFrame:
    """
    Compute sector score for a single day using TWSE day snapshot.
    score = avg_return + 2 * up_ratio(>=2%)
    """
    d = df_day.copy()
    d["ret"] = (d["ClosingPrice"] - d["OpeningPrice"]) / d["OpeningPrice"] * 100.0
    d["Sector"] = d["Code"].map(lambda x: sector_map.get(str(x), "Unknown"))

    g = d.groupby("Sector", dropna=False)
    out = []
    for sec, sub in g:
        if sec in [None, "", "nan"]:
            sec = "Unknown"
        sub = sub.dropna(subset=["ret"])
        if len(sub) < SECTOR_MIN_COUNT:
            continue
        avg_ret = float(sub["ret"].mean())
        up_ratio = float((sub["ret"] >= SECTOR_UP_PCT).mean())
        score = avg_ret + SECTOR_SCORE_UP_WEIGHT * up_ratio
        out.append((sec, score, avg_ret, up_ratio, len(sub)))

    res = pd.DataFrame(out, columns=["Sector", "Score", "AvgRet", "UpRatio", "Count"])
    res = res.sort_values("Score", ascending=False)
    return res


def compute_5day_main_sectors(sector_map: dict) -> tuple[set, list[str]]:
    """
    Main sectors = appear in daily TopN at least MIN_APPEAR times within last 5 trading days.
    Returns (main_sectors_set, trade_days_list_most_recent_first)
    """
    trade_days = find_recent_trade_days(SECTOR_LOOKBACK_DAYS)
    appear = {}

    for yyyymmdd in trade_days:
        df_day = twse_fetch_day(yyyymmdd)
        if df_day.empty:
            continue
        score_df = sector_score_for_day(df_day, sector_map)
        top = score_df.head(SECTOR_TOP_N)["Sector"].tolist()
        for sec in top:
            appear[sec] = appear.get(sec, 0) + 1

    main = {sec for sec, cnt in appear.items() if cnt >= SECTOR_MAIN_MIN_APPEAR}
    return main, trade_days


# =======================
# Main scan logic
# =======================
def load_today_candidates() -> pd.DataFrame:
    df = twse_fetch_day(None)  # latest
    if df.empty:
        return df

    df["chg_pct"] = (df["ClosingPrice"] - df["OpeningPrice"]) / df["OpeningPrice"] * 100.0
    df["range"] = df["HighestPrice"] - df["LowestPrice"]
    df["body_ratio"] = (df["ClosingPrice"] - df["OpeningPrice"]) / df["range"]
    df["lots"] = df["TradeVolume"] / 1000.0  # TWSE TradeVolume is shares

    cond = (
        (df["chg_pct"] >= MIN_CHG_PCT) &
        (df["ClosingPrice"] > df["OpeningPrice"]) &
        (df["body_ratio"] >= MIN_BODY_RATIO) &
        (df["lots"] >= MIN_LOTS) &
        (df["range"] > 0)
    )
    out = df.loc[cond, ["Code", "Name", "OpeningPrice", "HighestPrice", "LowestPrice",
                        "ClosingPrice", "TradeVolume", "chg_pct"]].copy()
    return out


def check_one_stock(stock_id: str, today_row: pd.Series) -> dict | None:
    """
    Use FinMind history to validate:
    - vol_mult > VOL_MULT x MA5 (exclude today)
    - consolidation breakout on prev CONSOL_DAYS
    - (NEW) return MA20/MA60 context for A/B tagging
    """
    end = dt.date.today()
    start = end - dt.timedelta(days=500)
    hist = get_price_history(stock_id, start, end)
    if hist.empty:
        return None

    c = float(today_row["ClosingPrice"])       # today close (TWSE)
    v_today = float(today_row["TradeVolume"])  # shares (TWSE)

    if len(hist) < (CONSOL_DAYS + 6):
        return None

    # EXCLUDE today in history for indicators (avoid look-ahead)
    base = hist.iloc[:-1].copy()
    if len(base) < (CONSOL_DAYS + 6):
        return None

    # ====== Volume check (MA5 exclude today) ======
    ma5 = float(base["Trading_Volume"].iloc[-5:].mean())
    vol_mult = (v_today / ma5) if ma5 > 0 else 0.0
    if not (v_today > VOL_MULT * ma5):
        return None

    # ====== Consolidation breakout check ======
    prev20 = base.iloc[-CONSOL_DAYS:]
    high20 = float(prev20["max"].max())
    low20 = float(prev20["min"].min())
    width = (high20 - low20) / low20 if low20 > 0 else 999.0
    if width > MAX_RANGE_PCT:
        return None

    if not (c >= high20 * (1.0 + BREAKOUT_PCT)):
        return None

    if BREAKOUT_VOL_GT_MA5 and not (v_today > ma5):
        return None

    break_pct = (c / high20 - 1.0) if high20 > 0 else 0.0

    # =========================
    # NEW: MA20 / MA60 (use FinMind close, EXCLUDE today)
    # =========================
    ma20 = None
    ma60 = None
    if len(base) >= 20:
        ma20 = float(base["close"].rolling(20).mean().iloc[-1])
    if len(base) >= 60:
        ma60 = float(base["close"].rolling(60).mean().iloc[-1])

    return {
        "Code": stock_id,
        "Name": str(today_row["Name"]),
        "chg": float(today_row["chg_pct"]),
        "vol_mult": float(vol_mult),
        "lots": float(v_today / LOTS_UNIT),
        "range20_pct": float(width),
        "break_pct": float(break_pct),

        # NEW
        "close": float(c),
        "ma20": ma20,
        "ma60": ma60,
    }



def run():
    print("Starting scanner")

    # -------------------------
    # Helper: always export json
    # -------------------------
    def export_scanner_result(stocks: list[str], signal_date: str, stocks_a: list[str] | None = None, stocks_b: list[str] | None = None):
        import json
        data = {
            "signal_date": signal_date,
            "stocks": stocks,          # keep for tracker compatibility
            "stocks_a": stocks_a or [],
            "stocks_b": stocks_b or [],
        }
        with open("scanner_result.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print("[SCANNER_RESULT_JSON]", json.dumps(data, ensure_ascii=False))

    # Default signal date = today (will be updated if we have trade_days)
    signal_date = dt.date.today().strftime("%Y-%m-%d")

    ok, msg = market_above_ma60(dt.date.today())
    send_telegram(("✅ 大盤站上季線：" if ok else "❌ 大盤未站上季線：") + msg)
    if not ok:
        export_scanner_result([], signal_date, [], [])
        return

    sector_map = load_sector_map()
    main_sectors, trade_days = compute_5day_main_sectors(sector_map)

    # Update signal_date from trade_days[0] if available (YYYYMMDD -> YYYY-MM-DD)
    if trade_days and isinstance(trade_days[0], str) and len(trade_days[0]) == 8:
        signal_date = f"{trade_days[0][0:4]}-{trade_days[0][4:6]}-{trade_days[0][6:8]}"

    if main_sectors:
        send_telegram("🔥🔥 5日主流族群（近5日Top5入榜≥3日）：\n" + "、".join(sorted(main_sectors)))
    else:
        send_telegram("ℹ️ 5日主流族群：資料不足或無法辨識（main_sectors 為空）")

    cand = load_today_candidates()
    if cand.empty:
        send_telegram("✅ 今日無符合『爆量長紅』初篩個股")
        export_scanner_result([], signal_date, [], [])
        return

    hits = []
    for _, r in cand.iterrows():
        sid = str(r["Code"])
        res = check_one_stock(sid, r)
        if res:
            res["Sector"] = sector_map.get(sid, "Unknown")
            hits.append(res)

    if not hits:
        send_telegram("✅ 今日無符合『爆量長紅＋盤整突破（含2×5日均量）』個股")
        export_scanner_result([], signal_date, [], [])
        return

    # ---- helper for sorting
    def is_main(sec: str) -> int:
        return 1 if sec in main_sectors else 0

    # ---- NEW: classify A/B
    # A: close >= MA20 AND MA20 > MA60  (both MA exist)
    hitsA = []
    hitsB = []
    for x in hits:
        close = x.get("close", None)
        ma20 = x.get("ma20", None)
        ma60 = x.get("ma60", None)
        if (close is not None) and (ma20 is not None) and (ma60 is not None) and (close >= ma20) and (ma20 > ma60):
            x["signal_type"] = "A"
            hitsA.append(x)
        else:
            x["signal_type"] = "B"
            hitsB.append(x)

    # ---- keep your original priority logic, but apply within A then B
    def sort_key(x):
        return (is_main(x.get("Sector", "")), x.get("chg", 0), x.get("vol_mult", 0))

    hitsA = sorted(hitsA, key=sort_key, reverse=True)
    hitsB = sorted(hitsB, key=sort_key, reverse=True)

    # ---- Telegram output
    def build_lines(xs, title):
        if not xs:
            return None
        lines = []
        for x in xs[:30]:
            sec = x.get("Sector", "Unknown")
            tag = "🔥🔥" if sec in main_sectors else "•"
            ma20 = x.get("ma20", None)
            ma60 = x.get("ma60", None)

            ma_txt = ""
            if (ma20 is not None) and (ma60 is not None):
                ma_txt = f"｜MA20 {ma20:.2f}｜MA60 {ma60:.2f}"

            lines.append(
                f"{tag}{x['Code']} {x['Name']}｜{x['chg']:.1f}%｜量倍 {x['vol_mult']:.2f}x｜突破 {x['break_pct']*100:.1f}%｜{sec}{ma_txt}"
            )
        return f"{title}\n" + "\n".join(lines)

    msgA = build_lines(hitsA, "🅰️ 訊號A（多頭排列 + 站上MA20）")
    msgB = build_lines(hitsB, "🅱️ 訊號B（符合原條件，但未達A）")

    if msgA:
        send_telegram(msgA)
    if msgB:
        send_telegram(msgB)

    # ---- Export json for tracker dispatch (keep stocks = all)
    export_scanner_result(
        stocks=[str(x["Code"]) for x in (hitsA + hitsB)],
        signal_date=signal_date,
        stocks_a=[str(x["Code"]) for x in hitsA],
        stocks_b=[str(x["Code"]) for x in hitsB],
    )
    print("=== EOF reached ===")
    # =========================
# Program entry point
# =========================
if __name__ == "__main__":
    print("=== SCANNER ENTRY ===")
    try:
        run()
        print("=== SCANNER EXIT (OK) ===")
    except Exception as e:
        print("=== SCANNER EXIT (ERROR) ===", repr(e))
        raise


