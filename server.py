#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
美股板块轮动实时监控服务 (US Sector Rotation Monitor)
=====================================================
三层框架: 环境开关(Regime) -> 板块打分(Selection) -> 风控纪律(Risk)
纯标准库实现, 无第三方依赖.

数据源:
  - Nasdaq API : 11 个板块 ETF + SPY + HYG/LQD, 近 2 年日线 OHLCV
  - CNBC       : 10Y/2Y/3M 美债、10Y TIPS 实际利率、VIX、SPX (主源, 实时)
  - Cboe       : VIX 日线历史 (备用)
  - FRED       : HY 利差 / EFFR / 国债收益率 (备用, 不可用时自动切换代理)
  - 手动输入   : 盈利修正广度 / PE / 持仓 / 组合回撤 (overrides.json, 前端可编辑)

用法:
  python server.py [--port 8077] [--demo]
浏览器访问 http://127.0.0.1:8077
"""

import datetime as dt
import gzip
import json
import math
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE, "data_cache")
OVERRIDES_FILE = os.path.join(BASE, "overrides.json")
SNAPSHOT_FILE = os.path.join(BASE, "snapshot.json")
DEMO_SNAPSHOT_FILE = os.path.join(BASE, "demo_snapshot.json")
DEMO_MARKER = os.path.join(BASE, "demo_mode.json")
os.makedirs(CACHE_DIR, exist_ok=True)

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# ---------------------------------------------------------------------------
# 静态配置
# ---------------------------------------------------------------------------

SECTORS = [
    ("XLK", "科技"), ("XLF", "金融"), ("XLE", "能源"), ("XLV", "医疗保健"),
    ("XLP", "必需消费"), ("XLU", "公用事业"), ("XLY", "可选消费"),
    ("XLB", "材料"), ("XLI", "工业"), ("XLRE", "房地产"), ("XLC", "通信服务"),
]
TICKERS = [s[0] for s in SECTORS] + ["SPY", "HYG", "LQD"]

# 静态参考 PE 带 (低, 高) —— 近似值, 用于估值因子; 真实 PE 可在界面手动输入覆盖
PE_BANDS = {
    "XLK": (18, 32), "XLF": (8, 15), "XLE": (6, 14), "XLV": (13, 22),
    "XLP": (15, 24), "XLU": (14, 24), "XLY": (15, 28), "XLB": (10, 20),
    "XLI": (12, 22), "XLRE": (18, 40), "XLC": (13, 24),
}

# 2026 年 FOMC 决议日 (会议第二天, 14:00 ET) —— 来源: federalreserve.gov
FOMC_2026 = [dt.date(2026, 1, 28), dt.date(2026, 3, 18), dt.date(2026, 4, 29),
             dt.date(2026, 6, 17), dt.date(2026, 7, 29), dt.date(2026, 9, 16),
             dt.date(2026, 10, 28), dt.date(2026, 12, 9)]
# 2026 年 CPI 公布日 (8:30 ET) —— 参考日期, 以 BLS 公告为准
CPI_2026 = [dt.date(2026, 1, 13), dt.date(2026, 2, 11), dt.date(2026, 3, 11),
            dt.date(2026, 4, 10), dt.date(2026, 5, 12), dt.date(2026, 6, 10),
            dt.date(2026, 7, 14), dt.date(2026, 8, 12), dt.date(2026, 9, 11),
            dt.date(2026, 10, 13), dt.date(2026, 11, 10), dt.date(2026, 12, 10)]

FRED_SERIES = ["DGS10", "DGS2", "DFII10", "BAMLH0A0HYM2", "DFF"]

PRICE_TTL = 3 * 3600          # 板块日线缓存
SLOW_TTL = 6 * 3600           # VIX / FRED 缓存
STATUS_TTL = 5 * 60           # SPY 市场状态缓存

# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------


def http_get(url, headers=None, timeout=20, retries=1):
    h = {"User-Agent": UA, "Accept": "*/*", "Accept-Encoding": "gzip, deflate"}
    if headers:
        h.update(headers)
    last = None
    for _ in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                if (r.headers.get("Content-Encoding") or "").lower() == "gzip":
                    raw = gzip.decompress(raw)
                return raw
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1.0)
    raise last


def read_cache(name):
    p = os.path.join(CACHE_DIR, name)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if time.time() - obj.get("fetched_at", 0) < obj.get("ttl", 0):
            return obj
    except Exception:
        pass
    return None


def write_cache(name, obj, ttl):
    obj["fetched_at"] = time.time()
    obj["ttl"] = ttl
    with open(os.path.join(CACHE_DIR, name), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def et_offset_for(date):
    """美东夏令时近似: 3月第2个周日 ~ 11月第1个周日 为 EDT(UTC-4), 其余 EST(UTC-5)."""

    def nth_dow(y, m, n, wd):
        d = dt.date(y, m, 1)
        off = (wd - d.weekday()) % 7
        return d + dt.timedelta(days=off + 7 * (n - 1))

    dst_start = nth_dow(date.year, 3, 2, 6)
    dst_end = nth_dow(date.year, 11, 1, 6)
    return -4 if dst_start <= date < dst_end else -5


def et_now():
    utc = dt.datetime.now(dt.timezone.utc)
    off = et_offset_for(utc.date())
    return utc.astimezone(dt.timezone(dt.timedelta(hours=off)))


def fmt_dt(d):
    return d.strftime("%Y-%m-%d %H:%M:%S")


def rnd(x, n=2):
    return None if x is None else round(float(x), n)


def pct(v):
    return None if v is None else round(v * 100, 2)


# ---------------------------------------------------------------------------
# 数据抓取
# ---------------------------------------------------------------------------


def nasdaq_chart(ticker, force=False):
    """返回 [(date, close, volume), ...] 按日期升序; 磁盘缓存."""
    cache = None if force else read_cache("nasdaq_" + ticker + ".json")
    if cache:
        return cache["data"]
    today = dt.date.today()
    frm = (today - dt.timedelta(days=560)).isoformat()
    url = ("https://api.nasdaq.com/api/quote/{}/chart?assetclass=etf"
           "&fromdate={}&todate={}").format(ticker, frm, today.isoformat())
    raw = http_get(url, headers={
        "Accept": "application/json",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
    })
    j = json.loads(raw.decode("utf-8", "ignore"))
    rows = (j.get("data") or {}).get("chart") or []
    out = []
    for r in rows:
        z = r.get("z") or {}
        try:
            d = dt.datetime.strptime(z.get("dateTime", ""), "%m/%d/%Y").date()
            out.append([d.isoformat(), float(z["close"]),
                        float(str(z.get("volume") or 0).replace(",", "") or 0)])
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    if len(out) < 30:
        raise RuntimeError("Nasdaq chart rows too few: " + ticker)
    write_cache("nasdaq_" + ticker + ".json", {"data": out}, PRICE_TTL)
    return out


def nasdaq_status(force=False):
    """SPY 市场状态 (开/收盘 + 最新价), 磁盘缓存 5 分钟."""
    cache = None if force else read_cache("nasdaq_status.json")
    if cache:
        return cache["data"]
    url = "https://api.nasdaq.com/api/quote/SPY/info?assetclass=etf"
    raw = http_get(url, headers={
        "Accept": "application/json",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
    })
    j = json.loads(raw.decode("utf-8", "ignore"))
    pd = (j.get("data") or {}).get("primaryData") or {}
    out = {
        "market_status": pd.get("marketStatus"),
        "last_price": pd.get("lastSalePrice"),
        "last_trade_ts": pd.get("lastTradeTimestamp"),
        "net_change": pd.get("netChange"),
    }
    write_cache("nasdaq_status.json", {"data": out}, STATUS_TTL)
    return out


def cnbc_quotes(force=False):
    """CNBC 报价: 美债收益率 / TIPS 实际利率 / VIX / SPX, 缓存 5 分钟."""
    cache = None if force else read_cache("cnbc_quotes.json")
    if cache:
        return cache["data"]
    syms = "US10Y|US2Y|US5Y|US30Y|US3M|US10YTIPS|VIX|.SPX"
    url = ("https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol"
           "?symbols={}&requestMethod=itv&noform=1&partnerId=2&fund=1"
           "&exthrs=1&ndf=1&output=json").format(syms)
    raw = http_get(url, timeout=15, retries=1)
    j = json.loads(raw.decode("utf-8", "ignore"))
    out = {}
    for q in (j.get("FormattedQuoteResult") or {}).get("FormattedQuote", []):
        s = q.get("symbol")
        last = q.get("last")
        if s and last not in (None, "", "N/A"):
            try:
                out[s] = float(str(last).replace("%", "").replace(",", ""))
            except Exception:
                pass
    if not out:
        raise RuntimeError("CNBC quotes empty")
    write_cache("cnbc_quotes.json", {"data": out}, STATUS_TTL)
    return out


def cboe_vix(force=False):
    """返回 [(date, close), ...] VIX 日线."""
    cache = None if force else read_cache("cboe_vix.json")
    if cache:
        return cache["data"]
    url = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
    text = http_get(url).decode("utf-8", "ignore")
    out = []
    for line in text.splitlines()[1:]:
        p = line.split(",")
        if len(p) >= 5:
            try:
                d = dt.datetime.strptime(p[0], "%m/%d/%Y").date()
                out.append([d.isoformat(), float(p[4])])
            except Exception:
                continue
    out.sort(key=lambda x: x[0])
    if len(out) < 30:
        raise RuntimeError("Cboe VIX rows too few")
    write_cache("cboe_vix.json", {"data": out}, SLOW_TTL)
    return out


def fred_series(sid, force=False):
    """返回 [(date, value), ...] FRED 序列."""
    cache = None if force else read_cache("fred_" + sid + ".json")
    if cache:
        return cache["data"]
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}".format(sid)
    text = http_get(url, timeout=12, retries=0).decode("utf-8", "ignore")
    out = []
    for line in text.splitlines()[1:]:
        p = line.split(",")
        if len(p) == 2 and p[1] not in ("", "."):
            try:
                d = dt.datetime.strptime(p[0], "%Y-%m-%d").date()
                out.append([d.isoformat(), float(p[1])])
            except Exception:
                continue
    out.sort(key=lambda x: x[0])
    if len(out) < 10:
        raise RuntimeError("FRED rows too few: " + sid)
    write_cache("fred_" + sid + ".json", {"data": out}, SLOW_TTL)
    return out


# ---------------------------------------------------------------------------
# 计算层
# ---------------------------------------------------------------------------

def series_value(rows, offset=0):
    """取最近第 offset 个观测值."""
    if not rows or len(rows) <= offset:
        return None
    return rows[-(1 + offset)][1]


def series_chg_bp(rows, days):
    """最近 days 个交易日的变动, 单位 bp."""
    if not rows or len(rows) <= days:
        return None
    return (series_value(rows) - series_value(rows, days)) * 100


def series_chg_pct(rows, days):
    if not rows or len(rows) <= days:
        return None
    return (series_value(rows) / series_value(rows, days) - 1) * 100


def zscores(vals):
    """横截面 z 分; 缺失值(空/None)给 0, 全等时给 0."""
    vs = [v for v in vals if v is not None]
    out = []
    if len(vs) >= 2:
        m = sum(vs) / len(vs)
        sd = math.sqrt(sum((x - m) ** 2 for x in vs) / len(vs))
        sd = sd if sd > 1e-12 else 1.0
        out = [(v - m) / sd if v is not None else 0.0 for v in vals]
    else:
        out = [0.0] * len(vals)
    return out


def load_all(force=False):
    """拉取/读取全部原始数据 (并发), 返回 dict; 单源失败不阻塞整体."""
    prices = {}
    errors = []

    def fetch_price(t):
        try:
            return t, nasdaq_chart(t, force), None
        except Exception as e:
            return t, None, str(e)[:100]

    with ThreadPoolExecutor(max_workers=6) as ex:
        for t, data, err in ex.map(fetch_price, TICKERS):
            prices[t] = data
            if err:
                errors.append("{}: {}".format(t, err))

    cnbc = None
    try:
        cnbc = cnbc_quotes(force)
    except Exception as e:
        errors.append("CNBC: {}".format(str(e)[:100]))

    vix = None
    if cnbc and cnbc.get("VIX") is not None:
        vix = cnbc["VIX"]
    else:
        try:
            v = cboe_vix(force)
            vix = series_value(v) if v else None
        except Exception as e:
            errors.append("VIX: {}".format(str(e)[:100]))

    rates = {}

    # FRED 负缓存: 上次全部失败则在 10 分钟内跳过, 避免拖慢刷新
    fred_down = read_cache("fred_down.json")
    if fred_down:
        for sid in FRED_SERIES:
            rates[sid] = None
            rates[sid + "_err"] = "down-cache"
        errors.append("FRED: 上次失败缓存(10分钟内跳过)")
    else:

        def fetch_rate(sid):
            try:
                return sid, fred_series(sid, force), None
            except Exception as e:
                return sid, None, str(e)[:120]

        any_ok = False
        with ThreadPoolExecutor(max_workers=5) as ex:
            for sid, rows, err in ex.map(fetch_rate, FRED_SERIES):
                rates[sid] = rows
                if err:
                    rates[sid + "_err"] = err
                    errors.append("FRED {}: {}".format(sid, err))
                else:
                    any_ok = True
        if not any_ok:
            write_cache("fred_down.json", {"down": True}, 600)

    status = None
    try:
        status = nasdaq_status(force)
    except Exception:
        status = None
    return {"prices": prices, "vix": vix, "rates": rates, "cnbc": cnbc,
            "status": status, "errors": errors}


def build_env(rates, vix, prices, cnbc):
    def pick(p, s):
        return p if p is not None else s

    dgs10 = pick(cnbc.get("US10Y") if cnbc else None, series_value(rates.get("DGS10")))
    dgs2 = pick(cnbc.get("US2Y") if cnbc else None, series_value(rates.get("DGS2")))
    real10y = pick(cnbc.get("US10YTIPS") if cnbc else None, series_value(rates.get("DFII10")))
    hy = series_value(rates.get("BAMLH0A0HYM2"))
    effr = series_value(rates.get("DFF"))
    us3m = cnbc.get("US3M") if cnbc else None
    spx = cnbc.get(".SPX") if cnbc else None

    spread = (dgs10 - dgs2) * 100 if (dgs10 is not None and dgs2 is not None) else None
    real10y_chg = series_chg_bp(rates.get("DFII10"), 63) if rates.get("DFII10") else None
    hy_chg = series_chg_bp(rates.get("BAMLH0A0HYM2"), 21) if rates.get("BAMLH0A0HYM2") else None

    # 信用利差代理: HYG/LQD 比值 1M 变动
    hy_proxy = None
    hyg, lqd = prices.get("HYG"), prices.get("LQD")
    if hyg and lqd and len(hyg) >= 22 and len(lqd) >= 22:
        r_now = hyg[-1][1] / lqd[-1][1]
        r_old = hyg[-22][1] / lqd[-22][1]
        hy_proxy = (r_now / r_old - 1) * 100

    easing_base = effr if effr is not None else us3m
    easing_proxy = effr is None and us3m is not None
    easing_gap = (dgs2 - easing_base) * 100 if (dgs2 is not None and easing_base is not None) else None

    votes = []

    # 1. 利率方向 (实际利率)
    if real10y is not None:
        if real10y_chg is not None and real10y_chg > 30:
            vote, tag = -1, "偏价值"
        elif real10y_chg is not None and real10y_chg < -30:
            vote, tag = 1, "偏成长"
        elif real10y > 2.5:
            vote, tag = -1, "偏价值"
        elif real10y < 1.8:
            vote, tag = 1, "偏成长"
        else:
            vote, tag = 0, "中性"
        votes.append({"key": "rate", "label": "利率方向",
                      "value": "10Y实际利率 {}% (3M {:s}bp)".format(
                          "%.2f" % real10y,
                          "{:+d}".format(int(real10y_chg)) if real10y_chg is not None else "--"),
                      "vote": vote, "tag": tag,
                      "detail": ">2.5% 或 3M 涨>30bp → 偏价值; <1.8% 或 3M 跌>30bp → 偏成长"})

    # 2. 收益率曲线
    if spread is not None:
        if spread < -40:
            vote, tag = -1, "防御"
        elif spread > 0:
            vote, tag = 1, "进攻"
        else:
            vote, tag = 0, "中性"
        votes.append({"key": "curve", "label": "收益率曲线 2s10s",
                      "value": "{:+.0f}bp".format(spread), "vote": vote, "tag": tag,
                      "detail": "<-40bp 衰退预警; >0 走陡进攻"})

    # 3. 波动率
    if vix is not None:
        if vix < 17:
            vote, tag = 1, "风险偏好高"
        elif vix > 25:
            vote, tag = -1, "恐慌"
        else:
            vote, tag = 0, "中性"
        votes.append({"key": "vix", "label": "VIX 波动率",
                      "value": "%.2f" % vix, "vote": vote, "tag": tag,
                      "detail": "<17 进攻; 17~25 中性; >25 防御"})

    # 4. 信用利差
    if hy_chg is not None:
        if hy_chg > 100:
            vote, tag = -1, "信用恶化"
        elif hy_chg < -50:
            vote, tag = 1, "信用改善"
        else:
            vote, tag = 0, "中性"
        votes.append({"key": "hy", "label": "高收益利差 1M变动",
                      "value": "{:+.0f}bp (现 {}%)".format(hy_chg, "%.2f" % hy if hy is not None else "--"),
                      "vote": vote, "tag": tag,
                      "detail": "1M扩大>100bp 信用恶化; 收窄<-50bp 改善"})
    elif hy_proxy is not None:
        if hy_proxy < -2:
            vote, tag = -1, "信用恶化"
        elif hy_proxy > 2:
            vote, tag = 1, "信用改善"
        else:
            vote, tag = 0, "中性"
        votes.append({"key": "hy", "label": "信用利差 (HYG/LQD 代理)",
                      "value": "1M {:+.1f}%".format(hy_proxy), "vote": vote,
                      "tag": tag + " ⚠",
                      "detail": "代理: 高收益跑输投资级 → 信用恶化 (FRED利差不可用)"})

    # 5. 宽松预期
    if easing_gap is not None:
        if easing_gap < -50:
            vote, tag = 1, "定价降息"
        else:
            vote, tag = 0, "中性"
        votes.append({"key": "easing",
                      "label": "宽松预期 (2Y−EFFR{})".format("·代理3M" if easing_proxy else ""),
                      "value": "{:+.0f}bp".format(easing_gap), "vote": vote,
                      "tag": tag + (" ⚠" if easing_proxy else ""),
                      "detail": "2Y显著低于短端利率 → 市场定价降息 → 偏成长"})

    total = sum(v["vote"] for v in votes)
    if total >= 2:
        vlabel, max_pos = "进攻", 100
    elif total == 1:
        vlabel, max_pos = "中性偏攻", 85
    elif total == 0:
        vlabel, max_pos = "中性", 70
    elif total == -1:
        vlabel, max_pos = "中性偏防", 55
    else:
        vlabel, max_pos = "防御", 40

    # 环境自适应权重
    if total >= 1:
        weights = {"rev": 0.45, "mom": 0.25, "trend": 0.15, "val": 0.05, "flow": 0.10}
    elif total <= -1:
        weights = {"rev": 0.20, "mom": 0.45, "trend": 0.20, "val": 0.10, "flow": 0.05}
    else:
        weights = {"rev": 0.35, "mom": 0.30, "trend": 0.15, "val": 0.10, "flow": 0.10}

    indicators = {
        "dgs10": rnd(dgs10, 3), "dgs2": rnd(dgs2, 3),
        "spread_2s10s": rnd(spread, 1), "real10y": rnd(real10y, 3),
        "real10y_chg3m": rnd(real10y_chg, 1), "hy_oas": rnd(hy, 3),
        "hy_oas_chg1m": rnd(hy_chg, 1), "effr": rnd(effr, 3),
        "us3m": rnd(us3m, 3), "vix": rnd(vix, 2),
        "easing_gap": rnd(easing_gap, 1), "hy_proxy": rnd(hy_proxy, 2),
        "spx": rnd(spx, 2),
    }

    # 市场广度: SPY 距 52 周高点回撤
    spy = prices.get("SPY")
    drawdown = None
    if spy and len(spy) >= 253:
        closes = [p[1] for p in spy]
        hi52 = max(closes[-253:])
        drawdown = (closes[-1] / hi52 - 1) * 100

    return {"verdict": total, "verdict_label": vlabel, "max_position": max_pos,
            "votes": votes, "indicators": indicators, "weights": weights,
            "spy_drawdown": rnd(drawdown, 2)}


def build_sectors(prices, overrides, env):
    spy = prices.get("SPY")
    spy_close_by_date = {}
    if spy:
        spy_close_by_date = {p[0]: p[1] for p in spy}

    def stats(rows):
        closes = [p[1] for p in rows]
        vols = [p[2] for p in rows]
        n = len(closes)
        if n < 30:
            return None
        out = {"close": closes[-1], "vol20": None, "ma200": None,
               "ma50": None, "above200": None, "mom20": None, "chg1m": None,
               "flow": None}
        if n >= 21:
            rets = [math.log(closes[i] / closes[i - 1]) for i in range(n - 20, n)]
            m = sum(rets) / len(rets)
            var = sum((x - m) ** 2 for x in rets) / len(rets)
            out["vol20"] = math.sqrt(var * 252) * 100
        if n >= 200:
            out["ma200"] = sum(closes[-200:]) / 200
            out["above200"] = closes[-1] > out["ma200"]
        if n >= 50:
            out["ma50"] = sum(closes[-50:]) / 50
        if n >= 21:
            out["mom20"] = (closes[-1] / closes[-21] - 1) * 100
        if n >= 22:
            out["chg1m"] = (closes[-1] / closes[-22] - 1) * 100
        if n >= 60:
            out["flow"] = (sum(vols[-5:]) / 5) / (sum(vols[-60:]) / 60)
        return out

    def rs_stats(rows):
        """RS = 板块价 / SPY价; 返回 (rs3m, rs6m, spark)."""
        if not spy_close_by_date:
            return None, None, []
        rs = []
        for d, c, _ in rows:
            s = spy_close_by_date.get(d)
            if s:
                rs.append(c / s)
        if len(rs) < 130:
            return None, None, []
        rs3m = (rs[-1] / rs[-64] - 1) * 100
        rs6m = (rs[-1] / rs[-127] - 1) * 100
        return rs3m, rs6m, [round(x, 4) for x in rs[-126:]]

    rows_out = []
    for ticker, name in SECTORS:
        rows = prices.get(ticker)
        if not rows:
            rows_out.append({"ticker": ticker, "name": name, "ok": False})
            continue
        st = stats(rows)
        if st is None:
            rows_out.append({"ticker": ticker, "name": name, "ok": False})
            continue
        rs3m, rs6m, spark = rs_stats(rows)
        # 手动输入
        rev = overrides.get("revision", {}).get(ticker, 0.0)
        pe = overrides.get("pe", {}).get(ticker)
        pe_manual = pe is not None
        # 估值分: PE 相对参考带的百分位 (高=贵), 得分高=便宜
        val_score = None
        if pe is not None:
            lo, hi = PE_BANDS.get(ticker, (10, 30))
            val_score = max(0.0, min(1.0, (hi - pe) / (hi - lo))) * 100
        rows_out.append({
            "ticker": ticker, "name": name, "ok": True,
            "close": rnd(st["close"], 2), "chg1m": rnd(st["chg1m"], 2),
            "rs3m": rnd(rs3m, 2), "rs6m": rnd(rs6m, 2),
            "above200": st["above200"], "ma200": rnd(st["ma200"], 2),
            "mom20": rnd(st["mom20"], 2), "vol20": rnd(st["vol20"], 2),
            "flow": rnd(st["flow"], 2), "pe": pe, "pe_manual": pe_manual,
            "rev": rev, "val_score": val_score, "spark": spark,
        })

    # ---- 因子 z 分与打分 ----
    ok = [r for r in rows_out if r.get("ok")]
    n = len(ok)
    z_mom = zscores([(0.5 * (r["rs3m"] or 0) + 0.5 * (r["rs6m"] or 0)) for r in ok])
    z_trend = zscores([((r["close"] / r["ma200"]) - 1) * 100 if r["ma200"] else 0 for r in ok])
    z_val = zscores([r["val_score"] for r in ok])
    z_flow = zscores([r["flow"] for r in ok])
    z_rev = zscores([r["rev"] for r in ok])

    weights = dict(env["weights"])
    has_val = any(r["val_score"] is not None for r in ok)
    if not has_val:
        weights["val"] = 0.0
    wsum = sum(weights.values()) or 1.0
    weights = {k: v / wsum for k, v in weights.items()}

    for i, r in enumerate(ok):
        zs = {"rev": z_rev[i], "mom": z_mom[i],
              "trend": z_trend[i], "val": z_val[i], "flow": z_flow[i]}
        cs = {k: weights[k] * zs[k] for k in zs}
        r["score"] = round(sum(cs.values()), 3)
        r["factors"] = {k: round(v, 3) for k, v in zs.items()}      # 各因子 z 分
        r["contrib"] = {k: round(v, 4) for k, v in cs.items()}      # 各因子加权贡献 w×z
        r["lead"] = max(cs, key=lambda k: abs(cs[k])) if any(cs.values()) else None  # 主导因子
        r["raw"] = {  # 原始因子值 (用于界面明细)
            "rev": r["rev"],
            "mom": round(0.5 * ((r.get("rs3m") or 0) + (r.get("rs6m") or 0)), 2),
            "trend": round((r["close"] / r["ma200"] - 1) * 100, 2) if r.get("ma200") else None,
            "val": round(r["val_score"], 1) if r.get("val_score") is not None else None,
            "flow": r.get("flow"),
        }

    ok.sort(key=lambda r: r["score"], reverse=True)
    for rank, r in enumerate(ok, 1):
        r["rank"] = rank
        if rank <= 3:
            sig = "买入候选"
        elif rank <= 5:
            sig = "关注"
        elif rank <= 8:
            sig = "观望"
        else:
            sig = "回避"
        if r.get("above200") is False and sig in ("买入候选", "关注"):
            sig = "观望"
        r["signal"] = sig

    # 风险平价建议权重: 前 3 名按 1/波动率 分配
    top3 = ok[:3]
    inv = [1.0 / max(r["vol20"], 5.0) for r in top3] if top3 else []
    isum = sum(inv) or 1.0
    for r in ok:
        r["suggest_weight"] = 0.0
    for r, iv in zip(top3, inv):
        r["suggest_weight"] = round(iv / isum * 100, 1)

    # 返回排序后的列表: 成功板块按总分降序, 数据缺失板块排末尾 (修复: 之前返回未排序的 rows_out 导致前端行序与排名不一致)
    failed = [r for r in rows_out if not r.get("ok")]
    return ok + failed, weights


def next_event(dates, hour_et, label, tone):
    now = dt.datetime.now(dt.timezone.utc)
    for d in dates:
        off = et_offset_for(d)
        ev_utc = dt.datetime(d.year, d.month, d.day, hour_et, 0,
                             tzinfo=dt.timezone.utc) + dt.timedelta(hours=-off)
        if ev_utc > now:
            hours = (ev_utc - now).total_seconds() / 3600
            return {"label": label, "date": d.isoformat(),
                    "hours_left": round(hours, 1), "tone": tone}
    return None


def build_risk(prices, rows_out, env, overrides):
    checks = []
    by = {r["ticker"]: r for r in rows_out if r.get("ok")}

    # 市场趋势
    spy = prices.get("SPY")
    if spy and len(spy) >= 200:
        closes = [p[1] for p in spy]
        ma200 = sum(closes[-200:]) / 200
        if closes[-1] > ma200:
            checks.append({"id": "mkt_trend", "label": "市场趋势 (SPY vs 200MA)",
                           "status": "ok", "detail": "SPY 站上 200 日均线, 多头市场"})
        else:
            checks.append({"id": "mkt_trend", "label": "市场趋势 (SPY vs 200MA)",
                           "status": "warn", "detail": "SPY 跌破 200 日均线, 空头市场, 谨慎进攻"})

    # 事件回避
    fomc = next_event(FOMC_2026, 14, "FOMC 决议", "event")
    cpi = next_event(CPI_2026, 8, "CPI 公布", "event")
    events = [e for e in (fomc, cpi) if e]
    for e in events:
        if e["hours_left"] < 24:
            checks.append({"id": "event_" + e["label"],
                           "label": "事件回避: {}".format(e["label"]),
                           "status": "warn",
                           "detail": "{} 小时后公布, 建议高贝塔板块减半仓".format(e["hours_left"])})
        else:
            checks.append({"id": "event_" + e["label"],
                           "label": "事件日历: {}".format(e["label"]),
                           "status": "ok",
                           "detail": "{} 小时后公布 ({} UTC)".format(e["hours_left"], e["date"])})

    # 持仓检查
    holdings = overrides.get("holdings", [])
    for t in holdings:
        r = by.get(t)
        if not r:
            continue
        msgs = []
        if r.get("above200") is False:
            msgs.append("跌破 200 日均线")
        if r.get("rank", 99) > 7:
            msgs.append("排名第 {} 名(出局线:7)".format(r["rank"]))
        if msgs:
            checks.append({"id": "hold_" + t, "label": "持仓 {}".format(t),
                           "status": "danger", "detail": "; ".join(msgs) + " → 卖出"})
        else:
            checks.append({"id": "hold_" + t, "label": "持仓 {}".format(t),
                           "status": "ok",
                           "detail": "趋势完好, 排名第 {} 名".format(r["rank"])})

    # 组合回撤
    pf = overrides.get("portfolio", {})
    pf_dd = None
    if pf.get("peak") and pf.get("current"):
        try:
            pf_dd = (float(pf["current"]) / float(pf["peak"]) - 1) * 100
        except Exception:
            pf_dd = None
    if pf_dd is not None:
        if pf_dd <= -15 + 1e-9:
            checks.append({"id": "pf_dd", "label": "组合回撤",
                           "status": "danger", "detail": "{:.1f}% → 清仓".format(pf_dd)})
        elif pf_dd <= -8 + 1e-9:
            checks.append({"id": "pf_dd", "label": "组合回撤",
                           "status": "warn", "detail": "{:.1f}% → 减半仓".format(pf_dd)})
        else:
            checks.append({"id": "pf_dd", "label": "组合回撤",
                           "status": "ok", "detail": "{:.1f}% (阈值: 8%/15%)".format(pf_dd)})

    # 市场回撤
    md = env.get("spy_drawdown")
    if md is not None:
        if md <= -10 + 1e-9:
            checks.append({"id": "mkt_dd", "label": "市场回撤 (SPY 52周高点)",
                           "status": "warn", "detail": "{:.1f}% → 注意防御".format(md)})
        else:
            checks.append({"id": "mkt_dd", "label": "市场回撤 (SPY 52周高点)",
                           "status": "ok", "detail": "{:.1f}%".format(md)})

    # 信用恶化
    hy_chg = env["indicators"].get("hy_oas_chg1m")
    if hy_chg is not None and hy_chg > 100:
        checks.append({"id": "hy_widen", "label": "信用利差恶化",
                       "status": "danger", "detail": "1M 扩大 {:+.0f}bp → 全面收缩仓位".format(hy_chg)})
    else:
        hp = env["indicators"].get("hy_proxy")
        if hp is not None and hp < -2:
            checks.append({"id": "hy_widen", "label": "信用利差恶化 (代理)",
                           "status": "danger", "detail": "HYG/LQD 1M {:.1f}% → 全面收缩仓位".format(hp)})

    # 换手上限
    sell_count = sum(1 for r in rows_out if r.get("signal") in ("回避", "观望") and r.get("above200") is False)
    if sell_count > 3:
        checks.append({"id": "turnover", "label": "换手提示",
                       "status": "warn",
                       "detail": "本周期建议卖出 {} 个板块, 注意控制换手".format(sell_count)})

    return {"checks": checks, "events": events,
            "portfolio_drawdown": rnd(pf_dd, 2), "market_drawdown": md,
            "suggested_turnover": sell_count}


def calc_effective_position(env, risk):
    """有效仓位 = 环境建议仓位 × 风控系数 (取最低系数).
    风控系数来源: 组合回撤>8% ×0.5 / >15% ×0 ; 市场跌破200日线 ×0.5 ;
    事件24h内 ×0.8 ; 信用利差恶化 ×0.6"""
    base = env.get("max_position", 70)
    coeff = 1.0
    reasons = []

    pf_dd = risk.get("portfolio_drawdown")
    if pf_dd is not None:
        if pf_dd <= -15 + 1e-9:
            coeff = min(coeff, 0.0)
            reasons.append("组合回撤 {:.1f}% 超 −15% → 清仓".format(pf_dd))
        elif pf_dd <= -8 + 1e-9:
            coeff = min(coeff, 0.5)
            reasons.append("组合回撤 {:.1f}% 超 −8% → 减半".format(pf_dd))

    for c in risk.get("checks", []):
        if c["id"] == "mkt_trend" and c["status"] == "warn":
            coeff = min(coeff, 0.5)
            reasons.append("SPY 跌破 200 日均线（空头市场）")
        if c["id"] == "hy_widen" and c["status"] == "danger":
            coeff = min(coeff, 0.6)
            reasons.append("信用利差恶化")

    for e in risk.get("events", []):
        if e["hours_left"] < 24:
            coeff = min(coeff, 0.8)
            reasons.append("{} 将于 {:.0f}h 后公布（事件回避）".format(e["label"], e["hours_left"]))

    effective = round(base * coeff)
    return {"base": base, "coeff": round(coeff, 2), "effective": effective, "reasons": reasons}


def build_overview(force=False):
    if os.path.exists(DEMO_MARKER):
        try:
            with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
                snap = json.load(f)
            snap["mode"] = "demo"
            return snap
        except Exception:
            pass
    try:
        data = load_all(force)
        mode = "live"
    except Exception as e:
        # 降级: 快照 / 演示
        if os.path.exists(SNAPSHOT_FILE):
            with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
                snap = json.load(f)
            snap["mode"] = "cached"
            snap["degraded_reason"] = str(e)[:200]
            return snap
        if os.path.exists(DEMO_SNAPSHOT_FILE):
            with open(DEMO_SNAPSHOT_FILE, "r", encoding="utf-8") as f:
                snap = json.load(f)
            snap["mode"] = "demo"
            snap["degraded_reason"] = str(e)[:200]
            return snap
        raise

    overrides = load_overrides()
    env = build_env(data["rates"], data["vix"], data["prices"], data["cnbc"])
    sectors, weights = build_sectors(data["prices"], overrides, env)
    risk = build_risk(data["prices"], sectors, env, overrides)
    eff = calc_effective_position(env, risk)

    now = dt.datetime.now(dt.timezone.utc)
    et = et_now()

    sources = [
        {"name": "CNBC (收益率/VIX/SPX)", "purpose": "10Y/2Y/3M/TIPS实际利率/VIX/SPX",
         "status": "ok" if data["cnbc"] else "err",
         "updated": "缓存5min", "note": "真实行情, 主源"},
        {"name": "Nasdaq API (板块日线)", "purpose": "13 只 ETF 近2年日线",
         "status": "ok", "updated": "缓存3h", "note": "真实行情, 含HYG/LQD"},
        {"name": "FRED (利差/EFFR/备份利率)", "purpose": "HY利差/EFFR/国债备份",
         "status": "ok" if (data["rates"].get("BAMLH0A0HYM2") or data["rates"].get("DFF")) else "err",
         "updated": "缓存6h", "note": "当前不可用时自动切换代理"},
        {"name": "信用利差代理 (HYG/LQD)", "purpose": "FRED利差不可用时的降级",
         "status": "proxy", "updated": "自动", "note": "HYG/LQD 比值1M变动(近似)"},
        {"name": "盈利修正广度", "purpose": "分析师预期修正", "status": "manual",
         "updated": "手动", "note": "默认0, 请手动更新(生产接FactSet)"},
        {"name": "估值 PE", "purpose": "估值因子", "status": "manual",
         "updated": "手动", "note": "缺失时用静态参考带(近似)"},
        {"name": "资金流(量能代理)", "purpose": "资金流因子", "status": "proxy",
         "updated": "自动", "note": "5日/60日均量比(近似)"},
    ]

    payload = {
        "mode": mode,
        "generated_at": now.isoformat(),
        "asof": (data["prices"].get("SPY") or [["--"]])[-1][0],
        "et_now": fmt_dt(et),
        "local_now": fmt_dt(dt.datetime.now()),
        "market_status": (data["status"] or {}).get("market_status"),
        "spy_last": (data["status"] or {}).get("last_price"),
        "spy_last_ts": (data["status"] or {}).get("last_trade_ts"),
        "env": env,
        "sectors": sectors,
        "sector_weights": weights,   # 实际生效权重 (归一化后, 估值禁用时 val=0)
        "risk": risk,
        "effective_position": eff,
        "sources": sources,
        "warnings": data["errors"],
        "overrides": overrides,
    }
    with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return payload


# ---------------------------------------------------------------------------
# overrides 持久化
# ---------------------------------------------------------------------------

def load_overrides():
    if os.path.exists(OVERRIDES_FILE):
        try:
            with open(OVERRIDES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_overrides(o):
    with open(OVERRIDES_FILE, "w", encoding="utf-8") as f:
        json.dump(o, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# HTTP 服务
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # 精简日志
        print("[%s] %s" % (time.strftime("%H:%M:%S"), fmt % args), flush=True)

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        data = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?")[0]
        query = urllib.parse.parse_qs(self.path.split("?", 1)[1] if "?" in self.path else "")
        try:
            if path in ("/", "/index.html"):
                with open(os.path.join(BASE, "monitor.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            elif path == "/api/overview":
                force = query.get("refresh", ["0"])[0] == "1"
                self._send(200, json.dumps(build_overview(force), ensure_ascii=False))
            elif path == "/api/overrides":
                self._send(200, json.dumps(load_overrides(), ensure_ascii=False))
            elif path == "/api/ping":
                self._send(200, json.dumps({"ok": True, "time": fmt_dt(dt.datetime.now())}))
            else:
                self._send(404, json.dumps({"error": "not found"}))
        except Exception as e:  # noqa: BLE001
            self._send(500, json.dumps({"error": str(e)[:300]}))

    def do_POST(self):
        path = self.path.split("?")[0]
        try:
            if path == "/api/overrides":
                n = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(n) if n else b"{}"
                patch = json.loads(raw.decode("utf-8"))
                cur = load_overrides()
                for k in ("revision", "pe", "holdings", "portfolio"):
                    if k in patch:
                        cur[k] = patch[k]
                save_overrides(cur)
                self._send(200, json.dumps({"ok": True, "overrides": cur}, ensure_ascii=False))
            else:
                self._send(404, json.dumps({"error": "not found"}))
        except Exception as e:  # noqa: BLE001
            self._send(400, json.dumps({"error": str(e)[:300]}))


def main():
    port = 8077
    demo = False
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--port" and i + 1 < len(args):
            port = int(args[i + 1]); i += 2
        elif args[i] == "--demo":
            demo = True; i += 1
        else:
            i += 1

    if demo:
        # 演示模式: 用内置示例快照, 并写标记使 API 直接返回演示数据
        payload = None
        if os.path.exists(DEMO_SNAPSHOT_FILE):
            with open(DEMO_SNAPSHOT_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)
        if payload:
            with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            with open(DEMO_MARKER, "w", encoding="utf-8") as f:
                json.dump({"demo": True}, f)
            print("[demo] 已装载演示快照 -> snapshot.json (演示模式)")
    else:
        if os.path.exists(DEMO_MARKER):
            os.remove(DEMO_MARKER)

    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print("=" * 62)
    print("  美股板块轮动实时监控服务  US Sector Rotation Monitor")
    print("  访问: http://127.0.0.1:{}".format(port))
    print("  数据: Nasdaq(板块) / Cboe(VIX) / FRED(利率)  纯标准库")
    print("  说明文档: README.md   按 Ctrl+C 停止")
    print("=" * 62, flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n服务已停止.")
        srv.server_close()


if __name__ == "__main__":
    main()
