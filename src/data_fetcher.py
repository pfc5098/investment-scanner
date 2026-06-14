"""Shared, concurrent Alpha Vantage data fetcher.

Both scanners (general + opportunity) need the SAME raw data per symbol
(overview, quote, RSI, balance sheet, cash flow, income statement). Previously
each scanner re-scanned all ~8,400 active stocks sequentially, so the GitHub
Actions job blew past the 6-hour runner cap and was cancelled before finishing.

This module fetches everything ONCE, concurrently (bounded to the API's
requests-per-minute limit), and caches the result to a dated JSON file so the
second scanner in the same run reuses the first scanner's fetch instead of
hitting the API again.
"""

import os
import json
import time
import logging
import threading
from io import StringIO
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

BASE_URL = "https://www.alphavantage.co/query"
MARKET_CAP_FLOOR = 10_000_000_000  # $10B


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


class RateLimiter:
    """Thread-safe sliding-window limiter: at most ``max_per_min`` acquisitions
    in any rolling 60-second window."""

    def __init__(self, max_per_min):
        self.max_per_min = max_per_min
        self._lock = threading.Lock()
        self._calls = deque()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                while self._calls and now - self._calls[0] >= 60:
                    self._calls.popleft()
                if len(self._calls) < self.max_per_min:
                    self._calls.append(now)
                    return
                sleep_for = 60 - (now - self._calls[0])
            time.sleep(max(sleep_for, 0.01))


class FetchClient:
    """Alpha Vantage client safe to call from many threads at once."""

    def __init__(self, api_key, max_req_per_min=300):
        self.api_key = api_key
        self.limiter = RateLimiter(max_req_per_min)
        self.session = requests.Session()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _request(self, params):
        self.limiter.acquire()
        params = {**params, "apikey": self.api_key}
        response = self.session.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()

        ctype = response.headers.get("Content-Type", "")
        data = response.json() if "json" in ctype else response.text
        if isinstance(data, dict) and "Information" in data and "rate limit" in data["Information"].lower():
            logger.warning("Rate limit hit from API response, backing off...")
            raise Exception("Rate limit hit")
        return data

    def active_listings(self):
        self.limiter.acquire()
        response = self.session.get(
            BASE_URL,
            params={"function": "LISTING_STATUS", "state": "active", "apikey": self.api_key},
            timeout=60,
        )
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        return df[df["assetType"] == "Stock"]["symbol"].tolist()

    def overview(self, symbol):
        return self._request({"function": "OVERVIEW", "symbol": symbol})

    def quote(self, symbol):
        data = self._request({"function": "GLOBAL_QUOTE", "symbol": symbol})
        q = data.get("Global Quote", {})
        return {
            "Price": q.get("05. price", None),
            "Volume": q.get("06. volume", None),
            "Latest Trading Day": q.get("07. latest trading day", None),
        }

    def rsi(self, symbol):
        data = self._request({
            "function": "RSI", "symbol": symbol, "interval": "daily",
            "time_period": 14, "series_type": "close",
        })
        ta = data.get("Technical Analysis: RSI", {})
        dates = list(ta.keys())
        return ta[dates[0]].get("RSI") if dates else None

    def balance_sheet(self, symbol):
        return self._request({"function": "BALANCE_SHEET", "symbol": symbol})

    def cash_flow(self, symbol):
        return self._request({"function": "CASH_FLOW", "symbol": symbol})

    def income_statement(self, symbol):
        return self._request({"function": "INCOME_STATEMENT", "symbol": symbol})


def build_dataset(api_key, max_req_per_min=300, workers=16):
    """Return ``{symbol: {overview, quote, rsi, balance_sheet, cash_flow,
    income_statement}}`` for every large-cap survivor, fetched concurrently.

    Honors the existing ``SYMBOL_LIST`` / ``SYMBOL_LIMIT`` test env vars (which
    bypass the market-cap screen). In production it caches to
    ``data/raw_cache_<UTC-date>.json`` so a second invocation in the same day
    reuses the fetch instead of re-hitting the API.
    """
    limit = os.environ.get("SYMBOL_LIMIT")
    symbol_list_env = os.environ.get("SYMBOL_LIST")
    is_test = bool(limit or symbol_list_env)

    cache_path = f"data/raw_cache_{datetime.now(timezone.utc).strftime('%Y%m%d')}.json"
    if not is_test and os.path.exists(cache_path):
        logger.info(f"Reusing cached dataset from {cache_path} (skipping API fetch)")
        with open(cache_path) as f:
            return json.load(f)

    client = FetchClient(api_key, max_req_per_min=max_req_per_min)

    if symbol_list_env:
        symbols = [s.strip() for s in symbol_list_env.split(",") if s.strip()]
        logger.info(f"Using provided SYMBOL_LIST: {symbols}")
    else:
        logger.info("Fetching active US equity listings...")
        symbols = client.active_listings()
        logger.info(f"Found {len(symbols)} active US Equities.")
        if limit:
            symbols = symbols[: int(limit)]
            logger.info(f"Limiting to {limit} symbols for testing.")

    # Phase 1: concurrent overviews, then screen by market cap.
    def _overview(sym):
        try:
            return sym, client.overview(sym)
        except Exception as e:
            logger.error(f"Overview error for {sym}: {e}")
            return sym, None

    logger.info(f"Phase 1: fetching {len(symbols)} overviews with {workers} workers...")
    overviews = {}
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for sym, ov in ex.map(_overview, symbols):
            overviews[sym] = ov
            done += 1
            if done % 250 == 0:
                logger.info(f"  ...overviews {done}/{len(symbols)}")

    survivors = []
    skipped_small = 0
    for sym in symbols:
        ov = overviews.get(sym)
        if not ov:
            continue
        if is_test:
            survivors.append(sym)
            continue
        if ov.get("Industry", "").upper() == "SHELL COMPANIES":
            continue
        if _safe_float(ov.get("MarketCapitalization")) < MARKET_CAP_FLOOR:
            skipped_small += 1
            continue
        survivors.append(sym)

    logger.info(
        f"Phase 1 done: {len(survivors)} survivors "
        f"(skipped {skipped_small} sub-$10B of {len(symbols)})."
    )

    # Phase 2: concurrent full fetch for survivors only.
    def _full(sym):
        try:
            return sym, {
                "overview": overviews[sym],
                "quote": client.quote(sym),
                "rsi": client.rsi(sym),
                "balance_sheet": client.balance_sheet(sym),
                "cash_flow": client.cash_flow(sym),
                "income_statement": client.income_statement(sym),
            }
        except Exception as e:
            logger.error(f"Fetch error for {sym}: {e}")
            return sym, None

    logger.info(f"Phase 2: fetching financials for {len(survivors)} survivors...")
    dataset = {}
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for sym, payload in ex.map(_full, survivors):
            if payload:
                dataset[sym] = payload
            done += 1
            if done % 100 == 0:
                logger.info(f"  ...financials {done}/{len(survivors)}")

    if not is_test:
        os.makedirs("data", exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump(dataset, f)
        logger.info(f"Cached {len(dataset)} symbols to {cache_path}")

    return dataset
