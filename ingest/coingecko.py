from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
import requests

API_KEY = os.getenv("COINGECKO_API_KEY", "")
BRONZE_PATH = Path("data/bronze/coingecko")
COINS = ["bitcoin", "ethereum", "binancecoin"]
BASE_URL = "https://api.coingecko.com/api/v3"
HEADERS = {
    "accept": "application/json",
    "x-cg-demo-api-key": API_KEY,
}


def fetch_current_prices() -> dict:
    ids = ",".join(COINS)
    url = f"{BASE_URL}/simple/price"
    params = {
        "ids": ids,
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
    }
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_market_chart(coin_id: str, days: int = 30) -> dict:
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days, "interval": "daily"}
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def run():
    print("=== Ingestion Bronze — CoinGecko ===")
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Prix actuels
    print("Fetch prix actuels...")
    prices = fetch_current_prices()
    out = BRONZE_PATH / f"prices_{ts}.json"
    out.write_text(json.dumps(prices, indent=2))
    print(f"[OK] {out.name}")

    time.sleep(2)

    # Historique 30 jours par coin
    for coin in COINS:
        print(f"Fetch historique {coin}...")
        try:
            chart = fetch_market_chart(coin, days=30)
            out = BRONZE_PATH / f"chart_{coin}_{ts}.json"
            out.write_text(json.dumps(chart, indent=2))
            print(f"[OK] {out.name}")
            time.sleep(2)
        except Exception as e:
            print(f"[ERREUR] {coin} : {e}")

    print(f"\nDonnees dans : {BRONZE_PATH}")


if __name__ == "__main__":
    run()