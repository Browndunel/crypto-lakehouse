from datetime import datetime, timedelta
import os
from pathlib import Path
import time
import zipfile
import requests

SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
INTERVAL = "1h"
BRONZE_PATH = Path("data/bronze/ohlcv")
BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"}


def download_monthly(symbol: str, year: int, month: int) -> bool:
    filename = f"{symbol}-{INTERVAL}-{year}-{month:02d}.zip"
    url = f"{BASE_URL}/{symbol}/{INTERVAL}/{filename}"
    out_dir = BRONZE_PATH / symbol

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_file = out_dir / filename.replace(".zip", ".csv")

    if csv_file.exists():
        print(f"[SKIP] {csv_file.name} deja present")
        return True

    try:
        print(f"[DOWNLOAD] {filename}...")
        resp = requests.get(url, headers=HEADERS, timeout=60)
        
        if resp.status_code == 404:
            print(f"[SKIP] {filename} non disponible")
            return False
            
        resp.raise_for_status()

        zip_path = out_dir / filename
        zip_path.write_bytes(resp.content)

        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(out_dir)

        zip_path.unlink()
        print(f"[OK] {csv_file.name}")
        time.sleep(0.5)
        return True

    except Exception as e:
        print(f"[ERREUR] {filename} : {e}")
        return False


def run():
    print("=== Ingestion Bronze — Binance OHLCV ===")
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    end = datetime.now()
    start = end - timedelta(days=730)
    total = 0
    success = 0
    current = start.replace(day=1)

    while current <= end:
        for symbol in SYMBOLS:
            total += 1
            if download_monthly(symbol, current.year, current.month):
                success += 1

        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    print(f"\n{success}/{total} fichiers telecharges")
    print(f"Donnees dans : {BRONZE_PATH}")


if __name__ == "__main__":
    run()