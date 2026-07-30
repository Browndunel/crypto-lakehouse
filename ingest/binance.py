import os
import time
import zipfile
import requests
from datetime import datetime, timedelta
from pathlib import Path

SYMBOLS   = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT",
             "XRPUSDT", "DOTUSDT", "DOGEUSDT", "AVAXUSDT", "MATICUSDT"]
INTERVALS = ["1m", "5m", "1h"]
BRONZE_PATH = Path("data/bronze/ohlcv")
BASE_URL    = "https://data.binance.vision/data/spot/monthly/klines"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
}


def download_monthly(symbol: str, interval: str,
                     year: int, month: int) -> bool:
    filename = f"{symbol}-{interval}-{year}-{month:02d}.zip"
    url      = f"{BASE_URL}/{symbol}/{interval}/{filename}"
    out_dir  = BRONZE_PATH / symbol / interval
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_file = out_dir / filename.replace(".zip", ".csv")

    if csv_file.exists():
        return True

    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        if resp.status_code == 404:
            return False
        resp.raise_for_status()

        zip_path = out_dir / filename
        zip_path.write_bytes(resp.content)
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(out_dir)
        zip_path.unlink()
        print(f"[OK] {symbol}/{interval}/{filename.replace('.zip','.csv')}")
        time.sleep(0.3)
        return True

    except Exception as e:
        print(f"[ERREUR] {filename} : {e}")
        return False


def run():
    print("=== Ingestion Bronze — Binance OHLCV ===")
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    end   = datetime.now()
    start = end - timedelta(days=1095)

    total = 0
    success = 0

    current = start.replace(day=1)
    while current <= end:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                total += 1
                if download_monthly(symbol, interval,
                                    current.year, current.month):
                    success += 1
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    print(f"\n{success}/{total} fichiers telecharges")
    du = os.popen("du -sh data/bronze/").read().strip()
    print(f"Volume Bronze : {du}")


if __name__ == "__main__":
    run()