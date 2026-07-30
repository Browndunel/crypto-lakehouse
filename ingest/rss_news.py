from datetime import datetime, timezone
import json
from pathlib import Path
import time
import urllib.request
import xml.etree.ElementTree as ET

BRONZE_PATH = Path("data/bronze/news")
RSS_FEEDS = {
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph": "https://cointelegraph.com/rss",
    "decrypt": "https://decrypt.co/feed",
}
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"}


def fetch_rss(name: str, url: str) -> list:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read()

        root = ET.fromstring(content)
        channel = root.find("channel")
        if channel is None:
            return []

        articles = []
        for item in channel.findall("item"):
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            pub_date = item.findtext("pubDate", "").strip()
            description = item.findtext("description", "").strip()

            if title:
                articles.append(
                    {
                        "source": name,
                        "title": title,
                        "link": link,
                        "pub_date": pub_date,
                        "description": description,
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

        print(f"[OK] {name} : {len(articles)} articles")
        return articles

    except Exception as e:
        print(f"[ERREUR] {name} : {e}")
        return []


def run():
    print("=== Ingestion Bronze — RSS News ===")
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    all_articles = []

    for name, url in RSS_FEEDS.items():
        articles = fetch_rss(name, url)
        all_articles.extend(articles)
        time.sleep(1)

    out = BRONZE_PATH / f"news_{ts}.json"
    out.write_text(json.dumps(all_articles, indent=2, ensure_ascii=False))
    print(f"\n{len(all_articles)} articles sauvegardes dans {out.name}")


if __name__ == "__main__":
    run()