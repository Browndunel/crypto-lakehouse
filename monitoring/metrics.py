import os
import time
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

import psycopg2

POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "postgres")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT",     "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "crypto_gold")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "crypto")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto123")
METRICS_PORT      = int(os.getenv("METRICS_PORT",  "8000"))

BRONZE_PATH = Path("/work/data/bronze")


def count_bronze_files() -> dict:
    result = {}
    for subdir in ["ohlcv", "news", "coingecko"]:
        path = BRONZE_PATH / subdir
        if path.exists():
            files = list(path.rglob("*"))
            result[subdir] = {
                "files": len([f for f in files if f.is_file()]),
                "size_bytes": sum(f.stat().st_size for f in files if f.is_file()),
            }
        else:
            result[subdir] = {"files": 0, "size_bytes": 0}
    return result


def get_postgres_metrics() -> dict:
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=int(POSTGRES_PORT),
            dbname=POSTGRES_DB, user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cur = conn.cursor()

        counts = {}
        for table in ["daily_market_kpi", "sentiment_daily_kpi",
                      "price_prediction_result", "correlation_insight",
                      "pipeline_metrics"]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]

        cur.execute(
            "SELECT layer, AVG(duration_s), SUM(rows_in), SUM(rows_out) "
            "FROM pipeline_metrics GROUP BY layer"
        )
        pipeline = cur.fetchall()

        cur.close()
        conn.close()
        return {"counts": counts, "pipeline": pipeline, "error": None}

    except Exception as e:
        return {"counts": {}, "pipeline": [], "error": str(e)}


def build_metrics() -> str:
    lines = []

    # Metriques Bronze
    bronze = count_bronze_files()
    lines.append("# HELP bronze_file_count Nombre de fichiers par sous-couche Bronze")
    lines.append("# TYPE bronze_file_count gauge")
    lines.append("# HELP bronze_size_bytes Taille en bytes par sous-couche Bronze")
    lines.append("# TYPE bronze_size_bytes gauge")
    for subdir, info in bronze.items():
        lines.append(f'bronze_file_count{{layer="{subdir}"}} {info["files"]}')
        lines.append(f'bronze_size_bytes{{layer="{subdir}"}} {info["size_bytes"]}')

    # Metriques Gold PostgreSQL
    pg = get_postgres_metrics()
    if pg["error"] is None:
        lines.append("# HELP gold_table_rows Nombre de lignes par table Gold")
        lines.append("# TYPE gold_table_rows gauge")
        for table, count in pg["counts"].items():
            lines.append(f'gold_table_rows{{table="{table}"}} {count}')

        lines.append("# HELP pipeline_duration_seconds Duree moyenne par couche")
        lines.append("# TYPE pipeline_duration_seconds gauge")
        lines.append("# HELP pipeline_rows_in Lignes lues par couche")
        lines.append("# TYPE pipeline_rows_in gauge")
        lines.append("# HELP pipeline_rows_out Lignes produites par couche")
        lines.append("# TYPE pipeline_rows_out gauge")
        for layer, avg_dur, rows_in, rows_out in pg["pipeline"]:
            lines.append(f'pipeline_duration_seconds{{layer="{layer}"}} {round(avg_dur, 2)}')
            lines.append(f'pipeline_rows_in{{layer="{layer}"}} {rows_in}')
            lines.append(f'pipeline_rows_out{{layer="{layer}"}} {rows_out}')
    else:
        lines.append(f"# ERREUR PostgreSQL : {pg['error']}")

    return "\n".join(lines) + "\n"


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            body = build_metrics().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def run():
    print(f"Serveur de metriques sur http://0.0.0.0:{METRICS_PORT}/metrics")
    server = HTTPServer(("0.0.0.0", METRICS_PORT), MetricsHandler)
    server.serve_forever()


if __name__ == "__main__":
    run()