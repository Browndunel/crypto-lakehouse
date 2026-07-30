"""
tests/test_e2e.py

Tests de bout en bout : suppose que `make up` et `make pipeline` ont déjà tourné.
Ne mocke rien - vérifie l'état réel de l'infra et des données.

Lancement :
  pip install pytest psycopg2-binary requests
  pytest tests/test_e2e.py -v
"""

import os
import subprocess

import psycopg2
import pytest
import requests

PG_CONFIG = {
    "host": "localhost",  # tests lancés depuis l'hôte Codespace, pas depuis un container
    "port": os.environ.get("POSTGRES_PORT", "5432"),
    "dbname": os.environ.get("POSTGRES_DB", "crypto_warehouse"),
    "user": os.environ.get("POSTGRES_USER", "crypto_admin"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

GRAFANA_URL = f"http://localhost:{os.environ.get('GRAFANA_PORT', '3000')}"
PROMETHEUS_URL = f"http://localhost:{os.environ.get('PROMETHEUS_PORT', '9090')}"
HDFS_WEBUI_URL = f"http://localhost:{os.environ.get('HDFS_NAMENODE_WEBUI_PORT', '9870')}"

GOLD_TABLES = [
    "daily_market_kpi",
    "sentiment_daily_kpi",
    "price_prediction_result",
    "correlation_insight",
]


@pytest.fixture(scope="module")
def pg_conn():
    conn = psycopg2.connect(**PG_CONFIG)
    yield conn
    conn.close()


# ============================================
# 1. Infrastructure de base
# ============================================
def test_postgres_reachable(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1


def test_hdfs_namenode_reachable():
    resp = requests.get(f"{HDFS_WEBUI_URL}/jmx", timeout=5)
    assert resp.status_code == 200


def test_prometheus_healthy():
    resp = requests.get(f"{PROMETHEUS_URL}/-/healthy", timeout=5)
    assert resp.status_code == 200


def test_grafana_healthy():
    resp = requests.get(f"{GRAFANA_URL}/api/health", timeout=5)
    assert resp.status_code == 200
    assert resp.json().get("database") == "ok"


# ============================================
# 2. Présence des données Silver (HDFS)
# ============================================
@pytest.mark.parametrize("dataset", ["clean_candles", "clean_reddit_posts", "clean_news_articles"])
def test_silver_dataset_exists_in_hdfs(dataset):
    silver_path = f"{os.environ.get('HDFS_SILVER_PATH', 'hdfs://namenode:9000/datalake/silver')}/{dataset}"
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-test", "-d", silver_path],
        capture_output=True,
    )
    assert result.returncode == 0, f"Dataset Silver manquant: {silver_path}"


# ============================================
# 3. Tables Gold peuplées
# ============================================
@pytest.mark.parametrize("table", GOLD_TABLES)
def test_gold_table_populated(pg_conn, table):
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
    assert count > 0, f"Table Gold vide: {table} - lancer `make gold` d'abord"


# ============================================
# 4. Traçabilité (lineage) - exigence explicite du prof
# ============================================
def test_batch_lineage_has_entries(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM batch_lineage")
        assert cur.fetchone()[0] > 0


def test_every_gold_row_has_traceable_lineage(pg_conn):
    """Chaque ligne Gold doit référencer un batch_id qui existe réellement."""
    with pg_conn.cursor() as cur:
        for table in GOLD_TABLES:
            cur.execute(
                f"""SELECT COUNT(*) FROM {table} t
                    LEFT JOIN batch_lineage b ON t.source_batch_id = b.batch_id
                    WHERE b.batch_id IS NULL"""
            )
            orphans = cur.fetchone()[0]
            assert orphans == 0, f"{table}: {orphans} lignes sans lineage traçable"


def test_no_failed_gold_batches_in_latest_run(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            """SELECT status FROM batch_lineage
               WHERE layer = 'gold'
               ORDER BY started_at DESC LIMIT 1"""
        )
        row = cur.fetchone()
        assert row is not None, "Aucun batch Gold trouvé - le pipeline a-t-il tourné ?"
        assert row[0] == "success", f"Dernier batch Gold en échec: status={row[0]}"


# ============================================
# 5. Indexation (exigence explicite du prof)
# ============================================
@pytest.mark.parametrize("table,index", [
    ("daily_market_kpi", "idx_market_kpi_symbol_day"),
    ("price_prediction_result", "idx_prediction_symbol_date"),
    ("correlation_insight", "idx_correlation_symbol_day"),
])
def test_gold_indexes_exist(pg_conn, table, index):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_indexes WHERE tablename = %s AND indexname = %s",
            (table, index),
        )
        assert cur.fetchone() is not None, f"Index manquant: {index} sur {table}"


# ============================================
# 6. Cohérence métier basique
# ============================================
def test_predictions_have_plausible_values(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM price_prediction_result WHERE predicted_close <= 0"
        )
        assert cur.fetchone()[0] == 0, "Des prix prédits sont négatifs ou nuls"


def test_custom_metrics_exporter_reachable():
    port = os.environ.get("METRICS_EXPORTER_PORT", "9200")
    resp = requests.get(f"http://localhost:{port}/metrics", timeout=5)
    assert resp.status_code == 200
    assert "datalake_records_processed" in resp.text