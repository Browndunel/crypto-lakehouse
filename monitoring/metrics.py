"""
monitoring/metrics.py

Exporteur Prometheus custom pour le data lake.

Ne remplace PAS Node Exporter (CPU/RAM/disque, géré par Personne A) :
expose des métriques MÉTIER lues dans PostgreSQL (batch_lineage, quality_metrics) :
  - nombre de records traités par couche/job
  - durée des jobs
  - taux de succès
  - métriques de qualité (nulls, doublons, invalides) par dataset

Répond à l'exigence "Opérations par couche" + "Logs de transformation"
(section Monitoring & Observabilité du sujet).

Lancement (indépendant des jobs Spark, tourne en continu) :
  python3 monitoring/metrics.py

A ajouter par Personne A dans prometheus/prometheus.yml :
  - job_name: "datalake-custom-metrics"
    static_configs:
      - targets: ["custom-metrics:9200"]
"""

import os
import sys
import time

import psycopg2
import psycopg2.extras
from prometheus_client import start_http_server, Gauge

POLL_INTERVAL_SECONDS = int(os.environ.get("METRICS_POLL_INTERVAL_SECONDS", "30"))
METRICS_PORT = int(os.environ.get("METRICS_EXPORTER_PORT", "9200"))

# ============================================
# Config Postgres depuis l'environnement
# ============================================
def get_pg_config():
    required = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print(f"ERREUR: variables d'environnement manquantes: {missing}")
        sys.exit(1)
    return {
        "host": os.environ["POSTGRES_HOST"],
        "port": os.environ["POSTGRES_PORT"],
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }


# ============================================
# Métriques Prometheus exposées
# ============================================
records_processed = Gauge(
    "datalake_records_processed",
    "Nombre de records traités lors du dernier batch",
    ["layer", "job_name"],
)
job_duration_seconds = Gauge(
    "datalake_job_duration_seconds",
    "Durée du dernier batch en secondes",
    ["layer", "job_name"],
)
job_success_ratio = Gauge(
    "datalake_job_success_ratio",
    "Ratio de succès des N derniers batches (0 à 1)",
    ["layer", "job_name"],
)
job_last_status = Gauge(
    "datalake_job_last_status",
    "Statut du dernier batch (1=success, 0=failed, 0.5=running)",
    ["layer", "job_name"],
)
quality_null_count = Gauge(
    "datalake_quality_null_count",
    "Nombre de valeurs nulles détectées (dernier contrôle qualité)",
    ["dataset_name"],
)
quality_duplicate_count = Gauge(
    "datalake_quality_duplicate_count",
    "Nombre de doublons détectés (dernier contrôle qualité)",
    ["dataset_name"],
)
quality_invalid_count = Gauge(
    "datalake_quality_invalid_count",
    "Nombre de records invalides détectés (dernier contrôle qualité)",
    ["dataset_name"],
)


# ============================================
# Requêtes de collecte
# ============================================
def fetch_latest_batch_per_job(conn):
    """Un batch le plus récent par (layer, job_name)."""
    query = """
        SELECT DISTINCT ON (layer, job_name)
            layer, job_name, status, records_in, records_out,
            EXTRACT(EPOCH FROM (COALESCE(finished_at, NOW()) - started_at)) AS duration_seconds
        FROM batch_lineage
        ORDER BY layer, job_name, started_at DESC
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        return cur.fetchall()


def fetch_success_ratio_per_job(conn, window=20):
    """Ratio de succès sur les `window` derniers batches par (layer, job_name)."""
    query = """
        SELECT layer, job_name,
               AVG(CASE WHEN status = 'success' THEN 1.0 ELSE 0.0 END) AS ratio
        FROM (
            SELECT layer, job_name, status,
                   ROW_NUMBER() OVER (PARTITION BY layer, job_name ORDER BY started_at DESC) AS rn
            FROM batch_lineage
            WHERE status IN ('success', 'failed')
        ) t
        WHERE rn <= %s
        GROUP BY layer, job_name
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, (window,))
        return cur.fetchall()


def fetch_latest_quality_metrics(conn):
    """Dernier contrôle qualité par dataset."""
    query = """
        SELECT DISTINCT ON (dataset_name)
            dataset_name, null_count, invalid_count, duplicate_count
        FROM quality_metrics
        ORDER BY dataset_name, checked_at DESC
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        return cur.fetchall()


# ============================================
# Boucle de collecte
# ============================================
def collect_once(conn):
    status_value = {"success": 1.0, "failed": 0.0, "running": 0.5}

    for row in fetch_latest_batch_per_job(conn):
        labels = {"layer": row["layer"], "job_name": row["job_name"]}
        records_processed.labels(**labels).set(row["records_out"] or 0)
        job_duration_seconds.labels(**labels).set(row["duration_seconds"] or 0)
        job_last_status.labels(**labels).set(status_value.get(row["status"], 0))

    for row in fetch_success_ratio_per_job(conn):
        job_success_ratio.labels(layer=row["layer"], job_name=row["job_name"]).set(row["ratio"] or 0)

    for row in fetch_latest_quality_metrics(conn):
        quality_null_count.labels(dataset_name=row["dataset_name"]).set(row["null_count"] or 0)
        quality_duplicate_count.labels(dataset_name=row["dataset_name"]).set(row["duplicate_count"] or 0)
        quality_invalid_count.labels(dataset_name=row["dataset_name"]).set(row["invalid_count"] or 0)


def main():
    cfg = get_pg_config()
    start_http_server(METRICS_PORT)
    print(f"[metrics] Exporteur Prometheus démarré sur le port {METRICS_PORT}")
    print(f"[metrics] Poll toutes les {POLL_INTERVAL_SECONDS}s depuis PostgreSQL")

    while True:
        try:
            conn = psycopg2.connect(**cfg)
            collect_once(conn)
            conn.close()
        except Exception as e:
            print(f"[metrics] Erreur de collecte: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()