# crypto-lakehouse# Crypto Data Lake & Warehouse — Projet Big Data IPSSI 2026

Plateforme data lake/warehouse en architecture Medallion (Bronze → Silver → Gold)
ingérant des données de marché crypto (cours OHLCV) et des actualités
(Reddit + presse financière), avec monitoring temps réel et prédiction de prix.

## Objectif

Ingérer des données de marché crypto structurées (cours OHLCV Binance) et des
actualités non-structurées (Reddit, presse financière via RSS/API), les nettoyer
et les structurer en 3 couches, calculer des indicateurs métier (volatilité,
tendances, sentiment), prédire le prix de clôture du lendemain, et surveiller
l'ensemble du pipeline en temps réel — le tout entièrement automatisé.

## Architecture

```
Sources externes                Bronze (HDFS)         Silver (HDFS)          Gold (PostgreSQL)
─────────────────                ─────────────         ─────────────          ──────────────────
Binance (bulk CSV)      ─────►   RawOHLCVCandle   ──►   CleanCandle      ──►   DailyMarketKPI
CoinGecko API           ─────►   RawPriceSnapshot                             PricePredictionResult
Reddit API (PRAW)       ─────►   RawRedditPost    ──►   CleanRedditPost  ──►   SentimentDailyKPI
NewsAPI / RSS           ─────►   RawNewsArticle   ──►   CleanNewsArticle ──►   CorrelationInsight
```

Traitement : Apache Spark (cluster Docker, non local).
Monitoring : Prometheus + Grafana + Node Exporter + exporteur custom (métriques métier).
Orchestration : Makefile, configuration 100% via `.env` / YAML (aucun `.sh`).

Détail des entités : voir `diagramme_classe_trading_crypto.docx`.

## Répartition de l'équipe

| Personne | Périmètre |
|---|---|
| **A — Infrastructure** | `docker-compose.yml`, `.env`, `Makefile`, `conf/hadoop.env`, `prometheus/`, `grafana/provisioning/` |
| **B — Pipeline Bronze & Silver** | `ingest/binance.py`, `ingest/coingecko.py`, `ingest/rss_news.py`, `transform/silver.py` |
| **C — Gold & Coordination** | `gold/gold.py`, `monitoring/metrics.py`, `tests/`, `README.md`, `warehouse/schema.sql` |

## Démarrage rapide

```bash
# 1. Configuration (une seule fois)
cp .env.example .env
# éditer .env : POSTGRES_PASSWORD, GRAFANA_ADMIN_PASSWORD, REDDIT_CLIENT_ID/SECRET, NEWSAPI_KEY

# 2. Infrastructure
make up            # démarre tous les containers
make init-hdfs      # crée l'arborescence HDFS Bronze/Silver
make init-db         # crée le schéma Gold dans PostgreSQL

# 3. Pipeline complet
make pipeline        # ingestion -> silver -> gold, dans cet ordre

# 4. Vérification
pytest tests/test_e2e.py -v
```

## Interfaces (Codespace : onglet PORTS)

| Service | Port | Usage |
|---|---|---|
| Spark Master UI | 8080 | suivi des jobs Spark |
| HDFS Namenode UI | 9870 | exploration Bronze/Silver |
| Grafana | 3000 | dashboards monitoring |
| Prometheus | 9090 | requêtes métriques brutes |
| PostgreSQL | 5432 | requêtes SQL sur Gold (DBeaver, psql...) |
| Exporteur custom | 9200 | métriques métier (`/metrics`) |

## Traçabilité (lineage)

Chaque exécution de job (`gold.py`, jobs Silver) est enregistrée dans
`batch_lineage` avec un `batch_id` (UUID). Chaque table Gold porte une colonne
`source_batch_id` qui référence ce batch. Pour retracer l'origine d'un KPI :

```sql
SELECT k.*, b.job_name, b.started_at, b.status
FROM daily_market_kpi k
JOIN batch_lineage b ON k.source_batch_id = b.batch_id
WHERE k.symbol = 'BTC' AND k.day = '2026-07-29';
```

## Modèle de prédiction

Régression linéaire (`pyspark.ml.regression.LinearRegression`) sur les features :
`sma_20`, `volatility_7d`, `volume_avg_7d`, `avg_sentiment_score` → prédit le
prix de clôture J+1. Choix volontairement simple (défendable en 24h, résultat
interprétable) plutôt qu'un modèle complexe non maîtrisé par l'équipe.

## Tests

`tests/test_e2e.py` vérifie, sur l'infra réellement démarrée (pas de mocks) :
- accessibilité de tous les services (Postgres, HDFS, Prometheus, Grafana)
- présence des données Silver en HDFS
- tables Gold peuplées après `make pipeline`
- traçabilité : aucune ligne Gold orpheline (sans `batch_id` valide)
- présence des index requis pour les requêtes complexes
- cohérence basique des prédictions (pas de prix négatifs)

## Choix techniques justifiés

- **HDFS pour Bronze/Silver** : volumétrie 5GB+, cohérent avec l'exigence "Spark non local"
- **PostgreSQL pour Gold** : requêtes relationnelles simples (agrégations, jointures symbole/date), pas besoin de NoSQL pour ce volume de KPIs
- **Régression linéaire plutôt que LSTM/ARIMA** : le sujet évalue la plateforme, pas la sophistication du modèle
- **Sentiment calculé en Silver, pas en Gold** : enrichissement ligne par ligne (NLP), pas une agrégation — cohérent avec le rôle de chaque couche