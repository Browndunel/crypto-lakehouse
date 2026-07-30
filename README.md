# CryptoLakehouse

Plateforme Data Lake & Warehouse pour l'analyse du marché crypto.
Architecture Medallion (Bronze → Silver → Gold) avec monitoring temps réel.

## Groupe

Teboh / Emmanuel Lamah / Tchinda Douanla

## Architecture

### Choix de stockage — justification

- **Bronze et Silver** : fichiers locaux au format Parquet (stockage colonnaire,
  compression native, compatible Spark). HDFS non utilisé car l'environnement
  Codespace impose des contraintes d'espace disque. Le stockage local Parquet
  offre les mêmes garanties de lecture distribuée via Spark.
- **Gold** : PostgreSQL 15 — base relationnelle pour les KPIs métier,
  requêtable directement par Grafana et exposable via API.

### Couches

Bronze → données brutes intactes (CSV Binance, JSON CoinGecko, JSON RSS)
Silver → nettoyage, déduplication, validation schéma, sentiment NLP (Parquet)
Gold → KPIs métier, prédictions, corrélations (PostgreSQL)


## Sources de données

| Source | Type | Format | Couche |
|---|---|---|---|
| Binance Public Data | Structuré | CSV OHLCV | Bronze batch |
| CoinGecko API | Structuré | JSON | Bronze temps réel |
| RSS CoinDesk / CoinTelegraph / Decrypt | Non-structuré | JSON texte | Bronze batch |

## Stack technique

| Service | Image | Rôle |
|---|---|---|
| spark-master | apache/spark:3.5.1 | Coordinateur Spark |
| spark-worker | apache/spark:3.5.1 | Exécuteur Spark |
| postgres | postgres:15 | Stockage Gold |
| prometheus | prom/prometheus:v2.53.0 | Collecte métriques |
| grafana | grafana/grafana:11.1.0 | Dashboard |
| node-exporter | prom/node-exporter:v1.8.1 | Métriques système |
| metrics | python:3.11-slim | Métriques pipeline custom |

## Lancement

```bash
# Démarrer la stack complète
make up

# Pipeline complet (ingest + transform + gold)
make all

# Étapes individuelles
make ingest     # Bronze
make transform  # Silver
make gold       # Gold

# Arrêter
make down
```

## Accès aux services

| Service | URL |
|---|---|
| Spark UI | http://localhost:8080 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Métriques custom | http://localhost:8000/metrics |

## KPIs calculés (couche Gold)

- **DailyMarketKPI** : close, SMA 20, RSI 14, volatilité 7j, volume moyen 7j
- **SentimentDailyKPI** : score sentiment moyen, nb articles, mentions BTC/ETH
- **PricePredictionResult** : prédiction du prix J+1 (modèle SMA momentum)
- **CorrelationInsight** : croisement prix/sentiment avec lignage Bronze→Gold

## Qualité des données (Silver)

- Validation de schéma : types explicites sur tous les champs OHLCV
- Déduplication : par `open_time` pour OHLCV, par `title` pour les news
- Métriques : doublons supprimés, nulls comptés, sentiment moyen loggé
- Enrichissement : `price_change_pct`, `dedup_key`, `is_valid`, `mention_btc/eth`