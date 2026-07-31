.PHONY: up down ingest transform gold all clean

up:
	docker compose up -d
	@echo "Stack demarree."

down:
	docker compose down

ingest:
	@echo "Ingestion Bronze..."
	python3 ingest/binance.py
	python3 ingest/coingecko.py
	python3 ingest/rss_news.py
	@echo "Ingestion terminee."

transform:
	@echo "Transformation Silver..."
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/work/transform/silver.py
	@echo "Silver terminee."

gold:
	@echo "Calcul Gold..."
	docker exec -u root spark-master pip install psycopg2-binary -q --no-cache-dir
	docker exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--jars /work/jars/postgresql-42.7.3.jar \
		--conf spark.executorEnv.PYTHONPATH=/work \
		/work/gold/gold.py
	@echo "Gold terminee."

all: up ingest transform gold
	@echo "Pipeline complet termine."

clean:
	docker compose down -v
	rm -rf data/bronze/* data/silver/* data/gold/*