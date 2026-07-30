import os
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SPARK_MASTER    = os.getenv("SPARK_MASTER",       "spark://spark-master:7077")
POSTGRES_HOST   = os.getenv("POSTGRES_HOST",      "postgres")
POSTGRES_PORT   = os.getenv("POSTGRES_PORT",      "5432")
POSTGRES_DB     = os.getenv("POSTGRES_DB",        "crypto_gold")
POSTGRES_USER   = os.getenv("POSTGRES_USER",      "crypto")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD","crypto123")

LOCAL_DATA = Path("/work/data")
JDBC_URL   = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPS = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver":   "org.postgresql.Driver",
}


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold-transform")
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.3"
        )
        .getOrCreate()
    )


def init_postgres():
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_market_kpi (
            id              SERIAL PRIMARY KEY,
            date            DATE NOT NULL,
            open_day        DOUBLE PRECISION,
            close_day       DOUBLE PRECISION,
            daily_return_pct DOUBLE PRECISION,
            volatility_7d   DOUBLE PRECISION,
            volume_avg_7d   DOUBLE PRECISION,
            sma_20          DOUBLE PRECISION,
            rsi_14          DOUBLE PRECISION,
            processed_at    TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sentiment_daily_kpi (
            id                  SERIAL PRIMARY KEY,
            date                DATE NOT NULL,
            source_name         VARCHAR(100),
            avg_sentiment_score DOUBLE PRECISION,
            article_count       INTEGER,
            mention_count_btc   INTEGER,
            mention_count_eth   INTEGER,
            processed_at        TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS price_prediction_result (
            id                  SERIAL PRIMARY KEY,
            prediction_date     DATE NOT NULL,
            predicted_close     DOUBLE PRECISION,
            actual_close        DOUBLE PRECISION,
            model_version       VARCHAR(50),
            processed_at        TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS correlation_insight (
            id                      SERIAL PRIMARY KEY,
            date                    DATE NOT NULL,
            price_change_pct        DOUBLE PRECISION,
            sentiment_change_pct    DOUBLE PRECISION,
            news_count              INTEGER,
            lineage_ref             TEXT,
            processed_at            TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_metrics (
            id           SERIAL PRIMARY KEY,
            layer        VARCHAR(50),
            rows_in      BIGINT,
            rows_out     BIGINT,
            duration_s   DOUBLE PRECISION,
            processed_at TIMESTAMP DEFAULT NOW()
        )
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("[OK] Tables PostgreSQL initialisees")


def compute_market_kpis(spark: SparkSession):
    print("=== Gold : DailyMarketKPI ===")
    silver_path = str(LOCAL_DATA / "silver" / "ohlcv")

    df = spark.read.parquet(silver_path)
    rows_in = df.count()
    print(f"Lignes lues : {rows_in}")

    # Fenetres glissantes
    w7   = Window.orderBy("date").rowsBetween(-6, 0)
    w20  = Window.orderBy("date").rowsBetween(-19, 0)
    w14  = Window.orderBy("date").rowsBetween(-13, 0)
    wlag = Window.orderBy("date")

    daily = (
        df.groupBy("date")
        .agg(
            F.first("open").alias("open_day"),
            F.last("close").alias("close_day"),
            F.avg("volume").alias("avg_volume"),
            F.avg("price_change_pct").alias("daily_return_pct"),
        )
        .orderBy("date")
    )

    daily = (
        daily
        .withColumn("volatility_7d",  F.stddev("close_day").over(w7))
        .withColumn("volume_avg_7d",  F.avg("avg_volume").over(w7))
        .withColumn("sma_20",         F.avg("close_day").over(w20))
    )

    # RSI 14 simplifie
    daily = daily.withColumn(
        "prev_close",
        F.lag("close_day", 1).over(wlag)
    )
    daily = daily.withColumn(
        "change",
        F.col("close_day") - F.col("prev_close")
    )
    daily = daily.withColumn(
        "gain",
        F.when(F.col("change") > 0, F.col("change")).otherwise(0.0)
    )
    daily = daily.withColumn(
        "loss",
        F.when(F.col("change") < 0, -F.col("change")).otherwise(0.0)
    )
    daily = daily.withColumn("avg_gain", F.avg("gain").over(w14))
    daily = daily.withColumn("avg_loss", F.avg("loss").over(w14))
    daily = daily.withColumn(
        "rsi_14",
        F.when(
            F.col("avg_loss") == 0, F.lit(100.0)
        ).otherwise(
            100 - (100 / (1 + F.col("avg_gain") / F.col("avg_loss")))
        )
    )

    daily = daily.drop("prev_close", "change", "gain",
                       "loss", "avg_gain", "avg_loss", "avg_volume")

    rows_out = daily.count()
    print(f"Lignes KPI produites : {rows_out}")

    daily.write.jdbc(
        url=JDBC_URL, table="daily_market_kpi",
        mode="overwrite", properties=JDBC_PROPS
    )
    print("[OK] daily_market_kpi ecrit dans PostgreSQL")
    return rows_in, rows_out, daily


def compute_sentiment_kpis(spark: SparkSession):
    print("=== Gold : SentimentDailyKPI ===")
    silver_path = str(LOCAL_DATA / "silver" / "news")

    try:
        df = spark.read.parquet(silver_path)
    except Exception:
        print("[SKIP] Pas de donnees news Silver")
        return 0, 0, None

    rows_in = df.count()
    if rows_in == 0:
        print("[SKIP] Silver news vide")
        return 0, 0, None

    df = df.withColumn("date", F.to_date(F.col("fetched_at")))

    sentiment = (
        df.groupBy("date", "source")
        .agg(
            F.avg("sentiment_score").alias("avg_sentiment_score"),
            F.count("*").alias("article_count"),
            F.sum(
                F.col("mention_btc").cast("int")
            ).alias("mention_count_btc"),
            F.sum(
                F.col("mention_eth").cast("int")
            ).alias("mention_count_eth"),
        )
        .withColumnRenamed("source", "source_name")
        .orderBy("date")
    )

    rows_out = sentiment.count()
    print(f"Lignes sentiment produites : {rows_out}")

    sentiment.write.jdbc(
        url=JDBC_URL, table="sentiment_daily_kpi",
        mode="overwrite", properties=JDBC_PROPS
    )
    print("[OK] sentiment_daily_kpi ecrit dans PostgreSQL")
    return rows_in, rows_out, sentiment


def compute_correlation(market_df, sentiment_df):
    print("=== Gold : CorrelationInsight ===")

    if sentiment_df is None:
        print("[SKIP] Pas de donnees sentiment")
        return

    wlag = Window.orderBy("date")
    market = market_df.select(
        "date", "daily_return_pct", "close_day"
    )

    sentiment_agg = (
        sentiment_df
        .groupBy("date")
        .agg(F.avg("avg_sentiment_score").alias("avg_sentiment"))
    )

    joined = market.join(sentiment_agg, on="date", how="left")

    joined = joined.withColumn(
        "prev_sentiment",
        F.lag("avg_sentiment", 1).over(wlag)
    )
    joined = joined.withColumn(
        "sentiment_change_pct",
        F.when(
            F.col("prev_sentiment").isNotNull() &
            (F.col("prev_sentiment") != 0),
            ((F.col("avg_sentiment") - F.col("prev_sentiment"))
             / F.abs(F.col("prev_sentiment")) * 100)
        ).otherwise(0.0)
    )

    joined = joined.withColumn(
        "lineage_ref",
        F.concat_ws("|",
            F.lit("bronze:ohlcv"),
            F.lit("bronze:news"),
            F.col("date").cast("string")
        )
    )

    corr = joined.select(
        "date",
        F.col("daily_return_pct").alias("price_change_pct"),
        "sentiment_change_pct",
        F.lit(0).alias("news_count"),
        "lineage_ref",
    )

    corr.write.jdbc(
        url=JDBC_URL, table="correlation_insight",
        mode="overwrite", properties=JDBC_PROPS
    )
    print("[OK] correlation_insight ecrit dans PostgreSQL")


def compute_prediction(market_df):
    print("=== Gold : PricePredictionResult ===")

    wlag = Window.orderBy("date")

    pred = (
        market_df
        .withColumn("sma_ratio",
            F.col("close_day") / F.col("sma_20")
        )
        .withColumn("prev_close",
            F.lag("close_day", 1).over(wlag)
        )
        .withColumn(
            "predicted_close",
            F.col("close_day") * (1 + F.col("daily_return_pct") / 100 * 0.5)
        )
        .withColumn(
            "prediction_date",
            F.date_add(F.col("date"), 1)
        )
        .withColumn("actual_close", F.lit(None).cast("double"))
        .withColumn("model_version", F.lit("sma_momentum_v1"))
        .select(
            "prediction_date",
            "predicted_close",
            "actual_close",
            "model_version",
        )
        .filter(F.col("predicted_close").isNotNull())
    )

    pred.write.jdbc(
        url=JDBC_URL, table="price_prediction_result",
        mode="overwrite", properties=JDBC_PROPS
    )
    print("[OK] price_prediction_result ecrit dans PostgreSQL")


def write_pipeline_metrics(layer: str, rows_in: int,
                           rows_out: int, duration_s: float):
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=int(POSTGRES_PORT),
        dbname=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO pipeline_metrics "
        "(layer, rows_in, rows_out, duration_s) VALUES (%s, %s, %s, %s)",
        (layer, rows_in, rows_out, duration_s),
    )
    conn.commit()
    cur.close()
    conn.close()


def run():
    init_postgres()
    spark = get_spark()
    start = time.time()

    try:
        t0 = time.time()
        mkt_in, mkt_out, market_df = compute_market_kpis(spark)
        write_pipeline_metrics("gold_market", mkt_in, mkt_out,
                               time.time() - t0)

        t1 = time.time()
        sent_in, sent_out, sentiment_df = compute_sentiment_kpis(spark)
        write_pipeline_metrics("gold_sentiment", sent_in, sent_out,
                               time.time() - t1)

        compute_correlation(market_df, sentiment_df)
        compute_prediction(market_df)

        print("\n=== Resume Gold ===")
        print(f"DailyMarketKPI      : {mkt_in} -> {mkt_out} lignes")
        print(f"SentimentDailyKPI   : {sent_in} -> {sent_out} lignes")
        print(f"Duree totale        : {time.time() - start:.1f}s")
        print("Couche Gold terminee.")

    finally:
        spark.stop()


if __name__ == "__main__":
    run()