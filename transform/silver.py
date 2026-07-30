from datetime import datetime, timezone
import os
from pathlib import Path
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
LOCAL_DATA = Path("/work/data")

OHLCV_SCHEMA = StructType(
    [
        StructField("open_time", LongType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("close_time", LongType(), True),
        StructField("quote_asset_volume", DoubleType(), True),
        StructField("number_of_trades", LongType(), True),
        StructField("taker_buy_base_volume", DoubleType(), True),
        StructField("taker_buy_quote_volume", DoubleType(), True),
        StructField("ignore", StringType(), True),
    ]
)

# Lexique de sentiment simple — pas de dépendance externe
POSITIVE_WORDS = [
    "bull", "bullish", "surge", "soar", "rally", "gain", "rise",
    "pump", "moon", "ath", "breakout", "buy", "long", "positive",
    "growth", "profit", "win", "up", "high", "strong", "record",
    "hausse", "monte", "positif", "croissance",
]
NEGATIVE_WORDS = [
    "bear", "bearish", "crash", "dump", "drop", "fall", "decline",
    "loss", "sell", "short", "negative", "down", "low", "weak",
    "fear", "panic", "risk", "warning", "baisse", "chute", "negatif",
]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver-transform")
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def compute_sentiment(text: str) -> float:
    if not text:
        return 0.0
    text_lower = text.lower()
    words = re.findall(r"\w+", text_lower)
    pos = sum(1 for w in words if w in POSITIVE_WORDS)
    neg = sum(1 for w in words if w in NEGATIVE_WORDS)
    total = pos + neg

    if total == 0:
        return 0.0
    return round((pos - neg) / total, 4)


def process_ohlcv(spark: SparkSession):
    print("=== Silver : OHLCV ===")
    bronze_path = str(LOCAL_DATA / "bronze" / "ohlcv")
    silver_path = str(LOCAL_DATA / "silver" / "ohlcv")

    df = (
        spark.read.option("header", "false")
        .schema(OHLCV_SCHEMA)
        .csv(f"{bronze_path}/**/*.csv")
    )

    rows_in = df.count()
    print(f"Lignes lues : {rows_in}")

    # Deduplication
    df = df.dropDuplicates(["open_time"])

    # Validation schema
    df = df.filter(
        F.col("open").isNotNull()
        & F.col("close").isNotNull()
        & F.col("volume").isNotNull()
        & (F.col("open") > 0)
        & (F.col("close") > 0)
        & (F.col("volume") >= 0)
    )

    # Enrichissement
    df = (
        df.withColumn(
            "open_datetime", F.to_timestamp(F.col("open_time") / 1000)
        )
        .withColumn("date", F.to_date("open_datetime"))
        .withColumn(
            "price_change_pct",
            ((F.col("close") - F.col("open")) / F.col("open") * 100).cast(
                DoubleType()
            ),
        )
        .withColumn(
            "dedup_key",
            F.concat_ws("_", F.col("open_time"), F.col("close")),
        )
        .withColumn("is_valid", F.lit(True))
        .withColumn(
            "processed_at",
            F.lit(datetime.now(timezone.utc).isoformat()),
        )
        .drop("ignore", "close_time")
    )

    rows_out = df.count()
    print(f"Lignes produites : {rows_out}")
    print(f"Doublons supprimes : {rows_in - rows_out}")

    # Metriques qualite
    null_count = df.filter(F.col("close").isNull()).count()
    print(f"Valeurs nulles restantes (close) : {null_count}")

    df.write.mode("overwrite").parquet(silver_path)
    print(f"[OK] Silver OHLCV -> {silver_path}")
    return rows_in, rows_out


def process_news(spark: SparkSession):
    print("=== Silver : News (RSS) ===")
    bronze_path = str(LOCAL_DATA / "bronze" / "news")
    silver_path = str(LOCAL_DATA / "silver" / "news")

    try:
        df = spark.read.option("multiline", "true").json(
            f"{bronze_path}/*.json"
        )
    except Exception as e:
        print(f"[SKIP] Impossible de lire les news : {e}")
        return 0, 0

    if df.count() == 0:
        print("[SKIP] Aucune news trouvee")
        return 0, 0

    rows_in = df.count()
    print(f"Lignes lues : {rows_in}")

    # Deduplication sur le titre
    df = df.dropDuplicates(["title"])

    # Nettoyage texte
    clean_html = F.udf(
        lambda t: re.sub(r"<[^>]+>", "", t or "").strip(), StringType()
    )
    df = df.withColumn("clean_text", clean_html(F.col("title")))
    df = df.withColumn(
        "clean_description", clean_html(F.col("description"))
    )

    # Validation
    df = df.filter(
        F.col("clean_text").isNotNull()
        & (F.length(F.col("clean_text")) > 5)
    )

    # Sentiment NLP (lexique)
    sentiment_udf = F.udf(compute_sentiment, DoubleType())
    df = df.withColumn(
        "sentiment_score",
        sentiment_udf(
            F.concat_ws(" ", F.col("clean_text"), F.col("clean_description"))
        ),
    )

    # Detection mentions crypto
    df = (
        df.withColumn(
            "mention_btc",
            F.col("clean_text").rlike("(?i)bitcoin|btc").cast("boolean"),
        )
        .withColumn(
            "mention_eth",
            F.col("clean_text").rlike("(?i)ethereum|eth").cast("boolean"),
        )
        .withColumn("is_duplicate", F.lit(False))
        .withColumn(
            "processed_at",
            F.lit(datetime.now(timezone.utc).isoformat()),
        )
    )

    rows_out = df.count()
    duplicates = rows_in - rows_out
    print(f"Lignes produites : {rows_out}")
    print(f"Doublons supprimes : {duplicates}")

    avg_sentiment = df.agg(F.avg("sentiment_score")).collect()[0][0]
    print(f"Sentiment moyen : {avg_sentiment:.4f}")

    df.write.mode("overwrite").parquet(silver_path)
    print(f"[OK] Silver News -> {silver_path}")
    return rows_in, rows_out


def run():
    spark = get_spark()
    try:
        ohlcv_in, ohlcv_out = process_ohlcv(spark)
        news_in, news_out = process_news(spark)
        print("\n=== Resume Silver ===")
        print(f"OHLCV : {ohlcv_in} -> {ohlcv_out} lignes")
        print(f"News  : {news_in} -> {news_out} lignes")
        print("Transformation Silver terminee.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()