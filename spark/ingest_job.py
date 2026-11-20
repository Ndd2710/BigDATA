import os
import time
from functools import reduce

from pyspark.sql import functions as F

from pipeline import cli, processed_path, spark_session, upload

ALIASES = {
    "app_id": ("appid", "app_id"),
    "created": ("unix_timestamp_created", "timestamp_created"),
    "vote": ("voted_up", "recommended"),
    "playtime": ("playtime_at_review", "author_playtime_at_review"),
    "review_count": ("num_reviews", "author_num_reviews"),
}


def normalized_csv(spark, paths):
    frames = []
    for path in paths:
        raw = (spark.read.option("header", True).option("multiLine", True)
               .option("escape", '"').option("mode", "FAILFAST").csv(path))
        names = {column.strip().lower().lstrip("\ufeff"): column for column in raw.columns}
        selected = {}
        for field, aliases in ALIASES.items():
            name = next((names[alias] for alias in aliases if alias in names), None)
            if name is None:
                raise ValueError(f"Missing column {aliases} in {path}. Found: {raw.columns}")
            selected[field] = F.trim(F.col(f"`{name.replace('`', '``')}`")).alias(field)
        for field, aliases in {"author_id": ("steamid", "author_steamid"), "review_text": ("review",)}.items():
            name = next((names[a] for a in aliases if a in names), None)
            selected[field] = (F.col(name) if name else F.lit(None).cast("string")).alias(field)
        frames.append(raw.select(*selected.values()))
    rows = reduce(lambda left, right: left.unionByName(right), frames)
    epoch = F.col("created").cast("long")
    positive = F.lower("vote")
    playtime = F.col("playtime").cast("decimal(18,4)")
    unit = os.environ["PLAYTIME_UNIT"]
    if unit not in {"minutes", "hours"}:
        raise ValueError("PLAYTIME_UNIT must be minutes or hours")
    if unit == "hours":
        playtime = (playtime * 60).cast("decimal(18,4)")
    review_count = F.col("review_count").cast("long")
    return rows.select(
        F.col("app_id").cast("long").alias("app_id"),
        F.col("author_id"), F.col("review_text"),
        epoch.alias("created_epoch"),
        F.when(epoch.between(1, 4102444800),
               F.trunc(F.to_date(F.from_unixtime(epoch)), "month")).alias("review_month"),
        F.when(positive.isin("true", "1", "1.0"), True)
        .when(positive.isin("false", "0", "0.0"), False).alias("recommended"),
        F.when(playtime >= 0, playtime).alias("playtime_minutes"),
        F.when(review_count >= 0, review_count).alias("author_review_count"),
    )


def valid_reviews(rows):
    return rows.where((F.col("app_id") > 0) & F.col("review_month").isNotNull()
                      & F.col("recommended").isNotNull())


def aggregate(rows):
    time = F.col("playtime_minutes")
    activity = F.col("author_review_count")
    return (
        rows.withColumn("playtime_band", F.when(time.isNull(), "unknown")
                        .when(time < 60, "under_1h").when(time < 600, "1_to_10h")
                        .when(time < 3000, "10_to_50h").otherwise("50h_plus"))
        .withColumn("activity_band", F.when(activity.isNull(), "unknown")
                    .when(activity < 5, "under_5").when(activity < 20, "5_to_19").otherwise("20_plus"))
        .groupBy("review_month", "app_id", "playtime_band", "activity_band")
        .agg(F.count("*").alias("reviews"),
             F.sum(F.col("recommended").cast("long")).alias("recommendations"),
             F.count("playtime_minutes").alias("known_playtime"),
             F.coalesce(F.sum("playtime_minutes"), F.lit(0)).alias("total_playtime_minutes"))
    )


def process(spark, paths, clean_path, aggregate_path):
    started = time.monotonic()
    rows = normalized_csv(spark, paths)
    invalid_app = F.col("app_id").isNull() | (F.col("app_id") <= 0)
    invalid_month = F.col("review_month").isNull()
    invalid_vote = F.col("recommended").isNull()
    invalid = invalid_app | invalid_month | invalid_vote
    def count_if(condition, name):
        return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0)).cast("long").alias(name)
    metrics = rows.agg(
        F.count("*").alias("input_reviews"),
        count_if(~invalid, "valid_reviews"),
        count_if(invalid, "rejected_reviews"),
        count_if(F.col("review_text").isNull() | (F.length(F.trim("review_text")) == 0), "empty_review_text"),
        count_if(invalid_app, "invalid_app_id"),
        count_if(invalid_month, "invalid_review_month"),
        count_if(invalid_vote, "invalid_recommendation"),
        count_if(~invalid & F.col("playtime_minutes").isNull(), "unknown_playtime"),
        count_if(~invalid & F.col("author_review_count").isNull(), "unknown_author_activity"),
    ).first().asDict()
    rejected = rows.where(invalid).withColumn("rejection_reasons", F.concat_ws(";",
        F.when(invalid_app, "invalid_app_id"), F.when(invalid_month, "invalid_review_month"),
        F.when(invalid_vote, "invalid_recommendation")))
    root = aggregate_path.rsplit("/", 1)[0]
    rejected.write.mode("overwrite").parquet(root + "/rejected")
    valid = valid_reviews(rows)
    valid.repartition("review_month").write.mode("overwrite").partitionBy("review_month").parquet(clean_path)
    clean = spark.read.parquet(clean_path)
    aggregate(clean).write.mode("overwrite").parquet(aggregate_path)
    actual = spark.read.parquet(aggregate_path).agg(F.sum("reviews")).first()[0] or 0
    if actual != metrics["valid_reviews"]:
        raise ValueError("Review count mismatch between normalized input and aggregates")
    analysis_started = time.monotonic()
    from review_analysis import analyze
    metrics.update(analyze(clean, root))
    metrics["analysis_seconds"] = round(time.monotonic() - analysis_started, 3)
    metrics["spark_threads"] = int(os.environ["SPARK_THREADS"])
    metrics["shuffle_partitions"] = int(os.environ["SPARK_SHUFFLE_PARTITIONS"])
    metrics["aggregate_reviews"] = actual
    metrics["processing_seconds"] = round(time.monotonic() - started, 3)
    spark.createDataFrame([metrics]).write.mode("overwrite").parquet(
        aggregate_path.rsplit("/", 1)[0] + "/quality")
    print(f"Data quality: {metrics}", flush=True)
    print(f"Parquet ready: {aggregate_path}", flush=True)


if __name__ == "__main__":
    args = cli()
    clean_path = processed_path(args.source, args.snapshot, "reviews")
    aggregate_path = processed_path(args.source, args.snapshot, "aggregates")
    paths = list(upload(args.source))
    spark = spark_session("Steam Reviews CSV to Parquet")
    try:
        process(spark, paths, clean_path, aggregate_path)
    finally:
        spark.stop()
