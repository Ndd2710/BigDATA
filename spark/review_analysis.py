"""Descriptive statistics and English keyword mentions; no sentiment inference."""
from pyspark.sql import functions as F

TOPICS = {
    "performance": "bug bugs crash crashes crashed crashing lag laggy fps optimization performance".split(),
    "gameplay": "gameplay combat mechanics controls difficulty".split(),
    "value": "price expensive cheap worth money refund".split(),
    "connection": "server servers connection disconnect disconnected disconnects latency ping".split(),
    "story": "story plot character characters narrative".split(),
    "community": "community toxic cheater cheaters hacker hackers support".split(),
}


def analyze(clean, root):
    known = clean.where(F.col("author_id").isNotNull() & F.col("created_epoch").isNotNull())
    duplicates = (known.groupBy("app_id", "author_id", "created_epoch").count()
                  .where("count > 1").agg(F.sum(F.col("count") - 1)).first()[0] or 0)
    # Report suspected duplicate keys; preserve rows until a review identity is verified.
    behavior = clean.groupBy("app_id", "recommended").agg(
        F.count("*").alias("reviews"), F.count("playtime_minutes").alias("known_playtime"),
        F.avg("playtime_minutes").alias("avg_playtime_minutes"),
        F.expr("percentile_approx(playtime_minutes, 0.5, 10000)").alias("median_playtime_minutes"),
        F.expr("percentile_approx(playtime_minutes, 0.9, 10000)").alias("p90_playtime_minutes"))
    behavior.write.mode("overwrite").parquet(root + "/behavior_stats")
    text = F.lower(F.coalesce(F.col("review_text"), F.lit("")))
    tokenized = clean.withColumn("_tokens", F.array_distinct(F.split(text, r"(?U)\W+")))
    mentions = tokenized.withColumn("topic", F.explode(F.array(*[
        F.when(F.size(F.array_intersect(F.col("_tokens"), F.array(*[F.lit(word) for word in words]))) > 0, F.lit(topic)) for topic, words in TOPICS.items()
    ]))).where(F.col("topic").isNotNull())
    (mentions.groupBy("app_id", "topic", "recommended").agg(F.count("*").alias("mentions"))
     .write.mode("overwrite").parquet(root + "/topic_mentions"))
    return {"suspected_duplicate_key_excess": int(duplicates)}
