import os
import json
from io import StringIO

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from pipeline import cli, object_store, processed_path, spark_session

COLUMNS = "review_month, app_id, playtime_band, activity_band, reviews, recommendations, known_playtime, total_playtime_minutes"
TABLE_SQL = """
    review_month date NOT NULL,
    app_id bigint NOT NULL,
    playtime_band text NOT NULL,
    activity_band text NOT NULL,
    reviews bigint NOT NULL CHECK (reviews > 0),
    recommendations bigint NOT NULL CHECK (recommendations BETWEEN 0 AND reviews),
    known_playtime bigint NOT NULL CHECK (known_playtime BETWEEN 0 AND reviews),
    total_playtime_minutes numeric NOT NULL CHECK (total_playtime_minutes >= 0),
    PRIMARY KEY (review_month, app_id, playtime_band, activity_band)
"""


def load(cursor, rows, schema):
    cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    table = sql.Identifier(schema, "review_aggregates")
    cursor.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(table, sql.SQL(TABLE_SQL)))
    cursor.execute(sql.SQL("CREATE TEMP TABLE next_aggregates (LIKE {} INCLUDING ALL)").format(table))
    buffer = StringIO()
    total = 0
    for row in rows:
        buffer.write("\t".join(str(value) for value in row) + "\n")
        total += 1
        if total % 10000 == 0:
            buffer.seek(0)
            cursor.copy_from(buffer, "next_aggregates")
            buffer.seek(0)
            buffer.truncate()
    if buffer.tell():
        buffer.seek(0)
        cursor.copy_from(buffer, "next_aggregates")
    if not total:
        raise ValueError("No usable reviews found. Previous warehouse results were preserved.")
    cursor.execute(sql.SQL("TRUNCATE {}").format(table))
    cursor.execute(sql.SQL("INSERT INTO {} SELECT * FROM next_aggregates").format(table))
    print(f"Loaded {total} aggregate rows into {schema}.review_aggregates", flush=True)


def load_analysis(cursor, spark, source, snapshot):
    schema = "raw_demo" if source == "demo" else "raw"
    definitions = {
        "behavior_stats": "app_id bigint, recommended boolean, reviews bigint, known_playtime bigint, avg_playtime_minutes numeric, median_playtime_minutes numeric, p90_playtime_minutes numeric",
        "topic_mentions": "app_id bigint, topic text, recommended boolean, mentions bigint",
    }
    for name, definition in definitions.items():
        path = processed_path(source, snapshot, name)
        from minio.error import S3Error
        try:
            object_store().stat_object("steam-reviews", path.split("/", 3)[3] + "/_SUCCESS")
        except S3Error as error:
            if error.code != "NoSuchKey": raise
            raise ValueError("Snapshot has no extended analysis; rerun the ingest DAG before loading it") from error
        columns = [part.strip().split()[0] for part in definition.split(",")]
        frame = spark.read.parquet(path).select(*columns)
        table = sql.Identifier(schema, name)
        cursor.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(table, sql.SQL(definition)))
        cursor.execute(sql.SQL("TRUNCATE {}").format(table))
        insert = sql.SQL("INSERT INTO {} VALUES %s").format(table).as_string(cursor)
        batch = []
        for row in frame.toLocalIterator():
            batch.append(tuple(row))
            if len(batch) == 1000:
                execute_values(cursor, insert, batch); batch = []
        if batch: execute_values(cursor, insert, batch)


if __name__ == "__main__":
    args = cli()
    path = processed_path(args.source, args.snapshot, "aggregates")
    object_store().stat_object("steam-reviews", path.split("/", 3)[3] + "/_SUCCESS")
    spark = spark_session("Steam Reviews Parquet to PostgreSQL")
    try:
        totals = spark.read.parquet(path).select(*COLUMNS.split(", "))
        with psycopg2.connect(host=os.environ["POSTGRES_HOST"], dbname=os.environ["POSTGRES_DB"],
                              user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"]) as connection:
            with connection.cursor() as cursor:
                load(cursor, totals.toLocalIterator(), "raw_demo" if args.source == "demo" else "raw")
                load_analysis(cursor, spark, args.source, args.snapshot)
                # Historical snapshots may predate quality reporting.
                quality_path = processed_path(args.source, args.snapshot, "quality")
                from minio.error import S3Error
                try:
                    object_store().stat_object("steam-reviews", quality_path.split("/", 3)[3] + "/_SUCCESS")
                except S3Error as error:
                    if error.code != "NoSuchKey":
                        raise
                    print("Historical snapshot: no quality report available", flush=True)
                else:
                    report = spark.read.parquet(quality_path).first().asDict()
                    schema = "raw_demo" if args.source == "demo" else "raw"
                    cursor.execute(sql.SQL("SELECT SUM(reviews) FROM {}.review_aggregates").format(sql.Identifier(schema)))
                    if cursor.fetchone()[0] != report["valid_reviews"]:
                        raise ValueError("Warehouse review count does not match quality report")
                    cursor.execute("""CREATE SCHEMA IF NOT EXISTS monitoring;
                        CREATE TABLE IF NOT EXISTS monitoring.pipeline_quality (
                            source text NOT NULL, snapshot text NOT NULL,
                            loaded_at timestamptz NOT NULL DEFAULT now(), metrics jsonb NOT NULL,
                            PRIMARY KEY (source, snapshot))""")
                    cursor.execute("""INSERT INTO monitoring.pipeline_quality(source, snapshot, metrics)
                        VALUES (%s, %s, %s::jsonb) ON CONFLICT (source, snapshot)
                        DO UPDATE SET metrics = EXCLUDED.metrics, loaded_at = now()""",
                        (args.source, args.snapshot, json.dumps(report)))
    finally:
        spark.stop()
