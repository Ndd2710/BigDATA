"""Integration fixture: invalid rows, duplicate keys, nulls, quantiles and topics."""
import csv
import tempfile
from pathlib import Path

from ingest_job import process
from pipeline import spark_session

spark = spark_session('Steam Reviews Upgrade Regression')
try:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        path = root / 'fixture.csv'
        fields = ['appid', 'steamid', 'voted_up', 'playtime_at_review', 'num_reviews',
                  'unix_timestamp_created', 'review']
        rows = [
            [10, 'a', True, 60, 1, 1640995200, 'crash server price'],
            [10, 'a', True, 60, 1, 1640995200, 'crash server price'],
            [10, 'b', False, 120, 2, 1640995200, 'good gameplay story'],
            [10, 'c', True, '', '', 1640995200, ''],
            [0, 'd', True, 1, 1, 1640995200, 'bad app'],
            [10, 'e', True, 1, 1, 'bad', 'bad timestamp'],
            [10, 'f', 'invalid', 1, 1, 1640995200, 'bad vote'],
        ]
        with path.open('w', newline='') as stream:
            writer = csv.writer(stream); writer.writerow(fields); writer.writerows(rows)
        process(spark, [str(path)], str(root / 'reviews'), str(root / 'aggregates'))
        metrics = spark.read.parquet(str(root / 'quality')).first().asDict()
        expected = dict(input_reviews=7, valid_reviews=4, rejected_reviews=3,
                        invalid_app_id=1, invalid_review_month=1, invalid_recommendation=1,
                        unknown_playtime=1, unknown_author_activity=1, empty_review_text=1,
                        suspected_duplicate_key_excess=1, aggregate_reviews=4)
        for key, value in expected.items():
            assert metrics[key] == value, (key, metrics[key], value)
        assert spark.read.parquet(str(root / 'rejected')).count() == 3
        positive = spark.read.parquet(str(root / 'behavior_stats')).where('recommended').first()
        assert positive['median_playtime_minutes'] == 60
        topics = {r['topic']: r['mentions'] for r in
                  spark.read.parquet(str(root / 'topic_mentions')).where('recommended').collect()}
        assert topics == dict(performance=2, connection=2, value=2), topics
        print('PASS: invalid-row quarantine, duplicate-key audit, null handling, quantiles, topic mentions', flush=True)
finally:
    spark.stop()
