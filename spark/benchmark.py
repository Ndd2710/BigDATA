"""Read-only CSV/Parquet benchmark; both inputs must describe the same snapshot."""
import argparse
import json
import os
import time
from pathlib import Path

from pyspark.sql import functions as F
from ingest_job import normalized_csv, valid_reviews
from pipeline import ROOT, processed_path, spark_session

parser = argparse.ArgumentParser()
parser.add_argument('--source', choices=['demo', 'raw'], required=True)
parser.add_argument('--snapshot', required=True)
parser.add_argument('--trials', type=int, default=3)
args = parser.parse_args()
if not 1 <= args.trials <= 10:
    parser.error('trials must be between 1 and 10')
spark = spark_session('Steam Reviews Benchmark')
results = []
try:
    csv_paths = [str(p) for p in sorted((ROOT / 'data' / args.source).glob('*.csv'))]
    for trial in range(args.trials):
        # Alternate order to reduce systematic first-reader advantage.
        formats = ['csv', 'parquet'] if trial % 2 == 0 else ['parquet', 'csv']
        expected = None
        for fmt in formats:
            spark.catalog.clearCache()
            start = time.monotonic()
            rows = (valid_reviews(normalized_csv(spark, csv_paths)) if fmt == 'csv' else
                    spark.read.parquet(processed_path(args.source, args.snapshot, 'reviews')))
            counts = rows.agg(F.count('*'), F.sum(F.col('recommended').cast('long'))).first()
            elapsed = time.monotonic() - start
            signature = list(counts)
            if expected is not None and signature != expected:
                raise ValueError('CSV differs from snapshot: benchmark is not comparable')
            expected = signature
            results.append(dict(trial=trial + 1, format=fmt, seconds=elapsed,
                                reviews=counts[0], recommendations=counts[1],
                                rows_per_second=counts[0] / elapsed))
    output = dict(source=args.source, snapshot=args.snapshot,
                  spark_version=spark.version, threads=os.environ['SPARK_THREADS'],
                  shuffle_partitions=os.environ['SPARK_SHUFFLE_PARTITIONS'],
                  scope='Read, normalize/filter CSV or read clean Parquet; count and sum recommendation. OS cache not cleared.',
                  results=results)
    dest = ROOT / 'scratch' / f'benchmark-{args.source}-{args.snapshot}-{time.time_ns()}.json'
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(output, indent=2), encoding='utf-8')
    print(json.dumps(output, indent=2), flush=True)
    print(f'Saved {dest}', flush=True)
finally:
    spark.stop()
