import sys
import requests
import pandas as pd
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# ── Glue job setup ────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bronze_bucket',
    'ingest_date'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Config ────────────────────────────────────────
BRONZE_BUCKET = args['bronze_bucket']
INGEST_DATE   = args['ingest_date']
SYMBOLS       = ['BTCUSDT', 'ETHUSDT']
INTERVAL      = '1d'
LIMIT         = 365

# ── Fetch from Binance ────────────────────────────
def fetch_binance_ohlcv(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol":   symbol,
        "interval": interval,
        "limit":    limit
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close',
        'volume', 'close_time', 'quote_volume',
        'trade_count', 'taker_buy_base_volume',
        'taker_buy_quote_volume', 'ignore'
    ])

    # Convert types
    df['open_time']  = pd.to_datetime(df['open_time'],  unit='ms', utc=True)
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms', utc=True)

    numeric_cols = [
        'open', 'high', 'low', 'close',
        'volume', 'quote_volume',
        'taker_buy_base_volume',
        'taker_buy_quote_volume'
    ]
    df[numeric_cols]  = df[numeric_cols].astype(float)
    df['trade_count'] = df['trade_count'].astype(int)
    df = df.drop(columns=['ignore'])

    # Add metadata
    df['symbol']       = symbol
    df['asset_class']  = 'crypto'
    df['currency']     = 'USD'
    df['source']       = 'binance'
    df['ingested_at']  = datetime.now(timezone.utc).isoformat()

    return df

# ── Quality checks ────────────────────────────────
def validate(df: pd.DataFrame, symbol: str) -> None:
    assert df.isnull().sum().sum() == 0,          f"{symbol}: nulls found"
    assert len(df[df['high'] < df['low']]) == 0,  f"{symbol}: high < low"
    assert len(df[df['close'] <= 0]) == 0,        f"{symbol}: close <= 0"
    assert len(df[df['volume'] <= 0]) == 0,        f"{symbol}: volume <= 0"
    print(f"✅ {symbol}: quality checks passed")

# ── Write to S3 bronze ────────────────────────────
def write_to_bronze(df: pd.DataFrame, symbol: str) -> None:
    spark_df = spark.createDataFrame(df.astype(str))

    date     = INGEST_DATE
    s3_path  = (
        f"s3://{BRONZE_BUCKET}/crypto/{symbol}/"
        f"year={date[:4]}/month={date[5:7]}/day={date[8:10]}/"
    )

    spark_df.write \
        .mode("overwrite") \
        .parquet(s3_path)

    print(f"✅ {symbol}: written to {s3_path}")

# ── Main ──────────────────────────────────────────
def main() -> None:
    print(f"Starting Binance extract job")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Symbols: {SYMBOLS}")

    for symbol in SYMBOLS:
        print(f"\nProcessing {symbol}...")

        # Fetch
        df = fetch_binance_ohlcv(symbol, INTERVAL, LIMIT)
        print(f"  Fetched {len(df)} rows")

        # Validate
        validate(df, symbol)

        # Write
        write_to_bronze(df, symbol)

    print("\n✅ Binance extract job complete")

main()
job.commit()