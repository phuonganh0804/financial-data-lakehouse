import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
import requests
from awsglue.utils import getResolvedOptions

# Glue Python Shell job — pure ingestion, no Spark.
# Stores the Binance klines API response BYTE-FOR-BYTE in the immutable
# landing zone: one file per page, exactly as returned (a JSON array of
# arrays). Nothing is projected, renamed, or cast here — that is the bronze
# job's responsibility. Request context (symbol, interval, ingest_date,
# run_id) is encoded in the path, never injected into the payload.

args = getResolvedOptions(sys.argv, [
    'landing_bucket',
    'ingest_date',
    'api_start_date',
    'api_end_date',
    'interval',
])

LANDING_BUCKET = args['landing_bucket']
INGEST_DATE    = args['ingest_date']
API_START_DATE = args['api_start_date']
API_END_DATE   = args['api_end_date']   # exclusive
INTERVAL       = args['interval']
SYMBOLS        = ['BTCUSDT', 'ETHUSDT']
MAX_LIMIT      = 1000
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

MAX_RETRIES = 3
RETRY_DELAY = 15
RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}

# Unique per run — landing keys include it, so payloads are append-only.
RUN_ID = (
    f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_"
    f"{uuid.uuid4().hex[:8]}"
)

s3 = boto3.client("s3")


def to_epoch_ms(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def put_raw(bucket: str, key: str, raw_text: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=raw_text.encode("utf-8"),
        ContentType="application/json",
    )


def fetch_page(
    session: requests.Session,
    symbol: str,
    params: dict,
) -> requests.Response:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(BINANCE_KLINES_URL, params=params, timeout=30)

            if response.status_code in RETRYABLE_HTTP_STATUS:
                raise RuntimeError(
                    f"temporary HTTP {response.status_code}: "
                    f"{response.text[:200]}"
                )

            response.raise_for_status()
            return response

        except (RuntimeError, requests.exceptions.RequestException) as e:
            last_error = e

            if attempt == MAX_RETRIES:
                break

            sleep_seconds = RETRY_DELAY * attempt
            print(
                f"{symbol}: attempt {attempt}/{MAX_RETRIES} failed: {e}. "
                f"Retrying in {sleep_seconds}s..."
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"{symbol}: page fetch failed after {MAX_RETRIES} attempts - {last_error}"
    )


def land_symbol(session: requests.Session, symbol: str) -> None:
    """Page through the date range, writing each raw page response as-is."""
    start_ms   = to_epoch_ms(API_START_DATE)
    end_ms     = to_epoch_ms(API_END_DATE)
    current_ms = start_ms
    page_index = 0
    total_rows = 0

    prefix = (
        f"binance_klines/"
        f"symbol={symbol}/"
        f"interval={INTERVAL}/"
        f"ingest_date={INGEST_DATE}/"
        f"run_id={RUN_ID}/"
    )

    while current_ms < end_ms:
        params = {
            "symbol":    symbol,
            "interval":  INTERVAL,
            "startTime": current_ms,
            "endTime":   end_ms,
            "limit":     MAX_LIMIT,
        }

        response = fetch_page(session, symbol, params)
        batch = response.json()

        if not batch:
            break

        for row in batch:
            if len(row) != 12:
                raise ValueError(
                    f"{symbol}: expected 12 fields per kline, "
                    f"got {len(row)} — Binance API may have changed"
                )

        # Land the page verbatim before doing anything else with it.
        key = f"{prefix}page_{page_index:04d}.json"
        put_raw(LANDING_BUCKET, key, response.text)
        page_index += 1
        total_rows += len(batch)
        print(f"{symbol}: landed page {page_index} ({len(batch)} rows) -> {key}")

        current_ms = batch[-1][6] + 1

        if len(batch) < MAX_LIMIT:
            break

    if total_rows == 0:
        raise ValueError(
            f"{symbol}: empty response for "
            f"{API_START_DATE} -> {API_END_DATE} "
            f"interval={INTERVAL}"
        )

    print(f"{symbol}: {total_rows} rows landed across {page_index} page(s)")


def main() -> None:
    print("Starting Binance klines landing")
    print(f"Interval:    {INTERVAL}")
    print(f"Date range:  {API_START_DATE} -> {API_END_DATE} (exclusive)")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Run id:      {RUN_ID}")
    print(f"Symbols:     {SYMBOLS}")

    with requests.Session() as session:
        for symbol in SYMBOLS:
            print(f"\nProcessing {symbol}...")
            land_symbol(session, symbol)

    print("\nBinance klines landing complete")


main()
