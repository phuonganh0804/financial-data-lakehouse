import json
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
import requests
from awsglue.utils import getResolvedOptions

# Glue Python Shell job — pure ingestion, no Spark.
# Stores the full Twelve Data response BYTE-FOR-BYTE in the immutable landing
# zone: one file per symbol containing the complete {meta, values, status}
# payload exactly as returned. Nothing is projected or cast here — the bronze
# job explodes `values` and pulls `meta`. Request context (provider, market,
# exchange, interval, symbol, ingest_date, run_id) is encoded in the path.

args = getResolvedOptions(sys.argv, [
    "landing_bucket",
    "ingest_date",
    "api_start_date",
    "api_end_date",
    "interval",
    "ticker_config_path",
])

LANDING_BUCKET = args["landing_bucket"]
INGEST_DATE = args["ingest_date"]
API_START_DATE = args["api_start_date"]
API_END_DATE = args["api_end_date"]
TICKER_CONFIG_PATH = args["ticker_config_path"]

SSM_PARAMETER = "/financial-data-lakehouse/twelvedata-api-key"
TWELVEDATA_URL = "https://api.twelvedata.com/time_series"

# Canonical interval tokens are binance-native (1d/1w/1M), so the binance job
# passes them raw; Twelve Data spells them differently, so translate here.
# Binance is case-sensitive: "1M" is month, "1m" is minute — keep the case.
INTERVAL_MAP = {"1d": "1day", "1w": "1week", "1M": "1month"}
INTERVAL = INTERVAL_MAP.get(args["interval"], args["interval"])

# Fail loud here if the mapped interval isn't one Twelve Data accepts. Otherwise
# an unsupported token reaches the API and comes back as one HTTP 400 per symbol
# — i.e. a confusing "100% failure rate" abort instead of a clear config error.
TWELVEDATA_INTERVALS = {
    "1min", "5min", "15min", "30min", "45min",
    "1h", "2h", "4h", "8h", "1day", "1week", "1month",
}
if INTERVAL not in TWELVEDATA_INTERVALS:
    raise ValueError(
        f"interval '{args['interval']}' -> '{INTERVAL}' is not supported by "
        f"Twelve Data. Supported (after mapping): {sorted(TWELVEDATA_INTERVALS)}."
    )

MAX_RETRIES = 3
REQUEST_DELAY = 8
RETRY_DELAY = 20
MAX_OUTPUT_SIZE = 5000
FAILURE_THRESHOLD = 0

RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
RETRYABLE_API_MESSAGES = ("rate limit", "too many requests")

# Unique per run — landing keys include it, so payloads are append-only.
RUN_ID = (
    f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_"
    f"{uuid.uuid4().hex[:8]}"
)

s3 = boto3.client("s3")


def put_raw(bucket: str, key: str, raw_text: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=raw_text.encode("utf-8"),
        ContentType="application/json",
    )


def put_jsonl(bucket: str, key: str, records: list) -> None:
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
    )


def parse_s3_uri(s3_uri: str) -> tuple:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {s3_uri}")

    path = s3_uri.replace("s3://", "", 1)
    bucket, _, key = path.partition("/")

    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    return bucket, key


def load_ticker_config(s3_uri: str) -> dict:
    bucket, key = parse_s3_uri(s3_uri)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    config = json.loads(body)

    # market/exchange are twelvedata-specific (it's a multi-exchange aggregator;
    # binance has neither). symbols is validated separately below.
    for field in ("market", "exchange"):
        if not config.get(field):
            raise ValueError(f"Ticker config missing required field: {field}")

    # Same shape as landing_binance's symbol validation (kept in sync by hand —
    # standalone Glue scripts can't share a helper without packaging py-files).
    symbols = config.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("Ticker config field 'symbols' must be a non-empty list")
    if not all(isinstance(symbol, str) for symbol in symbols):
        raise ValueError("All symbols must be strings")
    if len(symbols) != len(set(symbols)):
        raise ValueError("Ticker config contains duplicate symbols")
    return config


def get_api_key() -> str:
    ssm = boto3.client("ssm", region_name="eu-central-1")
    return ssm.get_parameter(
        Name=SSM_PARAMETER,
        WithDecryption=True,
    )["Parameter"]["Value"]


def should_retry_api_error(message: str) -> bool:
    text = message.lower()
    return any(token in text for token in RETRYABLE_API_MESSAGES)


def sleep_before_retry(symbol: str, attempt: int, error: Exception) -> None:
    sleep_seconds = RETRY_DELAY * attempt
    print(
        f"{symbol}: attempt {attempt}/{MAX_RETRIES} failed: {error}. "
        f"Retrying in {sleep_seconds}s..."
    )
    time.sleep(sleep_seconds)


def fetch_symbol(
    session: requests.Session,
    symbol: str,
    api_key: str,
    exchange: str,
) -> requests.Response:
    """Return the raw HTTP response; validation reads it but never alters it."""
    params = {
        "symbol": symbol,
        "exchange": exchange,
        "interval": INTERVAL,
        "adjust": "none",
        "start_date": API_START_DATE,
        "end_date": API_END_DATE,
        "outputsize": MAX_OUTPUT_SIZE,
        "apikey": api_key,
    }

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(TWELVEDATA_URL, params=params, timeout=30)

            if response.status_code in RETRYABLE_HTTP_STATUS:
                raise RuntimeError(
                    f"temporary HTTP {response.status_code}: "
                    f"{response.text[:200]}"
                )

            if response.status_code >= 400:
                raise ValueError(
                    f"{symbol}: non-retryable HTTP {response.status_code}: "
                    f"{response.text[:200]}"
                )

            data = response.json()

            if data.get("status") == "error":
                message = data.get("message", "unknown error")

                if should_retry_api_error(message):
                    raise RuntimeError(f"retryable API error: {message}")

                raise ValueError(f"{symbol}: API error - {message}")

            if not data.get("values"):
                raise ValueError(
                    f"{symbol}: empty response for "
                    f"{API_START_DATE} -> {API_END_DATE}"
                )

            print(f"{symbol}: {len(data['values'])} rows fetched")
            return response

        except (RuntimeError, requests.exceptions.RequestException) as e:
            last_error = e

            if attempt == MAX_RETRIES:
                break

            sleep_before_retry(symbol, attempt, e)

    raise RuntimeError(
        f"{symbol}: failed after {MAX_RETRIES} attempts - {last_error}"
    )


def land_symbol(response: requests.Response, config: dict, symbol: str) -> None:
    key = (
        f"equity_prices/"
        f"provider=twelvedata/"
        f"market={config['market']}/"
        f"exchange={config['exchange']}/"
        f"interval={INTERVAL}/"
        f"symbol={symbol}/"
        f"ingest_date={INGEST_DATE}/"
        f"run_id={RUN_ID}/"
        f"response.json"
    )
    put_raw(LANDING_BUCKET, key, response.text)
    print(f"{symbol}: raw response landed -> s3://{LANDING_BUCKET}/{key}")


def land_all(config: dict, api_key: str) -> list:
    failed = []

    symbols = config["symbols"]
    exchange = config["exchange"]

    with requests.Session() as session:
        for index, symbol in enumerate(symbols):
            try:
                response = fetch_symbol(session, symbol, api_key, exchange)
                land_symbol(response, config, symbol)
            except Exception as e:
                failed.append((symbol, str(e)))
                print(f"{symbol}: failed - {e}")

            if index < len(symbols) - 1:
                time.sleep(REQUEST_DELAY)

    return failed


def write_failed_audit(failed: list, config: dict) -> None:
    """Append-only audit of failed Twelve Data symbols, partitioned like the
    data it shadows. The run's constant dimensions (provider/market/exchange/
    interval/ingest_date) live in the path; only the per-row fields (symbol,
    reason) and run-level context stay in the record."""
    if not failed:
        print("No Twelve Data failures detected. Skipping audit write.")
        return

    audit_records = [
        {
            "symbol": symbol,
            "reason": reason,
            "universe": config.get("universe", "unknown"),
            "api_start_date": API_START_DATE,
            "api_end_date": API_END_DATE,
            "audited_at": datetime.now(timezone.utc).isoformat(),
        }
        for symbol, reason in failed
    ]

    key = (
        f"audit/equity_prices/"
        f"provider=twelvedata/"
        f"market={config['market']}/"
        f"exchange={config['exchange']}/"
        f"interval={INTERVAL}/"
        f"ingest_date={INGEST_DATE}/"
        f"audit_type=failed_symbols/"
        f"run_id={RUN_ID}/"
        f"audit.jsonl"
    )
    put_jsonl(LANDING_BUCKET, key, audit_records)
    print(f"Audit written: s3://{LANDING_BUCKET}/{key}")


def main() -> None:
    print("Starting Twelve Data equities landing")
    print(f"Interval:    {INTERVAL}")
    print(f"Date range:  {API_START_DATE} -> {API_END_DATE} (exclusive)")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Run id:      {RUN_ID}")
    print(f"Config path: {TICKER_CONFIG_PATH}")

    config = load_ticker_config(TICKER_CONFIG_PATH)
    symbols = config["symbols"]

    print(f"Market:      {config['market']}")
    print(f"Exchange:    {config['exchange']}")
    print(f"Universe:    {config.get('universe', 'unknown')}")
    print(f"Symbols:     {len(symbols)}")

    api_key = get_api_key()
    print("API key retrieved from SSM")

    failed = land_all(config, api_key)

    write_failed_audit(failed, config)

    if failed:
        print(f"Failed symbols ({len(failed)}):")
        for symbol, reason in failed:
            print(f"{symbol}: {reason}")

    failure_rate = len(failed) / len(symbols)

    if failure_rate > FAILURE_THRESHOLD:
        raise RuntimeError(
            f"Failure rate {failure_rate:.0%} exceeds "
            f"threshold {FAILURE_THRESHOLD:.0%} - "
            f"{len(failed)}/{len(symbols)} symbols failed. "
            f"Aborting to prevent partial data in landing."
        )

    print(f"Landed {len(symbols) - len(failed)}/{len(symbols)} symbols")
    print("Twelve Data equities landing complete")


main()
