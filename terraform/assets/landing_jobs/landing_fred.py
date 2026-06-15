import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3
import requests
from awsglue.utils import getResolvedOptions

# Glue Python Shell job — pure ingestion, no Spark.
# Stores the full FRED observations response BYTE-FOR-BYTE in the immutable
# landing zone: one file per series, exactly as returned (including "."
# missing markers and the response envelope). Nothing is projected or cast
# here — the bronze job explodes `observations` and drops "." rows.
#
# Each series is fetched over a frequency-sized LOOKBACK window (see
# WINDOW_DAYS), not a single day, so monthly/quarterly series — whose latest
# value is rarely dated exactly today — always come back non-empty. Silver
# MERGEs on (series_id, date), so the overlap across runs dedupes harmlessly.
#
# series name/frequency/unit are reference metadata, NOT part of FRED's raw
# payload, so they are never written into the landing response files. They are
# used only to (a) drive which series to fetch and (b) make the failure audit
# human-readable. The bronze job keeps its own copy for structuring.

args = getResolvedOptions(sys.argv, [
    'landing_bucket',
    'ingest_date',
    'api_start_date',
    'api_end_date',
    'macro_series_config_path',
])

LANDING_BUCKET           = args['landing_bucket']
INGEST_DATE              = args['ingest_date']
API_START_DATE           = args['api_start_date']
API_END_DATE             = args['api_end_date']
MACRO_SERIES_CONFIG_PATH = args['macro_series_config_path']
SSM_PARAMETER            = '/financial-data-lakehouse/fred-api-key'
MAX_FAILED_SERIES        = 0

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
MAX_RETRIES = 3
RETRY_DELAY = 20
RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}

# Unique per run — landing keys include it, so payloads are append-only.
RUN_ID = (
    f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_"
    f"{uuid.uuid4().hex[:8]}"
)

s3 = boto3.client("s3")


def parse_s3_uri(s3_uri: str) -> tuple:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {s3_uri}")
    path = s3_uri.replace("s3://", "", 1)
    bucket, _, key = path.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return bucket, key


def load_macro_series(s3_uri: str) -> dict:
    """Load the FRED macro series reference config from S3 — the single source
    shared with the bronze job. Drives the fetch loop and enriches the failure
    audit; never injected into the raw landing payloads."""
    bucket, key = parse_s3_uri(s3_uri)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    series = json.loads(body).get("series", [])
    if not series:
        raise ValueError(f"Macro series config is empty: {s3_uri}")
    return {
        item["series_id"]: {
            "name": item["name"],
            "frequency": item["frequency"],
            "unit": item["unit"],
        }
        for item in series
    }


# Single source of truth, shared with the bronze job (see macro_series.json).
MACRO_SERIES = load_macro_series(MACRO_SERIES_CONFIG_PATH)


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


def get_api_key(parameter_name: str) -> str:
    ssm = boto3.client('ssm', region_name='eu-central-1')
    return ssm.get_parameter(
        Name=parameter_name,
        WithDecryption=True
    )['Parameter']['Value']


# FRED filters by observation (period) date, and dates each value at the START
# of its period while releasing it much later (Q1 GDP is dated Jan 1 but
# published late April). So a single-day window misses every series whose latest
# value isn't dated exactly today — i.e. ~always for monthly/quarterly. Instead
# we request a lookback sized to the series' cadence PLUS its release lag, so the
# most recent observation is always inside the window. Silver MERGEs on
# (series_id, date), so the overlap between consecutive runs dedupes to one row
# — re-fetching is free correctness, and it also picks up FRED revisions.
WINDOW_DAYS = {
    "daily":     10,    # long weekends/holidays + next-business-day publish lag
    "weekly":    30,
    "monthly":   90,    # ~2 months back covers the period + release lag
    "quarterly": 270,   # ~2-3 quarters back: GDP is dated 3-6 months before release
}
DEFAULT_WINDOW_DAYS = 90


def observation_start(frequency: str) -> str:
    """Lookback start for a series, sized to its frequency. Anchored to the
    run's API_END_DATE so it moves with the schedule (and with backfills)."""
    days = WINDOW_DAYS.get(frequency, DEFAULT_WINDOW_DAYS)
    end = datetime.fromisoformat(API_END_DATE)
    return (end - timedelta(days=days)).date().isoformat()


def fetch_series(
    session: requests.Session,
    series_id: str,
    api_key: str,
    frequency: str,
) -> requests.Response:
    """Return the raw HTTP response; validation reads it but never alters it."""
    obs_start = observation_start(frequency)
    params = {
        "series_id":         series_id,
        "observation_start": obs_start,
        "observation_end":   API_END_DATE,
        "api_key":           api_key,
        "file_type":         "json",
    }

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(FRED_URL, params=params, timeout=30)

            if response.status_code in RETRYABLE_HTTP_STATUS:
                raise RuntimeError(
                    f"temporary HTTP {response.status_code}: "
                    f"{response.text[:200]}"
                )

            response.raise_for_status()

            # No observations across the whole lookback window = a real miss
            # (series discontinued, or FRED down); "." rows still count as a
            # non-empty response and are kept verbatim.
            if not response.json().get("observations"):
                raise ValueError(
                    f"empty series for {obs_start} -> {API_END_DATE}"
                )

            return response

        except (RuntimeError, requests.exceptions.RequestException) as e:
            last_error = e

            if attempt == MAX_RETRIES:
                break

            sleep_seconds = RETRY_DELAY * attempt
            print(
                f"{series_id}: attempt {attempt}/{MAX_RETRIES} failed: {e}. "
                f"Retrying in {sleep_seconds}s..."
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"{series_id}: failed after {MAX_RETRIES} attempts - {last_error}"
    )


def land_series(response: requests.Response, series_id: str) -> None:
    key = (
        f"fred_macro/"
        f"series_id={series_id}/"
        f"ingest_date={INGEST_DATE}/"
        f"run_id={RUN_ID}/"
        f"response.json"
    )
    put_raw(LANDING_BUCKET, key, response.text)
    print(f"{series_id}: raw response landed -> s3://{LANDING_BUCKET}/{key}")


def land_all(api_key: str) -> list:
    failed = []

    with requests.Session() as session:
        for series_id, meta in MACRO_SERIES.items():
            try:
                response = fetch_series(session, series_id, api_key, meta["frequency"])
                land_series(response, series_id)
            except Exception as e:
                failed.append((series_id, meta, str(e)))
                print(f"{series_id}: failed — {e}")

    return failed


def write_failed_audit(failed: list) -> None:
    if not failed:
        print("No FRED failures detected. Skipping audit write.")
        return

    # series_id varies per row (an audit file can hold several failed series),
    # so it stays in the record; ingest_date / run_id live in the path.
    audit_records = [
        {
            "series_id":      series_id,
            "series_name":    meta["name"],
            "frequency":      meta["frequency"],
            "unit":           meta["unit"],
            "reason":         reason,
            "api_start_date": API_START_DATE,
            "api_end_date":   API_END_DATE,
            "audited_at":     datetime.now(timezone.utc).isoformat(),
        }
        for series_id, meta, reason in failed
    ]

    key = (
        f"audit/fred_macro/"
        f"ingest_date={INGEST_DATE}/"
        f"audit_type=failed_series/"
        f"run_id={RUN_ID}/"
        f"audit.jsonl"
    )
    put_jsonl(LANDING_BUCKET, key, audit_records)
    print(f"Audit written: s3://{LANDING_BUCKET}/{key}")


def main() -> None:
    print("Starting FRED macro landing")
    print(f"Date range:  {API_START_DATE} -> {API_END_DATE}")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Run id:      {RUN_ID}")
    print(f"Series:      {len(MACRO_SERIES)}")
    print(f"Max failed series allowed: {MAX_FAILED_SERIES}")

    api_key = get_api_key(SSM_PARAMETER)
    print("API key retrieved from SSM")

    failed = land_all(api_key)

    write_failed_audit(failed)

    if failed:
        print(f"\nFailed series ({len(failed)}):")
        for series_id, meta, reason in failed:
            print(f"  {series_id} ({meta['frequency']}): {reason}")

    if len(failed) > MAX_FAILED_SERIES:
        raise RuntimeError(
            f"{len(failed)}/{len(MACRO_SERIES)} FRED series failed — "
            f"aborting to prevent incomplete macro data. "
            f"Failed: {[s for s, _, _ in failed]}"
        )

    print(f"\nLanded {len(MACRO_SERIES) - len(failed)}/{len(MACRO_SERIES)} series")
    print("FRED macro landing complete")


main()
