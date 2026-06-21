#!/usr/bin/env bash
# One-off historical backfill — runs landing -> bronze -> transform per source
# over a wide date range, OUTSIDE Airflow.
#
# Airflow runs with catchup=False, so it only ingests forward from "now". History
# (a fresh deploy, or a newly added series/symbol) must be loaded here instead.
# The whole range lands under ONE ingest_date batch; the real time axis is each
# row's `date` column, and silver MERGE dedups, so re-running is safe.
#
# Prereq: `terraform apply` first, so the deployed Glue scripts honor api_start_date
# (FRED needs the observation_start = min(api_start_date, window) fix).
#
# Usage: ./backfill.sh <start YYYY-MM-DD> <end YYYY-MM-DD> [source ...]
#   ./backfill.sh 2024-01-01 2026-06-20                # all three sources
#   ./backfill.sh 2024-01-01 2026-06-20 fred           # just FRED
set -euo pipefail

REGION="eu-central-1"
PROJECT="financial-data-lakehouse"

START="${1:?usage: backfill.sh <start YYYY-MM-DD> <end YYYY-MM-DD> [sources...]}"
END="${2:?usage: backfill.sh <start YYYY-MM-DD> <end YYYY-MM-DD> [sources...]}"
shift 2
if [ "$#" -gt 0 ]; then SOURCES=("$@"); else SOURCES=(fred binance twelvedata); fi
INGEST_DATE="$START"            # batch label only; not the data's time axis

# run_glue <job-name> <arguments-json> — starts a Glue job run and blocks until
# it reaches a terminal state. Returns non-zero (and prints the error) on failure.
run_glue() {
  local job="$1" args="$2" id st
  id=$(aws glue start-job-run --region "$REGION" --job-name "$job" \
        --arguments "$args" --query JobRunId --output text)
  printf '  %-46s %s ... ' "$job" "$id"
  while :; do
    st=$(aws glue get-job-run --region "$REGION" --job-name "$job" --run-id "$id" \
          --query 'JobRun.JobRunState' --output text)
    case "$st" in
      SUCCEEDED) echo "SUCCEEDED"; return 0 ;;
      FAILED|STOPPED|TIMEOUT)
        echo "$st"
        aws glue get-job-run --region "$REGION" --job-name "$job" --run-id "$id" \
          --query 'JobRun.ErrorMessage' --output text >&2
        return 1 ;;
    esac
    sleep 15
  done
}

date_args=$(printf '{"--api_start_date":"%s","--api_end_date":"%s","--ingest_date":"%s"}' "$START" "$END" "$INGEST_DATE")
ingest_arg=$(printf '{"--ingest_date":"%s"}' "$INGEST_DATE")

for src in "${SOURCES[@]}"; do
  echo "=== $src  ($START -> $END, ingest_date=$INGEST_DATE) ==="
  run_glue "$PROJECT-landing-$src"   "$date_args"   # landing + bronze take the range
  run_glue "$PROJECT-bronze-$src"    "$date_args"
  run_glue "$PROJECT-transform-$src" "$ingest_arg"  # transform reads the ingest_date batch
done

echo "Backfill complete. Gold refreshes on the next dbt_build (or run dbt manually)."
