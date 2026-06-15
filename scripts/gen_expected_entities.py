#!/usr/bin/env python3
"""Generate the dbt coverage seed from the terraform source-of-truth configs.

The dbt coverage tests need the EXPECTED entity universes (which symbols/series
should exist). Those already live in terraform; rather than hand-maintain a
second copy in dbt (drift risk), this derives the seed from them.

Re-run whenever a universe config changes (add/remove a symbol or series):

    python scripts/gen_expected_entities.py

Then `dbt seed` (or `dbt build`) loads the refreshed seed. Keep the generated
CSV committed so the dbt container can seed it without running this script.
"""
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TERRAFORM = ROOT / "terraform"
SEED = ROOT / "dbt_modeling" / "seeds" / "expected_entities.csv"

# (source label used by the coverage tests, config file, key path to the list)
SOURCES = [
    ("crypto", "crypto_symbols.json", lambda c: c["symbols"]),
    ("equity", "equity_tickers.json", lambda c: c["symbols"]),
    ("fred", "macro_series.json", lambda c: [s["series_id"] for s in c["series"]]),
]


def main() -> None:
    rows = []
    for source, filename, extract in SOURCES:
        config = json.loads((TERRAFORM / filename).read_text())
        for entity_id in extract(config):
            rows.append((source, entity_id))

    SEED.parent.mkdir(parents=True, exist_ok=True)
    with SEED.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["source", "entity_id"])
        writer.writerows(rows)

    print(f"Wrote {len(rows)} expected entities -> {SEED.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
