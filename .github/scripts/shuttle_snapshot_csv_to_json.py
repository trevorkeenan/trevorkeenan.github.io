#!/usr/bin/env python3
"""Convert FLUXNET Shuttle CSV snapshot to a compact browser JSON payload."""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Optional


def to_snake_case(name: str) -> str:
    value = (name or "").strip()
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_").lower()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input CSV path (e.g. assets/shuttle_snapshot.csv)")
    parser.add_argument("--output", required=True, help="Output JSON path (e.g. assets/shuttle_snapshot.json)")
    return parser.parse_args()


def first_present(row: Dict[str, str], *keys: str) -> str:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return str(row[key]).strip()
    return ""


def derive_country(site_id: str, fallback: str = "") -> str:
    if fallback:
        return fallback.strip()
    site_id = (site_id or "").strip()
    if "-" in site_id:
        return site_id.split("-", 1)[0].upper()
    if "_" in site_id:
        return site_id.split("_", 1)[0].upper()
    return site_id[:2].upper() if len(site_id) >= 2 else ""


def maybe_int(value: str) -> Optional[int]:
    value = (value or "").strip()
    if not value:
        return None
    if re.fullmatch(r"-?\d+", value):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def normalize_row(raw_row: Dict[str, str], header_map: Dict[str, str]) -> Dict[str, str]:
    normalized: Dict[str, str] = {}
    for original_key, value in raw_row.items():
        key = header_map.get(original_key, to_snake_case(original_key))
        normalized[key] = "" if value is None else str(value).strip()
    return normalized


def build_record(row: Dict[str, str]) -> Dict[str, object]:
    site_id = first_present(row, "site_id")
    vegetation_type = first_present(row, "vegetation_type", "igbp", "veg_type")
    source_network = first_present(row, "source_network", "product_source_network")
    record: Dict[str, object] = {
        "site_id": site_id,
        "country": derive_country(site_id, first_present(row, "country", "country_code")),
        "data_hub": first_present(row, "data_hub", "hub"),
        "network": first_present(row, "network"),
        "source_network": source_network,
        "vegetation_type": vegetation_type,
        "first_year": maybe_int(first_present(row, "first_year", "year_start")),
        "last_year": maybe_int(first_present(row, "last_year", "year_end")),
        "download_link": first_present(row, "download_link", "url"),
    }

    site_name = first_present(row, "site_name")
    if site_name:
        record["site_name"] = site_name

    return record


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    with input_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        original_headers = reader.fieldnames or []
        if not original_headers:
            raise RuntimeError(f"CSV has no header row: {input_path}")
        header_map = {header: to_snake_case(header) for header in original_headers}
        rows = [normalize_row(row, header_map) for row in reader]

    if not rows:
        raise RuntimeError("Refusing to convert empty snapshot CSV.")

    records = [build_record(row) for row in rows]

    required_fields = ("site_id", "data_hub", "download_link")
    for idx, record in enumerate(records, start=1):
        missing = [field for field in required_fields if not record.get(field)]
        if missing:
            raise RuntimeError(f"Row {idx} missing required fields after normalization: {missing}")

    # Stable compact schema for browser use: one columns list + row arrays reduces key repetition.
    columns: List[str] = ["site_id"]
    if any("site_name" in record for record in records):
        columns.append("site_name")
    columns.extend(["country", "data_hub", "network"])
    if any(record.get("source_network") for record in records):
        columns.append("source_network")
    columns.extend(["vegetation_type", "first_year", "last_year", "download_link"])

    payload_rows: List[List[object]] = []
    for record in records:
        payload_rows.append([record.get(column) for column in columns])

    payload = {"columns": columns, "rows": payload_rows}

    output_path.parent.mkdir(parents=True, exist_ok=True)
    json_text = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    output_path.write_text(json_text, encoding="utf-8")

    print(f"Wrote compact JSON: {output_path}")
    print(f"Columns: {columns}")
    print(f"Rows: {len(payload_rows)}")
    print(f"Bytes: {len(json_text.encode('utf-8'))}")


if __name__ == "__main__":
    main()
