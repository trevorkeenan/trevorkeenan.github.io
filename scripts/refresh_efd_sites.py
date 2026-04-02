#!/usr/bin/env python3
"""Refresh a cached EFD site snapshot for the FLUXNET Data Explorer."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

SITES_LIST_URL = "https://www.europe-fluxdata.eu/home/sites-list"
REQUEST_PAGE_URL = "https://www.europe-fluxdata.eu/home/data/request-data"
DATA_POLICY_URL = "https://www.europe-fluxdata.eu/home/data/data-policy"
GUIDELINES_URL = "https://www.europe-fluxdata.eu/home/guidelines/obtaining-data/general-information"
PUBLIC_SITE_JSON_URL = "https://www.europe-fluxdata.eu/GService.asmx/getSites"
PUBLIC_SITE_CSV_URL = "https://gaia.agraria.unitus.it/homeData/SitesList.csv"
USER_AGENT = "trevorkeenan.github.io/fluxnet-explorer-efd-refresh"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_RETRIES = 5
DEFAULT_RETRY_DELAY_SECONDS = 2.0

EFD_SOURCE = "EFD"
EFD_SOURCE_ORIGIN = "efd"
EFD_SOURCE_PRIORITY = 50
REQUEST_PAGE_DOWNLOAD_MODE = "request_page"
EFD_SOURCE_REASON = (
    "Listed in the public European Fluxes Database site catalog. "
    "Access is request-based via EFD login; some data may require PI approval "
    "and download links are emailed after request submission."
)

OUTPUT_COLUMNS: Sequence[str] = (
    "site_id",
    "site_name",
    "country",
    "data_hub",
    "network",
    "source_network",
    "processing_lineage",
    "vegetation_type",
    "first_year",
    "last_year",
    "latitude",
    "longitude",
    "download_link",
    "download_mode",
    "source",
    "source_label",
    "source_reason",
    "source_priority",
    "source_origin",
    "flux_list",
    "access_label",
    "data_use_label",
    "request_page_url",
    "sites_list_url",
    "guidelines_url",
    "data_policy_url",
    "last_updated",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-csv", required=True, help="Destination CSV path.")
    parser.add_argument("--output-json", required=True, help="Destination JSON path.")
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Per-request timeout in seconds (default: {DEFAULT_TIMEOUT_SECONDS}).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help=f"Maximum retries per request (default: {DEFAULT_RETRIES}).",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=DEFAULT_RETRY_DELAY_SECONDS,
        help=f"Base retry delay in seconds (default: {DEFAULT_RETRY_DELAY_SECONDS}).",
    )
    parser.add_argument(
        "--snapshot-updated-at",
        default="",
        help="Snapshot refresh timestamp in ISO-8601 form.",
    )
    parser.add_argument(
        "--snapshot-updated-date",
        default="",
        help="Snapshot refresh date in YYYY-MM-DD form.",
    )
    return parser.parse_args()


def clean_string(value: Any) -> str:
    raw = unescape(str(value if value is not None else "")).strip()
    if not raw or raw.upper() == "NULL":
        return ""
    return raw


def derive_country(site_id: str) -> str:
    raw = clean_string(site_id)
    if not raw:
        return ""
    if "-" in raw:
        return raw.split("-", 1)[0].upper()
    if "_" in raw:
        return raw.split("_", 1)[0].upper()
    return raw[:2].upper()


def maybe_float(value: Any) -> str:
    raw = clean_string(value)
    if not raw:
        return ""
    try:
        number = float(raw)
    except ValueError:
        return ""
    return str(number)


def split_html_tokens(value: Any) -> List[str]:
    raw = clean_string(value)
    if not raw:
        return []
    normalized = re.sub(r"(?i)<br\s*/?>", "\n", raw)
    normalized = normalized.replace("|", "\n")
    normalized = normalized.replace("\r", "\n")
    tokens: List[str] = []
    seen = set()
    for piece in normalized.split("\n"):
        token = clean_string(piece).strip(" \"'")
        if not token:
            continue
        key = token.casefold()
        if key in seen:
            continue
        seen.add(key)
        tokens.append(token)
    return tokens


def join_tokens(tokens: Iterable[str]) -> str:
    cleaned = [clean_string(token).strip(" \"'") for token in tokens]
    return "; ".join(token for token in cleaned if token)


def extract_entry_names(entries: Any) -> List[str]:
    names: List[str] = []
    if not isinstance(entries, list):
        return names
    for entry in entries:
        if isinstance(entry, dict):
            name = clean_string(entry.get("Name"))
            if name:
                names.append(name)
    return names


def normalize_multivalue(value: Any, fallback_entries: Any = None) -> str:
    tokens = split_html_tokens(value)
    if not tokens:
        tokens = extract_entry_names(fallback_entries)
    return join_tokens(tokens)


def normalize_flux_list(raw_value: Any, fallback_entries: Any = None) -> str:
    tokens = split_html_tokens(raw_value)
    if not tokens:
        tokens = extract_entry_names(fallback_entries)
    return join_tokens(tokens)


def normalize_site_id(value: Any) -> str:
    return clean_string(value)


def score_row(row: Dict[str, Any]) -> Tuple[int, int]:
    populated = 0
    for key in (
        "site_name",
        "network",
        "vegetation_type",
        "flux_list",
        "access_label",
        "data_use_label",
        "latitude",
        "longitude",
    ):
        if clean_string(row.get(key)):
            populated += 1
    return populated, len(clean_string(row.get("flux_list")))


def build_site_row(raw: Dict[str, Any], last_updated: str) -> Optional[Dict[str, Any]]:
    site_id = normalize_site_id(raw.get("Code") or raw.get("site_id"))
    if not site_id:
        return None

    network = normalize_multivalue(raw.get("Networks"), raw.get("lNet"))
    row: Dict[str, Any] = {
        "site_id": site_id,
        "site_name": clean_string(raw.get("Name") or raw.get("site_name")),
        "country": derive_country(site_id),
        "data_hub": EFD_SOURCE,
        "network": network,
        "source_network": network,
        "processing_lineage": "",
        "vegetation_type": clean_string(raw.get("Igbp") or raw.get("vegetation_type")),
        "first_year": "",
        "last_year": "",
        "latitude": maybe_float(raw.get("Latitude") or raw.get("latitude")),
        "longitude": maybe_float(raw.get("Longitude") or raw.get("longitude")),
        "download_link": REQUEST_PAGE_URL,
        "download_mode": REQUEST_PAGE_DOWNLOAD_MODE,
        "source": EFD_SOURCE,
        "source_label": EFD_SOURCE,
        "source_reason": EFD_SOURCE_REASON,
        "source_priority": str(EFD_SOURCE_PRIORITY),
        "source_origin": EFD_SOURCE_ORIGIN,
        "flux_list": normalize_flux_list(raw.get("FluxList") or raw.get("flux_list"), raw.get("mFlux")),
        "access_label": normalize_multivalue(raw.get("Access")),
        "data_use_label": normalize_multivalue(raw.get("DataUse")),
        "request_page_url": REQUEST_PAGE_URL,
        "sites_list_url": SITES_LIST_URL,
        "guidelines_url": GUIDELINES_URL,
        "data_policy_url": DATA_POLICY_URL,
        "last_updated": last_updated,
    }
    return row


def build_site_row_from_csv(raw: Dict[str, Any], last_updated: str) -> Optional[Dict[str, Any]]:
    site_id = normalize_site_id(raw.get("Site Code") or raw.get("site_id"))
    if not site_id:
        return None
    mapped = {
        "Code": site_id,
        "Name": clean_string(raw.get("Site Name")),
        "Igbp": clean_string(raw.get("IGBP Code")),
        "Latitude": clean_string(raw.get("Site Latitude")),
        "Longitude": clean_string(raw.get("Site Longitude")),
        "FluxList": clean_string(raw.get("Fluxes")),
    }
    return build_site_row(mapped, last_updated)


def dedupe_site_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    chosen: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        site_id = normalize_site_id(row.get("site_id"))
        if not site_id:
            continue
        existing = chosen.get(site_id)
        if existing is None or score_row(row) > score_row(existing):
            chosen[site_id] = dict(row, site_id=site_id)
    return [chosen[site_id] for site_id in sorted(chosen)]


def load_existing_meta(output_path: Path) -> Dict[str, Any]:
    if not output_path.exists():
        return {}
    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    meta = payload.get("meta")
    return meta if isinstance(meta, dict) else {}


def normalize_snapshot_updated_at(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_snapshot_updated_date(value: str, fallback_at: str = "") -> str:
    raw = (value or "").strip()
    if raw and re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    fallback = normalize_snapshot_updated_at(fallback_at)
    if fallback:
        return fallback.split("T", 1)[0]
    return ""


def choose_snapshot_updated_fields(
    existing_meta: Dict[str, Any],
    version_value: str,
    requested_updated_at: str,
    requested_updated_date: str,
) -> Tuple[str, str]:
    existing_version = clean_string(existing_meta.get("version"))
    existing_updated_at = normalize_snapshot_updated_at(clean_string(existing_meta.get("snapshot_updated_at")))
    existing_updated_date = normalize_snapshot_updated_date(
        clean_string(existing_meta.get("snapshot_updated_date")),
        existing_updated_at,
    )
    if existing_version == version_value and existing_updated_at and existing_updated_date:
        return existing_updated_at, existing_updated_date

    updated_at = normalize_snapshot_updated_at(requested_updated_at)
    if not updated_at:
        updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    updated_date = normalize_snapshot_updated_date(requested_updated_date, updated_at)
    return updated_at, updated_date


def fetch_json_with_retry(
    url: str,
    payload: Dict[str, Any],
    timeout: int,
    retries: int,
    retry_delay: float,
) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "User-Agent": USER_AGENT,
        "X-Requested-With": "XMLHttpRequest",
    }
    errors: List[str] = []
    for attempt in range(1, max(1, retries) + 1):
        try:
            request = Request(url, data=body, headers=headers, method="POST")
            with urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except (HTTPError, URLError, json.JSONDecodeError) as exc:
            errors.append(f"attempt {attempt}: {exc}")
            if attempt >= max(1, retries):
                break
            time.sleep(retry_delay * attempt)
    raise RuntimeError(f"Failed to fetch EFD public site JSON from {url}: {'; '.join(errors)}")


def fetch_text_with_retry(
    url: str,
    timeout: int,
    retries: int,
    retry_delay: float,
) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/csv,text/plain,*/*",
    }
    errors: List[str] = []
    for attempt in range(1, max(1, retries) + 1):
        try:
            request = Request(url, headers=headers, method="GET")
            with urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8-sig")
        except (HTTPError, URLError, UnicodeDecodeError) as exc:
            errors.append(f"attempt {attempt}: {exc}")
            if attempt >= max(1, retries):
                break
            time.sleep(retry_delay * attempt)
    raise RuntimeError(f"Failed to fetch EFD fallback CSV from {url}: {'; '.join(errors)}")


def parse_public_site_json(payload: Dict[str, Any], last_updated: str) -> List[Dict[str, Any]]:
    rows = payload.get("d")
    if not isinstance(rows, list):
        raise RuntimeError("Unexpected EFD site JSON payload shape: missing list at key 'd'.")
    parsed = [build_site_row(row, last_updated) for row in rows if isinstance(row, dict)]
    return dedupe_site_rows([row for row in parsed if row])


def parse_public_site_csv(text: str, last_updated: str) -> List[Dict[str, Any]]:
    reader = csv.DictReader(text.splitlines())
    parsed = [build_site_row_from_csv(row, last_updated) for row in reader]
    return dedupe_site_rows([row for row in parsed if row])


def load_public_site_rows(
    last_updated: str,
    timeout: int,
    retries: int,
    retry_delay: float,
) -> Tuple[List[Dict[str, Any]], str]:
    try:
        payload = fetch_json_with_retry(
            PUBLIC_SITE_JSON_URL,
            {"st": {"RefProj": 0}},
            timeout,
            retries,
            retry_delay,
        )
        return parse_public_site_json(payload, last_updated), ""
    except Exception as primary_error:
        csv_text = fetch_text_with_retry(PUBLIC_SITE_CSV_URL, timeout, retries, retry_delay)
        rows = parse_public_site_csv(csv_text, last_updated)
        warning = (
            "Used the public EFD CSV export fallback because the public JSON endpoint "
            f"was unavailable: {primary_error}"
        )
        return rows, warning


def build_version_payload(records: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    version_columns = [column for column in OUTPUT_COLUMNS if column != "last_updated"]
    return {
        "columns": version_columns,
        "rows": [[record.get(column, "") for column in version_columns] for record in records],
    }


def write_csv(path: Path, records: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(OUTPUT_COLUMNS))
        writer.writeheader()
        writer.writerows(records)


def write_json(
    path: Path,
    records: Sequence[Dict[str, Any]],
    snapshot_updated_at: str,
    snapshot_updated_date: str,
    version_value: str,
) -> None:
    columns = list(OUTPUT_COLUMNS)
    payload = {
        "meta": {
            "schema_version": 1,
            "version": version_value,
            "snapshot_updated_at": snapshot_updated_at,
            "snapshot_updated_date": snapshot_updated_date,
        },
        "columns": columns,
        "rows": [[record.get(column, "") for column in columns] for record in records],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")


def main() -> None:
    args = parse_args()
    output_csv = Path(args.output_csv)
    output_json = Path(args.output_json)

    provisional_updated_at = normalize_snapshot_updated_at(args.snapshot_updated_at)
    if not provisional_updated_at:
        provisional_updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    records, warning = load_public_site_rows(
        provisional_updated_at,
        args.timeout,
        args.retries,
        args.retry_delay,
    )
    if not records:
        raise RuntimeError("Refusing to write empty EFD snapshot.")

    version_payload = build_version_payload(records)
    canonical_data = json.dumps(version_payload, ensure_ascii=True, separators=(",", ":"))
    version_hash = hashlib.sha256(canonical_data.encode("utf-8")).hexdigest()
    version_value = f"sha256:{version_hash}"

    existing_meta = load_existing_meta(output_json)
    snapshot_updated_at, snapshot_updated_date = choose_snapshot_updated_fields(
        existing_meta,
        version_value,
        args.snapshot_updated_at,
        args.snapshot_updated_date,
    )

    finalized_records = [dict(record, last_updated=snapshot_updated_at) for record in records]
    write_csv(output_csv, finalized_records)
    write_json(output_json, finalized_records, snapshot_updated_at, snapshot_updated_date, version_value)

    print(f"Wrote {len(finalized_records)} EFD site rows")
    print(f"CSV: {output_csv}")
    print(f"JSON: {output_json}")
    print(f"Version: {version_value}")
    if warning:
        print(f"Warning: {warning}")


if __name__ == "__main__":
    main()
