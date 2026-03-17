#!/usr/bin/env python3
"""Refresh a cached ICOS-direct FLUXNET snapshot for the FLUXNET Data Explorer."""

from __future__ import annotations

import argparse
import csv
import functools
import hashlib
import json
import math
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen

DATACITE_API_URL = "https://api.datacite.org/dois"
DATACITE_QUERY = 'publisher:"ICOS Carbon Portal" AND types.resourceType:"FLUXNET zip archive"'
ICOS_SOURCE = "ICOS"
ICOS_SOURCE_ORIGIN = "icos_direct"
ICOS_SOURCE_PRIORITY = 300
PROJECT_FLUXNET = "http://meta.icos-cp.eu/resources/projects/FLUXNET"
ARCHIVE_SPEC_URI = "http://meta.icos-cp.eu/resources/cpmeta/miscFluxnetArchiveProduct"
PRODUCT_SPEC_URI = "http://meta.icos-cp.eu/resources/cpmeta/miscFluxnetProduct"
TARGET_METADATA_HOST = "meta.icos-cp.eu"
DEFAULT_PAGE_SIZE = 100
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_RETRIES = 5
DEFAULT_RETRY_DELAY_SECONDS = 2.0

OUTPUT_COLUMNS: Sequence[str] = (
    "site_id",
    "site_name",
    "country",
    "data_hub",
    "network",
    "source_network",
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
    "object_id",
    "file_name",
    "direct_download_url",
    "metadata_url",
    "access_url",
    "object_spec",
    "project",
    "coverage_start",
    "coverage_end",
    "production_end",
    "citation",
)

YEAR_RANGE_RE = re.compile(r"_(\d{4})-(\d{4})(?:[_\.]|$)")
CURRENT_VERSION_RE = re.compile(r"_v(\d+(?:\.\d+)*)_r(\d+)", re.IGNORECASE)
BETA_VERSION_RE = re.compile(r"_beta[-_]?(\d+)", re.IGNORECASE)
RESOLUTION_PRODUCT_RE = re.compile(r"_(HH|HR|DD|WW|MM|YY|NRT)_", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-csv", required=True, help="Destination CSV path.")
    parser.add_argument("--output-json", required=True, help="Destination JSON path.")
    parser.add_argument(
        "--shuttle-csv",
        default="",
        help="Optional FLUXNET Shuttle CSV used only for overlap/suppression counts.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"DataCite page size (default: {DEFAULT_PAGE_SIZE}).",
    )
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


def first_present(mapping: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def maybe_float(value: Any) -> Optional[float]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        number = float(raw)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def maybe_iso_to_year(value: str) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).year
    except ValueError:
        return None


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


def load_existing_meta(output_path: Path) -> Dict[str, Any]:
    if not output_path.exists():
        return {}
    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    meta = payload.get("meta")
    return meta if isinstance(meta, dict) else {}


def choose_snapshot_updated_fields(
    existing_meta: Dict[str, Any],
    version_value: str,
    requested_updated_at: str,
    requested_updated_date: str,
) -> Tuple[str, str]:
    existing_version = str(existing_meta.get("version") or "").strip()
    existing_updated_at = normalize_snapshot_updated_at(str(existing_meta.get("snapshot_updated_at") or ""))
    existing_updated_date = normalize_snapshot_updated_date(
        str(existing_meta.get("snapshot_updated_date") or ""),
        existing_updated_at,
    )
    if existing_version == version_value and existing_updated_at and existing_updated_date:
        return existing_updated_at, existing_updated_date

    updated_at = normalize_snapshot_updated_at(requested_updated_at)
    if not updated_at:
        updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    updated_date = normalize_snapshot_updated_date(requested_updated_date, updated_at)
    return updated_at, updated_date


def parse_year_range(file_name: str, start_iso: str, end_iso: str) -> Tuple[Optional[int], Optional[int]]:
    match = YEAR_RANGE_RE.search(file_name or "")
    if match:
        return int(match.group(1)), int(match.group(2))
    return maybe_iso_to_year(start_iso), maybe_iso_to_year(end_iso)


def parse_network_prefix(file_name: str, site_id: str) -> str:
    file_name = (file_name or "").strip()
    site_id = (site_id or "").strip()
    if not file_name:
        return ""
    if site_id and f"_{site_id}_" in file_name:
        return file_name.split(f"_{site_id}_", 1)[0].strip("_")
    return file_name.split("_", 1)[0]


def build_direct_download_url(object_id: str, file_name: str) -> str:
    encoded_ids = quote(json.dumps([object_id], ensure_ascii=True, separators=(",", ":")), safe="")
    encoded_file_name = quote(file_name, safe="")
    return f"https://data.icos-cp.eu/licence_accept?ids={encoded_ids}&fileName={encoded_file_name}"


def is_resolution_product(file_name: str) -> bool:
    upper = (file_name or "").upper()
    return upper.endswith(".CSV.ZIP") or bool(RESOLUTION_PRODUCT_RE.search(upper))


def is_canonical_archive_candidate(candidate: Dict[str, Any]) -> bool:
    file_name = str(candidate.get("file_name") or "")
    upper = file_name.upper()
    if is_resolution_product(file_name):
        return False
    if candidate.get("object_spec") == ARCHIVE_SPEC_URI:
        return True
    if "FULLSET" in upper:
        return True
    return "_FLUXNET_" in upper


def parse_version_rank(file_name: str) -> Tuple[int, Tuple[int, ...], int]:
    current = CURRENT_VERSION_RE.search(file_name or "")
    if current:
        return (
            2,
            tuple(int(part) for part in current.group(1).split(".")),
            int(current.group(2)),
        )
    beta = BETA_VERSION_RE.search(file_name or "")
    if beta:
        return (1, (int(beta.group(1)),), 0)
    return (0, tuple(), 0)


def coverage_rank(candidate: Dict[str, Any]) -> Tuple[int, int, int]:
    first_year = candidate.get("first_year")
    last_year = candidate.get("last_year")
    first_year = int(first_year) if isinstance(first_year, int) else 0
    last_year = int(last_year) if isinstance(last_year, int) else 0
    length = max(0, (last_year - first_year) + 1) if first_year and last_year and last_year >= first_year else 0
    earlier_start_bonus = (9999 - first_year) if first_year else 0
    return last_year, length, earlier_start_bonus


def canonical_name_bonus(candidate: Dict[str, Any]) -> Tuple[int, int]:
    upper = str(candidate.get("file_name") or "").upper()
    return (
        1 if upper.startswith("FLX_") else 0,
        1 if "FULLSET" in upper else 0,
    )


def compare_candidates(left: Dict[str, Any], right: Dict[str, Any]) -> int:
    ranked_pairs = (
        (1 if is_canonical_archive_candidate(left) else 0, 1 if is_canonical_archive_candidate(right) else 0),
        (parse_version_rank(str(left.get("file_name") or "")), parse_version_rank(str(right.get("file_name") or ""))),
        (coverage_rank(left), coverage_rank(right)),
        (canonical_name_bonus(left), canonical_name_bonus(right)),
        (str(left.get("production_end") or ""), str(right.get("production_end") or "")),
        (str(left.get("coverage_end") or ""), str(right.get("coverage_end") or "")),
    )
    for left_value, right_value in ranked_pairs:
        if left_value == right_value:
            continue
        return -1 if left_value > right_value else 1

    left_tail = (str(left.get("file_name") or ""), str(left.get("object_id") or ""))
    right_tail = (str(right.get("file_name") or ""), str(right.get("object_id") or ""))
    if left_tail < right_tail:
        return -1
    if left_tail > right_tail:
        return 1
    return 0


def choose_best_candidate(candidates: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None
    ordered = sorted(candidates, key=functools.cmp_to_key(compare_candidates))
    best = ordered[0]
    if not is_canonical_archive_candidate(best):
        return None
    return best


def load_shuttle_site_ids(path: str) -> set[str]:
    shuttle_path = Path(path)
    if not path or not shuttle_path.exists():
        return set()
    with shuttle_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return {
            str(row.get("site_id") or "").strip()
            for row in reader
            if str(row.get("site_id") or "").strip()
        }


def dedupe_candidates(candidates: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for candidate in candidates:
        site_id = str(candidate.get("site_id") or "").strip()
        if not site_id:
            continue
        grouped.setdefault(site_id, []).append(candidate)

    chosen: List[Dict[str, Any]] = []
    skipped_noncanonical = 0
    for site_id in sorted(grouped):
        best = choose_best_candidate(grouped[site_id])
        if best is None:
            skipped_noncanonical += 1
            continue
        chosen.append(best)

    return chosen, skipped_noncanonical


def normalize_csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(OUTPUT_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow({column: normalize_csv_value(row.get(column)) for column in OUTPUT_COLUMNS})


def write_json(
    path: Path,
    rows: Sequence[Dict[str, Any]],
    meta_extra: Dict[str, Any],
    snapshot_updated_at: str,
    snapshot_updated_date: str,
) -> str:
    payload_rows = [[row.get(column) for column in OUTPUT_COLUMNS] for row in rows]
    data_payload = {"columns": list(OUTPUT_COLUMNS), "rows": payload_rows}
    canonical_data_json = json.dumps(data_payload, ensure_ascii=True, separators=(",", ":"))
    version_hash = hashlib.sha256(canonical_data_json.encode("utf-8")).hexdigest()
    version_value = f"sha256:{version_hash}"
    existing_meta = load_existing_meta(path)
    updated_at, updated_date = choose_snapshot_updated_fields(
        existing_meta,
        version_value,
        snapshot_updated_at,
        snapshot_updated_date,
    )
    payload = {
        "meta": {
            "schema_version": 1,
            "version": version_value,
            "snapshot_updated_at": updated_at,
            "snapshot_updated_date": updated_date,
            **meta_extra,
        },
        "columns": list(OUTPUT_COLUMNS),
        "rows": payload_rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")
    return version_hash


def fetch_json(
    url: str,
    timeout: int,
    retries: int,
    retry_delay: float,
    *,
    accept: str = "application/json",
    label: str = "",
) -> Dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            request = Request(
                url,
                headers={
                    "Accept": accept,
                    "User-Agent": "trevorkeenan.github.io/fluxnet-explorer-icos-refresh",
                },
                method="GET",
            )
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as err:
            last_error = err
            if attempt >= retries:
                break
            time.sleep(min(30.0, retry_delay * (2 ** (attempt - 1))))
    detail = f" ({label})" if label else ""
    raise RuntimeError(f"Request failed after {retries} attempt(s){detail}: {last_error}")


def fetch_datacite_object_urls(page_size: int, timeout: int, retries: int, retry_delay: float) -> Tuple[List[str], int]:
    object_urls: List[str] = []
    seen: set[str] = set()
    page_number = 1
    total_matches = 0
    while True:
        params = urlencode(
            {
                "query": DATACITE_QUERY,
                "page[size]": page_size,
                "page[number]": page_number,
            }
        )
        payload = fetch_json(
            f"{DATACITE_API_URL}?{params}",
            timeout,
            retries,
            retry_delay,
            label=f"datacite-page-{page_number}",
        )
        if page_number == 1:
            total_matches = int(payload.get("meta", {}).get("total") or 0)
        rows = payload.get("data", [])
        if not rows:
            break
        print(f"DataCite page {page_number}: {len(rows)} dataset(s)", flush=True)
        for row in rows:
            attributes = row.get("attributes", {})
            metadata_url = str(attributes.get("url") or "").strip()
            parsed = urlparse(metadata_url)
            if not metadata_url or parsed.netloc != TARGET_METADATA_HOST or not parsed.path.startswith("/objects/"):
                continue
            if metadata_url not in seen:
                seen.add(metadata_url)
                object_urls.append(metadata_url)
        page_number += 1
    return sorted(object_urls), total_matches


def extract_object_id(metadata_url: str) -> str:
    raw = (metadata_url or "").strip().rstrip("/")
    if not raw:
        return ""
    return raw.rsplit("/", 1)[-1]


def filter_target_metadata(metadata: Dict[str, Any]) -> bool:
    specification = metadata.get("specification", {})
    project_uri = first_present(specification.get("project", {}).get("self", {}), "uri")
    spec_uri = first_present(specification.get("self", {}), "uri")
    file_name = first_present(metadata, "fileName")
    upper = file_name.upper()
    if project_uri != PROJECT_FLUXNET:
        return False
    if spec_uri not in {ARCHIVE_SPEC_URI, PRODUCT_SPEC_URI}:
        return False
    if not upper.startswith("FLX_"):
        return False
    if "FLUXNET" not in upper or not upper.endswith(".ZIP"):
        return False
    return True


def extract_station_place(site_id: str, coverage_geo: Dict[str, Any]) -> Dict[str, Any]:
    for feature in coverage_geo.get("features", []):
        if not isinstance(feature, dict):
            continue
        if str(feature.get("name") or "").strip() == site_id:
            return feature
    return {}


def build_candidate(metadata_url: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    specific_info = metadata.get("specificInfo", {})
    acquisition = specific_info.get("acquisition", {})
    station = acquisition.get("station", {})
    interval = acquisition.get("interval", {})
    location = station.get("location", {})
    station_specific = station.get("specificInfo", {})
    vegetation = first_present(station_specific.get("ecosystemType", {}), "label")
    site_id = first_present(station, "id")
    coverage_geo = metadata.get("coverageGeo", {})
    station_place = extract_station_place(site_id, coverage_geo)
    country = first_present(station, "countryCode")
    if not country:
        country = first_present(station_place.get("containedInPlace", {}), "identifier", "name")
    file_name = first_present(metadata, "fileName")
    object_id = extract_object_id(metadata_url)
    coverage_start = first_present(interval, "start")
    coverage_end = first_present(interval, "stop")
    first_year, last_year = parse_year_range(file_name, coverage_start, coverage_end)
    network = parse_network_prefix(file_name, site_id)
    direct_download_url = build_direct_download_url(object_id, file_name) if object_id and file_name else ""
    references = metadata.get("references", {})
    specification = metadata.get("specification", {})

    return {
        "site_id": site_id,
        "site_name": first_present(station.get("org", {}), "name") or first_present(location, "label") or site_id,
        "country": country,
        "data_hub": ICOS_SOURCE,
        "network": network,
        "source_network": network,
        "vegetation_type": vegetation,
        "first_year": first_year,
        "last_year": last_year,
        "latitude": maybe_float(location.get("lat")),
        "longitude": maybe_float(location.get("lon")),
        "download_link": direct_download_url,
        "download_mode": "direct",
        "source": ICOS_SOURCE,
        "source_label": ICOS_SOURCE,
        "source_reason": "Discovered directly from ICOS Carbon Portal FLUXNET metadata.",
        "source_priority": ICOS_SOURCE_PRIORITY,
        "source_origin": ICOS_SOURCE_ORIGIN,
        "object_id": object_id,
        "file_name": file_name,
        "direct_download_url": direct_download_url,
        "metadata_url": metadata_url,
        "access_url": first_present(metadata, "accessUrl"),
        "object_spec": first_present(specification.get("self", {}), "uri"),
        "project": first_present(specification.get("project", {}).get("self", {}), "uri"),
        "coverage_start": coverage_start,
        "coverage_end": coverage_end,
        "production_end": first_present(metadata.get("submission", {}), "stop", "start"),
        "citation": first_present(references, "citationString"),
    }


def fetch_candidates(page_size: int, timeout: int, retries: int, retry_delay: float) -> Tuple[List[Dict[str, Any]], int, int]:
    object_urls, datacite_total_matches = fetch_datacite_object_urls(page_size, timeout, retries, retry_delay)
    candidates: List[Dict[str, Any]] = []
    for index, metadata_url in enumerate(object_urls, start=1):
        if index == 1 or index % 25 == 0 or index == len(object_urls):
            print(f"Fetching ICOS metadata {index}/{len(object_urls)}: {metadata_url}", flush=True)
        metadata = fetch_json(
            metadata_url,
            timeout,
            retries,
            retry_delay,
            accept="application/json",
            label=f"icos-object-{extract_object_id(metadata_url)}",
        )
        if not filter_target_metadata(metadata):
            continue
        candidate = build_candidate(metadata_url, metadata)
        if candidate.get("site_id"):
            candidates.append(candidate)
        time.sleep(0.05)
    return candidates, datacite_total_matches, len(object_urls)


def main() -> None:
    args = parse_args()
    output_csv = Path(args.output_csv)
    output_json = Path(args.output_json)

    candidates, datacite_total_matches, fetched_metadata_objects = fetch_candidates(
        page_size=max(1, args.page_size),
        timeout=max(1, args.timeout),
        retries=max(1, args.retries),
        retry_delay=max(0.1, args.retry_delay),
    )
    deduped_rows, skipped_noncanonical_sites = dedupe_candidates(candidates)
    deduped_rows = sorted(deduped_rows, key=lambda row: (str(row.get("site_id") or ""), str(row.get("file_name") or "")))

    shuttle_site_ids = load_shuttle_site_ids(args.shuttle_csv)
    suppressed_by_shuttle = sum(1 for row in deduped_rows if str(row.get("site_id") or "") in shuttle_site_ids)
    final_additional_rows = len(deduped_rows) - suppressed_by_shuttle

    write_csv(output_csv, deduped_rows)
    version_hash = write_json(
        output_json,
        deduped_rows,
        meta_extra={
            "discovery_method": "DataCite query + ICOS object metadata JSON",
            "datacite_api_url": DATACITE_API_URL,
            "datacite_query": DATACITE_QUERY,
            "datacite_total_matches": datacite_total_matches,
            "icos_metadata_objects_fetched": fetched_metadata_objects,
            "candidate_rows_discovered": len(candidates),
            "retained_rows_after_dedup": len(deduped_rows),
            "skipped_noncanonical_sites": skipped_noncanonical_sites,
            "suppressed_by_shuttle": suppressed_by_shuttle,
            "final_rows_after_precedence": final_additional_rows,
            "project": PROJECT_FLUXNET,
        },
        snapshot_updated_at=args.snapshot_updated_at,
        snapshot_updated_date=args.snapshot_updated_date,
    )

    print(f"Wrote ICOS-direct CSV: {output_csv}")
    print(f"Wrote ICOS-direct JSON: {output_json}")
    print(f"DataCite FLUXNET zip datasets discovered: {datacite_total_matches}")
    print(f"ICOS object metadata records fetched: {fetched_metadata_objects}")
    print(f"ICOS candidate rows discovered: {len(candidates)}")
    print(f"ICOS rows retained after per-site dedup: {len(deduped_rows)}")
    print(f"ICOS sites skipped as non-canonical: {skipped_noncanonical_sites}")
    print(f"ICOS rows suppressed by Shuttle precedence: {suppressed_by_shuttle}")
    print(f"ICOS rows that would merge into explorer: {final_additional_rows}")
    print(f"Version: sha256:{version_hash}")


if __name__ == "__main__":
    main()
