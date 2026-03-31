#!/usr/bin/env python3
"""Refresh a cached JapanFlux2024 snapshot for the FLUXNET Data Explorer.

Discovers all JapanFlux2024 site records from the ADS REST API and builds
a standalone supplement CSV/JSON for merging into the explorer snapshot.

Reference: Ueyama, M. et al. (2025). The JapanFlux2024 dataset for eddy
covariance observations covering Japan and East Asia from 1990 to 2023.
Earth Syst. Sci. Data, 17, 3807-3833.
https://doi.org/10.5194/essd-17-3807-2025
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ADS_API_BASE = "https://ads.nipr.ac.jp/api/v1"
ADS_LANDING_PAGE_BASE = "https://ads.nipr.ac.jp/dataset"

JAPANFLUX_SOURCE = "JapanFlux"
JAPANFLUX_SOURCE_ORIGIN = "japanflux_direct"
JAPANFLUX_SOURCE_PRIORITY = 250

DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_RETRIES = 5
DEFAULT_RETRY_DELAY_SECONDS = 2.0

# Filename pattern: FLX_{SITE}_{DATASET}_{PRODUCT}_{RES}_{FIRST}-{LAST}_{VER}.csv
# Exclude ERA5 companion files when parsing year ranges.
YEAR_RANGE_RE = re.compile(r"_(\d{4})-(\d{4})(?:[_.]|$)")
ERA5_RE = re.compile(r"_ERA5_", re.IGNORECASE)

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
    "metadata_id",
    "version",
    "landing_page_url",
)

# Authoritative inventory of all 83 JapanFlux2024 metadata IDs extracted from the live portal.
# The portal is a Nuxt.js SPA that cannot be scraped; this list was extracted from a browser session.
SITE_INVENTORY: Sequence[Dict[str, Any]] = (
    {"metadata_id": "A20240722-001", "site_id": "JP-Ozm", "site_name": "Oizumi Urban Park", "igbp": "URB", "lat": 34.563470, "lon": 135.533484},
    {"metadata_id": "A20240722-002", "site_id": "JP-BBY", "site_name": "Bibai bog", "igbp": "WET", "lat": 43.322960, "lon": 141.810790},
    {"metadata_id": "A20240722-003", "site_id": "JP-Om1", "site_name": "B11 building OMU", "igbp": "URB", "lat": 34.547178, "lon": 135.502861},
    {"metadata_id": "A20240722-004", "site_id": "JP-Om2", "site_name": "Farm field OMU", "igbp": "GRA", "lat": 34.542453, "lon": 135.508228},
    {"metadata_id": "A20241022-001", "site_id": "JP-Api", "site_name": "Appi forest", "igbp": "DBF", "lat": 40.001359, "lon": 140.936586},
    {"metadata_id": "A20241022-002", "site_id": "JP-Fjy", "site_name": "Fujiyoshida forest", "igbp": "ENF", "lat": 35.454540, "lon": 138.762250},
    {"metadata_id": "A20241022-003", "site_id": "KH-Kmp", "site_name": "Kampong Thom forest", "igbp": "EBF", "lat": 12.744580, "lon": 105.478566},
    {"metadata_id": "A20241022-004", "site_id": "JP-Kwg", "site_name": "Kawagoe forest", "igbp": "DBF", "lat": 35.872500, "lon": 139.486900},
    {"metadata_id": "A20241022-005", "site_id": "JP-Kzw", "site_name": "Karuizawa", "igbp": "DBF", "lat": 36.406667, "lon": 138.572500},
    {"metadata_id": "A20241022-006", "site_id": "MY-LHP", "site_name": "Lambir Hills National Park", "igbp": "EBF", "lat": 4.201007, "lon": 114.039079},
    {"metadata_id": "A20241022-007", "site_id": "JP-MBF", "site_name": "Moshiri Birch Forest", "igbp": "DBF", "lat": 44.384167, "lon": 142.318611},
    {"metadata_id": "A20241022-008", "site_id": "JP-MMF", "site_name": "Moshiri Mixed Forest", "igbp": "MF", "lat": 44.321944, "lon": 142.261389},
    {"metadata_id": "A20241022-009", "site_id": "JP-Mra", "site_name": "Muramatsu agricultural field", "igbp": "CRO", "lat": 37.690275, "lon": 139.194429},
    {"metadata_id": "A20241022-010", "site_id": "RU-NeB", "site_name": "Neleger Burnt Forest", "igbp": "GRA", "lat": 62.325937, "lon": 129.487342},
    {"metadata_id": "A20241022-011", "site_id": "RU-NeC", "site_name": "Neleger Cutover", "igbp": "OSH", "lat": 62.314844, "lon": 129.500075},
    {"metadata_id": "A20241022-012", "site_id": "RU-NeF", "site_name": "Neleger larch forest", "igbp": "DNF", "lat": 62.315615, "lon": 129.499964},
    {"metadata_id": "A20241022-013", "site_id": "ID-PaB", "site_name": "Palangkaraya Drained Burnt forest", "igbp": "OSH", "lat": -2.340796, "lon": 114.037900},
    {"metadata_id": "A20241022-014", "site_id": "CN-HaM", "site_name": "Qinghai Flux Research Site", "igbp": "GRA", "lat": 37.607432, "lon": 101.332000},
    {"metadata_id": "A20241022-015", "site_id": "JP-Sac", "site_name": "Sakai City Office", "igbp": "URB", "lat": 34.573914, "lon": 135.482889},
    {"metadata_id": "A20241022-016", "site_id": "JP-Sb1", "site_name": "Sarobetsu Mire Moss", "igbp": "WET", "lat": 45.104722, "lon": 141.688194},
    {"metadata_id": "A20241022-017", "site_id": "JP-Sb2", "site_name": "Sarobetsu Mire Sasa", "igbp": "WET", "lat": 45.103611, "lon": 141.680833},
    {"metadata_id": "A20241022-018", "site_id": "JP-Srk", "site_name": "Shirakami Beech Forest", "igbp": "DBF", "lat": 40.565485, "lon": 140.127794},
    {"metadata_id": "A20241022-019", "site_id": "JP-SwL", "site_name": "Suwa Lake Site", "igbp": "WAT", "lat": 36.046572, "lon": 138.108353},
    {"metadata_id": "A20241022-020", "site_id": "JP-Ta2", "site_name": "Takayama evergreen coniferous", "igbp": "ENF", "lat": 36.139722, "lon": 137.370833},
    {"metadata_id": "A20241022-021", "site_id": "JP-Tak", "site_name": "Takayama deciduous broadleaf", "igbp": "DBF", "lat": 36.146167, "lon": 137.423111},
    {"metadata_id": "A20241022-022", "site_id": "JP-Tmk", "site_name": "Tomakomai Flux Research Site", "igbp": "DNF", "lat": 42.736972, "lon": 141.516944},
    {"metadata_id": "A20241022-023", "site_id": "RU-Tur", "site_name": "Tura", "igbp": "DNF", "lat": 64.208888, "lon": 100.463555},
    {"metadata_id": "A20241022-024", "site_id": "JP-Yms", "site_name": "Yamashiro forest", "igbp": "DBF", "lat": 34.790278, "lon": 135.840939},
    {"metadata_id": "A20241022-025", "site_id": "JP-Ynf", "site_name": "Yona-Field Tower Site", "igbp": "EBF", "lat": 26.751000, "lon": 128.212667},
    {"metadata_id": "A20241022-026", "site_id": "MN-Hst", "site_name": "Hustai grassland", "igbp": "GRA", "lat": 47.594131, "lon": 105.856439},
    {"metadata_id": "A20241022-027", "site_id": "MN-Nkh", "site_name": "Nalaikh grassland", "igbp": "GRA", "lat": 47.693592, "lon": 107.489342},
    {"metadata_id": "A20241022-028", "site_id": "JP-Hc3", "site_name": "Hachihama Double Crop", "igbp": "CRO", "lat": 34.539672, "lon": 133.911731},
    {"metadata_id": "A20241022-029", "site_id": "RU-Ege", "site_name": "Elgeeii forest station", "igbp": "DNF", "lat": 60.015516, "lon": 133.824012},
    {"metadata_id": "A20241022-030", "site_id": "JP-KaL", "site_name": "Koshin Lake Kasumigaura", "igbp": "WAT", "lat": 36.037778, "lon": 140.404167},
    {"metadata_id": "A20241022-031", "site_id": "JP-Nkm", "site_name": "Nishikoma Site", "igbp": "ENF", "lat": 35.808064, "lon": 137.833883},
    {"metadata_id": "A20241022-032", "site_id": "ID-PaD", "site_name": "Palangkaraya Drained forest", "igbp": "EBF", "lat": -2.346071, "lon": 114.036408},
    {"metadata_id": "A20241022-033", "site_id": "ID-Pag", "site_name": "Palangkaraya Undrained Forest", "igbp": "EBF", "lat": -2.323917, "lon": 113.904392},
    {"metadata_id": "A20241022-034", "site_id": "JP-SMF", "site_name": "Seto Mixed Forest Site", "igbp": "MF", "lat": 35.261528, "lon": 137.078750},
    {"metadata_id": "A20241022-035", "site_id": "RU-SkP", "site_name": "Yakutsk Spasskaya Pad larch", "igbp": "DNF", "lat": 62.254710, "lon": 129.618543},
    {"metadata_id": "A20241022-036", "site_id": "RU-Sk2", "site_name": "Yakutsk Spasskaya Pad Pine", "igbp": "ENF", "lat": 62.241291, "lon": 129.651336},
    {"metadata_id": "A20241022-037", "site_id": "JP-Hc1", "site_name": "Hachihama Intl Rice Experiment", "igbp": "CRO", "lat": 34.537892, "lon": 133.926797},
    {"metadata_id": "A20241022-038", "site_id": "JP-KaP", "site_name": "Kasumigaura lotus paddy", "igbp": "CRO", "lat": 36.080000, "lon": 140.240000},
    {"metadata_id": "A20241022-039", "site_id": "JP-Km1", "site_name": "Kushiro Mire Onnenai Fen", "igbp": "WET", "lat": 43.107511, "lon": 144.330906},
    {"metadata_id": "A20241022-040", "site_id": "JP-Nsb", "site_name": "NIAES Soybean", "igbp": "CRO", "lat": 36.024303, "lon": 140.114975},
    {"metadata_id": "A20241022-041", "site_id": "CN-In1", "site_name": "Inner Mongolia dune", "igbp": "BSV", "lat": 42.929708, "lon": 120.707350},
    {"metadata_id": "A20241022-042", "site_id": "CN-In2", "site_name": "Inner Mongolia grassland", "igbp": "GRA", "lat": 42.933964, "lon": 120.710964},
    {"metadata_id": "A20241022-043", "site_id": "CN-In3", "site_name": "Inner Mongolia soybean", "igbp": "CRO", "lat": 42.925572, "lon": 120.699039},
    {"metadata_id": "A20241022-044", "site_id": "CN-In4", "site_name": "Inner Mongolia maize", "igbp": "CRO", "lat": 42.944133, "lon": 120.726622},
    {"metadata_id": "A20241022-045", "site_id": "CN-In5", "site_name": "Inner Mongolia no grazing", "igbp": "GRA", "lat": 42.934158, "lon": 120.709078},
    {"metadata_id": "A20241022-046", "site_id": "CN-In6", "site_name": "Inner Mongolia heavy grazing", "igbp": "GRA", "lat": 42.934014, "lon": 120.711547},
    {"metadata_id": "A20241022-047", "site_id": "CN-In7", "site_name": "Inner Mongolia light grazing", "igbp": "GRA", "lat": 42.933919, "lon": 120.709606},
    {"metadata_id": "A20241022-048", "site_id": "CN-In8", "site_name": "Inner Mongolia medium grazing", "igbp": "GRA", "lat": 42.933967, "lon": 120.710531},
    {"metadata_id": "A20241022-049", "site_id": "JP-Hc2", "site_name": "Hachihama Experimental Farm", "igbp": "CRO", "lat": 34.537518, "lon": 133.927545},
    {"metadata_id": "A20241210-001", "site_id": "JP-Ako", "site_name": "Akou green belt", "igbp": "EBF", "lat": 34.735192, "lon": 134.374798},
    {"metadata_id": "A20241210-002", "site_id": "JP-Fhk", "site_name": "Fuji Hokuroku Flux Obs Site", "igbp": "DNF", "lat": 35.443556, "lon": 138.764693},
    {"metadata_id": "A20241210-003", "site_id": "JP-Fmt", "site_name": "Field Museum Tama Hills", "igbp": "MF", "lat": 35.638745, "lon": 139.379748},
    {"metadata_id": "A20241210-004", "site_id": "MN-Kbu", "site_name": "Kherlenbayan Ulaan", "igbp": "GRA", "lat": 47.213972, "lon": 108.737333},
    {"metadata_id": "A20241210-005", "site_id": "JP-Khw", "site_name": "Kahoku Experiment watershed", "igbp": "ENF", "lat": 33.136580, "lon": 130.708340},
    {"metadata_id": "A20241210-006", "site_id": "CN-Lsh", "site_name": "Laoshan", "igbp": "DNF", "lat": 45.279839, "lon": 127.578206},
    {"metadata_id": "A20241210-007", "site_id": "JP-Mse", "site_name": "Mase paddy flux site", "igbp": "CRO", "lat": 36.053930, "lon": 140.026930},
    {"metadata_id": "A20241210-008", "site_id": "JP-Nuf", "site_name": "Nagoya University Forest", "igbp": "DBF", "lat": 35.152417, "lon": 136.971889},
    {"metadata_id": "A20241210-009", "site_id": "JP-Shn", "site_name": "Shinshu Univ Experimental Forest", "igbp": "MF", "lat": 35.865755, "lon": 137.932563},
    {"metadata_id": "A20241210-010", "site_id": "JP-Spp", "site_name": "Sapporo forest", "igbp": "DBF", "lat": 42.986843, "lon": 141.385331},
    {"metadata_id": "A20241210-011", "site_id": "MN-Skt", "site_name": "Southern Khentei Taiga", "igbp": "DNF", "lat": 48.351861, "lon": 108.654333},
    {"metadata_id": "A20241210-012", "site_id": "JP-Tdf", "site_name": "Toyota Deciduous Forest", "igbp": "DBF", "lat": 35.035889, "lon": 137.185778},
    {"metadata_id": "A20241210-013", "site_id": "JP-Tgf", "site_name": "TERC Univ of Tsukuba", "igbp": "GRA", "lat": 36.113530, "lon": 140.094880},
    {"metadata_id": "A20241210-014", "site_id": "JP-Toc", "site_name": "Tomakomai Crane site", "igbp": "DBF", "lat": 42.709727, "lon": 141.565898},
    {"metadata_id": "A20241210-015", "site_id": "JP-Tom", "site_name": "Tomakomai Experimental Forest", "igbp": "DBF", "lat": 42.698906, "lon": 141.571488},
    {"metadata_id": "A20241210-016", "site_id": "JP-Tef", "site_name": "Teshio Experimental Forest", "igbp": "DNF", "lat": 45.055808, "lon": 142.107122},
    {"metadata_id": "A20241210-017", "site_id": "MN-Udg", "site_name": "Udleg practice forest", "igbp": "DNF", "lat": 48.256389, "lon": 106.851111},
    {"metadata_id": "A20241210-018", "site_id": "JP-Kgu", "site_name": "Kugahara urban residential", "igbp": "URB", "lat": 35.582859, "lon": 139.693543},
    {"metadata_id": "A20241210-019", "site_id": "JP-Yrp", "site_name": "Yawara Rice paddy", "igbp": "CRO", "lat": 36.007667, "lon": 140.030175},
    {"metadata_id": "A20241210-020", "site_id": "JP-Hrt", "site_name": "Hiratsuka Rice Paddy", "igbp": "CRO", "lat": 35.362778, "lon": 139.338056},
    {"metadata_id": "A20241210-021", "site_id": "TH-Kms", "site_name": "Kamphaeng Saen Rice Paddy", "igbp": "CRO", "lat": 14.009167, "lon": 99.984167},
    {"metadata_id": "A20241210-022", "site_id": "JP-Tkb", "site_name": "Tsukuba Experimental Watershed", "igbp": "ENF", "lat": 36.173379, "lon": 140.176634},
    {"metadata_id": "A20241210-023", "site_id": "TH-Kog", "site_name": "Kog-Ma Watershed", "igbp": "EBF", "lat": 18.800000, "lon": 98.900000},
    {"metadata_id": "A20241210-024", "site_id": "TH-Mae", "site_name": "Mae Moh plantation", "igbp": "DBF", "lat": 18.383333, "lon": 99.716667},
    {"metadata_id": "A20241210-025", "site_id": "JP-Nap", "site_name": "Nunoike Agricultural Pond", "igbp": "WAT", "lat": 34.774850, "lon": 134.892442},
    {"metadata_id": "A20241210-026", "site_id": "JP-Km2", "site_name": "Kushiro Mire Akanuma Bog", "igbp": "WET", "lat": 43.100000, "lon": 144.350000},
    {"metadata_id": "A20241210-027", "site_id": "RU-USk", "site_name": "Ulakhan Sykkhan Alas", "igbp": "GRA", "lat": 62.150995, "lon": 130.527517},
    {"metadata_id": "A20241210-028", "site_id": "JP-NsM", "site_name": "Nasu Research Manure Plot", "igbp": "GRA", "lat": 36.915833, "lon": 139.935833},
    {"metadata_id": "A20241210-029", "site_id": "JP-NsC", "site_name": "Nasu Research Chemical Fert Plot", "igbp": "GRA", "lat": 36.915000, "lon": 139.936667},
    {"metadata_id": "A20241210-030", "site_id": "JP-Tmd", "site_name": "Tomakomai Flux Research Disturbed", "igbp": "DNF", "lat": 42.735911, "lon": 141.523147},
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


def _build_ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    return ctx


def ads_api_get(
    path: str,
    timeout: int,
    retries: int,
    retry_delay: float,
    label: str = "",
) -> Any:
    """GET a JSON response from the ADS REST API with retry logic."""
    url = f"{ADS_API_BASE}/{path}"
    ctx = _build_ssl_context()
    last_error: Optional[Exception] = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            request = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "trevorkeenan.github.io/fluxnet-explorer-japanflux-refresh",
                },
                method="GET",
            )
            with urlopen(request, timeout=timeout, context=ctx) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as err:
            detail = err.read().decode("utf-8", "replace")
            last_error = RuntimeError(f"HTTP {err.code}: {detail}")
            if attempt >= retries:
                break
            time.sleep(min(30.0, retry_delay * (2 ** (attempt - 1))))
        except (URLError, TimeoutError, json.JSONDecodeError, OSError) as err:
            last_error = err
            if attempt >= retries:
                break
            time.sleep(min(30.0, retry_delay * (2 ** (attempt - 1))))
    detail_label = f" ({label})" if label else ""
    raise RuntimeError(f"ADS API request failed after {retries} attempt(s){detail_label}: {last_error}")


def head_check(url: str, timeout: int) -> bool:
    """Issue a HEAD request and return True if the server responds with 2xx/3xx."""
    ctx = _build_ssl_context()
    try:
        request = Request(
            url,
            headers={"User-Agent": "trevorkeenan.github.io/fluxnet-explorer-japanflux-refresh"},
            method="HEAD",
        )
        with urlopen(request, timeout=timeout, context=ctx) as response:
            return response.status < 400
    except Exception:
        return False


def get_current_version(
    metadata_id: str,
    timeout: int,
    retries: int,
    retry_delay: float,
) -> str:
    """Get the latest version string for a metadata record."""
    versions = ads_api_get(
        f"metadata/{metadata_id}/versions",
        timeout,
        retries,
        retry_delay,
        label=f"versions:{metadata_id}",
    )
    if isinstance(versions, list) and versions:
        # Return the last (latest) version
        return str(versions[-1].get("version", versions[-1]) if isinstance(versions[-1], dict) else versions[-1])
    if isinstance(versions, dict):
        version_list = versions.get("versions", [])
        if version_list:
            v = version_list[-1]
            return str(v.get("version", v) if isinstance(v, dict) else v)
    raise RuntimeError(f"No versions found for {metadata_id}: {versions}")


def get_data_directory(
    metadata_id: str,
    version: str,
    timeout: int,
    retries: int,
    retry_delay: float,
) -> list:
    """Get the DATA directory listing for a metadata record."""
    return ads_api_get(
        f"metadata/{metadata_id}/{version}/directory?object_type=DATA",
        timeout,
        retries,
        retry_delay,
        label=f"directory:{metadata_id}",
    )


def get_allvars_files(
    metadata_id: str,
    version: str,
    timeout: int,
    retries: int,
    retry_delay: float,
) -> list:
    """Get the file listing within the ALLVARS subdirectory."""
    return ads_api_get(
        f"metadata/{metadata_id}/{version}/directory?object_type=DATA&path=ALLVARS",
        timeout,
        retries,
        retry_delay,
        label=f"allvars:{metadata_id}",
    )


def parse_year_range_from_files(files: list) -> Tuple[Optional[int], Optional[int]]:
    """Parse first_year/last_year from ALLVARS filenames, excluding ERA5 companions."""
    first_year: Optional[int] = None
    last_year: Optional[int] = None
    for entry in files:
        name = str(entry.get("name", "") if isinstance(entry, dict) else entry)
        if ERA5_RE.search(name):
            continue
        match = YEAR_RANGE_RE.search(name)
        if match:
            fy, ly = int(match.group(1)), int(match.group(2))
            if first_year is None or fy < first_year:
                first_year = fy
            if last_year is None or ly > last_year:
                last_year = ly
    return first_year, last_year


def build_download_url(metadata_id: str, version: str) -> str:
    """Build a candidate direct download URL for the DATA ZIP."""
    # Construct the download path based on the file_info pattern
    version_normalized = version.replace(".", "")
    return f"https://ads.nipr.ac.jp/download/JapanFlux/{metadata_id}/v{version_normalized}/dataset_zip"


def derive_country(site_id: str) -> str:
    """Extract 2-letter country code from the site ID prefix."""
    site_id = (site_id or "").strip()
    if "-" in site_id:
        return site_id.split("-", 1)[0].upper()
    return site_id[:2].upper() if len(site_id) >= 2 else ""


def normalize_csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


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


def process_site(
    entry: Dict[str, Any],
    timeout: int,
    retries: int,
    retry_delay: float,
) -> Optional[Dict[str, Any]]:
    """Process a single site from the inventory, querying the ADS API."""
    metadata_id = entry["metadata_id"]
    site_id = entry["site_id"]

    try:
        version = get_current_version(metadata_id, timeout, retries, retry_delay)
    except RuntimeError as err:
        print(f"  WARN: {site_id} ({metadata_id}): failed to get version: {err}", flush=True)
        return None

    # Check DATA directory exists and is accessible
    try:
        data_dirs = get_data_directory(metadata_id, version, timeout, retries, retry_delay)
    except RuntimeError as err:
        print(f"  WARN: {site_id} ({metadata_id}): failed to get data directory: {err}", flush=True)
        return None

    has_allvars = False
    is_public = False
    for d in (data_dirs if isinstance(data_dirs, list) else []):
        if isinstance(d, dict) and str(d.get("name", "")).upper() == "ALLVARS":
            has_allvars = True
            if str(d.get("authority", "")).lower() == "allow":
                is_public = True
            break

    if not has_allvars:
        print(f"  WARN: {site_id} ({metadata_id}): no ALLVARS directory found", flush=True)
        return None
    if not is_public:
        print(f"  WARN: {site_id} ({metadata_id}): ALLVARS not publicly accessible", flush=True)
        return None

    # Parse year range from ALLVARS filenames
    try:
        allvars_files = get_allvars_files(metadata_id, version, timeout, retries, retry_delay)
    except RuntimeError as err:
        print(f"  WARN: {site_id} ({metadata_id}): failed to list ALLVARS files: {err}", flush=True)
        return None

    first_year, last_year = parse_year_range_from_files(allvars_files)

    # Build and validate download URL
    candidate_url = build_download_url(metadata_id, version)
    landing_page = f"{ADS_LANDING_PAGE_BASE}/{metadata_id}"
    download_link = landing_page  # Default to landing page

    if head_check(candidate_url, timeout):
        download_link = candidate_url
        download_mode = "direct"
    else:
        download_mode = "landing_page"

    return {
        "site_id": site_id,
        "site_name": entry["site_name"],
        "country": derive_country(site_id),
        "data_hub": JAPANFLUX_SOURCE,
        "network": JAPANFLUX_SOURCE,
        "source_network": JAPANFLUX_SOURCE,
        "vegetation_type": entry.get("igbp", ""),
        "first_year": first_year,
        "last_year": last_year,
        "latitude": entry.get("lat"),
        "longitude": entry.get("lon"),
        "download_link": download_link,
        "download_mode": download_mode,
        "source": JAPANFLUX_SOURCE,
        "source_label": JAPANFLUX_SOURCE,
        "source_reason": (
            "Available from the JapanFlux2024 dataset (Ueyama et al., 2025, ESSD) "
            "hosted on the ADS archive. CC BY 4.0."
        ),
        "source_priority": JAPANFLUX_SOURCE_PRIORITY,
        "source_origin": JAPANFLUX_SOURCE_ORIGIN,
        "metadata_id": metadata_id,
        "version": version,
        "landing_page_url": landing_page,
    }


def main() -> None:
    args = parse_args()
    output_csv = Path(args.output_csv)
    output_json = Path(args.output_json)

    print(f"JapanFlux2024 refresh: processing {len(SITE_INVENTORY)} sites from inventory", flush=True)

    rows: List[Dict[str, Any]] = []
    failed_sites: List[str] = []
    landing_page_only: List[str] = []
    total = len(SITE_INVENTORY)

    for index, entry in enumerate(SITE_INVENTORY, start=1):
        site_id = entry["site_id"]
        metadata_id = entry["metadata_id"]
        print(f"  [{index}/{total}] {site_id} ({metadata_id})...", flush=True)

        row = process_site(
            entry,
            timeout=max(1, args.timeout),
            retries=max(1, args.retries),
            retry_delay=max(0.1, args.retry_delay),
        )

        if row is None:
            failed_sites.append(site_id)
            continue

        if row["download_mode"] == "landing_page":
            landing_page_only.append(site_id)

        rows.append(row)

        # Polite delay between API requests
        if index < total:
            time.sleep(0.2)

    rows.sort(key=lambda r: (str(r.get("site_id", "")), str(r.get("metadata_id", ""))))

    print(f"\nJapanFlux2024 refresh complete:", flush=True)
    print(f"  Sites processed: {total}", flush=True)
    print(f"  Sites succeeded: {len(rows)}", flush=True)
    print(f"  Sites failed: {len(failed_sites)}", flush=True)
    if failed_sites:
        print(f"  Failed sites: {', '.join(failed_sites)}", flush=True)
    print(f"  Direct download URLs: {len(rows) - len(landing_page_only)}", flush=True)
    print(f"  Landing page fallbacks: {len(landing_page_only)}", flush=True)
    if landing_page_only:
        print(f"  Landing page sites: {', '.join(landing_page_only)}", flush=True)

    write_csv(output_csv, rows)
    version_hash = write_json(
        output_json,
        rows,
        meta_extra={
            "discovery_method": "ADS REST API with static inventory",
            "api_base": ADS_API_BASE,
            "total_inventory_sites": total,
            "successful_sites": len(rows),
            "failed_sites": len(failed_sites),
            "direct_download_urls": len(rows) - len(landing_page_only),
            "landing_page_fallbacks": len(landing_page_only),
            "source": "JapanFlux2024 (Ueyama et al., 2025, ESSD)",
            "license": "CC BY 4.0",
        },
        snapshot_updated_at=args.snapshot_updated_at,
        snapshot_updated_date=args.snapshot_updated_date,
    )

    print(f"Wrote JapanFlux CSV: {output_csv}", flush=True)
    print(f"Wrote JapanFlux JSON: {output_json}", flush=True)
    print(f"Version: sha256:{version_hash}", flush=True)


if __name__ == "__main__":
    main()
