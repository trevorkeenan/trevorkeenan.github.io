# trevorkeenan.github.io

## FLUXNET explorer snapshot automation

This repo includes a GitHub Actions workflow at `.github/workflows/update-shuttle-snapshot.yml` that refreshes committed FLUXNET explorer metadata artifacts daily (UTC) and on manual dispatch.

- Output files:
  - `assets/shuttle_snapshot.csv`
  - `assets/shuttle_snapshot.json`
  - `assets/icos_direct_fluxnet.csv`
  - `assets/icos_direct_fluxnet.json`
  - `assets/japanflux_direct_snapshot.csv`
  - `assets/japanflux_direct_snapshot.json`
- Source: `fluxnet/shuttle` installed from GitHub during the workflow run
- ICOS direct source: `scripts/refresh_icos_direct_fluxnet.py` discovers candidate FLUXNET files from the official ICOS Carbon Portal SPARQL metadata endpoint, then hydrates the chosen per-site rows from the ICOS object JSON endpoint so the build does not depend on timeout-prone metadata joins
- JapanFlux direct source: `scripts/refresh_japanflux_direct.py` queries the ADS REST API for the published JapanFlux2024 inventory, validates direct ZIP candidates when possible, and otherwise falls back to the ADS dataset landing page
- JSON format: compact browser payload with normalized `snake_case` column names (`{"columns":[...],"rows":[...]}`), keeping key discovery/download fields
- Safety behavior: the workflow only stages/commits the generated snapshot files, and it refuses to overwrite the Shuttle CSV snapshot if `listall()` returns zero rows

This supports a no-backend GitHub Pages setup where the site reads stable snapshot paths directly.

## Source integration

The explorer now merges five effective source layers:

- FLUXNET Shuttle snapshot rows (`assets/shuttle_snapshot.json` / `assets/shuttle_snapshot.csv`)
- ICOS-direct FLUXNET discovery snapshot (`assets/icos_direct_fluxnet.json` / `assets/icos_direct_fluxnet.csv`)
- JapanFlux-direct archive snapshot (`assets/japanflux_direct_snapshot.json` / `assets/japanflux_direct_snapshot.csv`)
- AmeriFlux FLUXNET API availability (`https://amfcdn.lbl.gov/api/v2/data_availability/AmeriFlux/FLUXNET/CCBY4.0`)
- AmeriFlux FLUXNET2015 API availability (`https://amfcdn.lbl.gov/api/v2/data_availability/FLUXNET/FLUXNET2015/CCBY4.0`)

Canonical precedence is:

- Shuttle
- ICOS-direct
- JapanFlux-direct
- AmeriFlux FLUXNET
- FLUXNET2015

Merge behavior:

- If a site exists in Shuttle and AmeriFlux FLUXNET, Shuttle is canonical and the row is labeled `AmeriFlux-shuttle`.
- If a site exists in Shuttle and ICOS-direct, Shuttle is canonical and the ICOS-direct row is suppressed.
- If a site exists in Shuttle or ICOS-direct and JapanFlux-direct, the higher-precedence row remains canonical and the JapanFlux-direct row is suppressed.
- If a site is missing from Shuttle but present in ICOS-direct, the ICOS-direct row is canonical and both AmeriFlux FLUXNET / FLUXNET2015 fallbacks are suppressed.
- If a site is missing from Shuttle and ICOS-direct but present in JapanFlux-direct, the JapanFlux-direct row is canonical and AmeriFlux FLUXNET / FLUXNET2015 fallbacks are suppressed.
- If a site exists only in AmeriFlux FLUXNET, it is shown as `AmeriFlux`.
- `FLUXNET2015` is used only as a fallback for sites missing from both Shuttle and AmeriFlux FLUXNET.
- If a site exists in both AmeriFlux FLUXNET and FLUXNET2015, only the AmeriFlux FLUXNET row is kept when Shuttle and ICOS-direct are both absent.

The ICOS-direct snapshot is discovered from ICOS Carbon Portal metadata rather than HTML scraping or a DOI subset. The ICOS per-row UX remains a direct `licence_accept` link with the existing button text `Accept ICOS license and download`.
JapanFlux-direct rows remain labeled as `JapanFlux` rather than `FLUXNET-Shuttle` so the explorer does not imply the JapanFlux2024 archive came from the FLUXNET Shuttle.

AmeriFlux availability is cached in browser localStorage with separate keys for AmeriFlux FLUXNET and FLUXNET2015 availability refreshes, so the app does not call those endpoints on every page load.

### AmeriFlux download identity and static deployments

- AmeriFlux **availability** is public and is always queried in-browser.
- AmeriFlux **download requests** require user identity fields (`user_id`, `user_email`), so this site does **not** embed personal credentials in shipped JavaScript.
- Hardcoded identity values must never be committed.

For static/public deployments (for example GitHub Pages):

- AmeriFlux rows show `Copy AmeriFlux curl command`.
- FLUXNET2015 rows show `Copy FLUXNET2015 curl command`.
- The copied command contains placeholders (`YOUR_AMERIFLUX_USERNAME`, `YOUR_EMAIL`) and a site-specific payload.
- The command preserves the filename fix by stripping query strings for naming:
  - `clean_url="${url%%\\?*}"`
  - `filename="$(basename "$clean_url")"`
  - `curl -L "$url" -o "$filename"`

For trusted/private runtime deployments:

- AmeriFlux API downloads can be enabled via runtime config (`window.FLUXNET_EXPLORER_CONFIG`) with:
  - `amerifluxTrustedRuntime: true`
  - `amerifluxUserId` (from `AMERIFLUX_USER_ID`)
  - `amerifluxUserEmail` (from `AMERIFLUX_USER_EMAIL`)
- When both sources contain a site, Shuttle download flow remains canonical.

### Bulk download behavior (source-separated)

- `download_all_selected.sh` is the easiest default bulk-download option.
- It delegates to `download_shuttle_selected.sh` and `download_ameriflux_selected.sh` instead of duplicating their logic.
- Keep `download_all_selected.sh`, the source-specific scripts, and the generated selected-sites files in the same directory when you run it.
- Source-specific scripts remain available for debugging and advanced use.

- The direct-link bulk tools apply to Shuttle-backed rows plus validated ICOS-direct / JapanFlux-direct links.
- AmeriFlux API bulk tools apply to both `AmeriFlux` and `FLUXNET2015` rows.
- AmeriFlux API-backed rows are not mixed into Shuttle links files or Shuttle CLI helper outputs.
- JapanFlux rows that only expose an ADS landing page stay in the manifest but are skipped by the direct-link shell script and `shuttle_links.txt`.

Generated bulk artifacts include:

- `download_all_selected.sh`
- `shuttle_selected_sites.txt`
- `shuttle_links.txt`
- `download_shuttle_selected.sh`
- `shuttle_selected_manifest.csv`
- `ameriflux_selected_sites.txt`
- `download_ameriflux_selected.sh`

AmeriFlux API bulk script behavior:

- Iterates selected AmeriFlux API-backed site IDs.
- Reads `ameriflux_selected_sites.txt` as tab-delimited `site_id`, `data_product`, `source_label`.
- POSTs to `https://amfcdn.lbl.gov/api/v1/data_download` per site with `FULLSET` / `CCBY4.0`.
- Uses `data_product: FLUXNET` for `AmeriFlux` rows and `data_product: FLUXNET2015` for `FLUXNET2015` rows.
- Uses `intended_use: "QED Lab FLUXNET Data Explorer"`.
- Parses `data_urls[].url` dynamically and downloads files.
- Uses query-strip filename logic for local naming while keeping the full URL for requests.

## Dev checks

- Unit tests (Node built-in test runner): `node --test tests/shuttle-explorer.test.js`
- JapanFlux refresh unit tests: `python3 -m unittest tests.test_refresh_japanflux_direct`
- AmeriFlux smoke check (counts; optional AR-Bal download URL if credentials are set): `python3 scripts/ameriflux_api_smoke.py`
- Refresh the cached ICOS-direct snapshot locally:
  `python3 scripts/refresh_icos_direct_fluxnet.py --shuttle-csv assets/shuttle_snapshot.csv --output-csv assets/icos_direct_fluxnet.csv --output-json assets/icos_direct_fluxnet.json`
- Refresh the cached JapanFlux-direct snapshot locally:
  `python3 scripts/refresh_japanflux_direct.py --output-csv assets/japanflux_direct_snapshot.csv --output-json assets/japanflux_direct_snapshot.json`
- Validate that the final Shuttle/ICOS explorer inputs still contain the expected regression sites:
  `python3 scripts/validate_icos_direct_fluxnet.py --shuttle-csv assets/shuttle_snapshot.csv --icos-csv assets/icos_direct_fluxnet.csv`
