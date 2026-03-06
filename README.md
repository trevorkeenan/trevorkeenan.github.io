# trevorkeenan.github.io

## FLUXNET Shuttle snapshot automation

This repo includes a GitHub Actions workflow at `.github/workflows/update-shuttle-snapshot.yml` that refreshes committed FLUXNET Shuttle metadata artifacts daily (UTC) and on manual dispatch.

- Output files: `assets/shuttle_snapshot.csv` and `assets/shuttle_snapshot.json`
- Source: `fluxnet/shuttle` installed from GitHub during the workflow run
- JSON format: compact browser payload with normalized `snake_case` column names (`{"columns":[...],"rows":[...]}`), keeping key discovery/download fields
- Safety behavior: the workflow only stages/commits the generated snapshot files, and it refuses to overwrite the CSV snapshot if `listall()` returns zero rows

This supports a no-backend GitHub Pages setup where the site reads stable snapshot paths directly.

## AmeriFlux source integration

The explorer now merges two sources:

- FLUXNET Shuttle snapshot rows (`assets/shuttle_snapshot.json` / `assets/shuttle_snapshot.csv`)
- AmeriFlux FLUXNET API availability (`https://amfcdn.lbl.gov/api/v2/data_availability/AmeriFlux/FLUXNET/CCBY4.0`)

Merge behavior:

- If a site exists in both sources, Shuttle is canonical and the row is labeled `AmeriFlux-shuttle`.
- If a site exists only in the AmeriFlux API (with non-empty `publish_years`), it is shown as `AmeriFlux`.

AmeriFlux availability is cached in browser localStorage (`shuttle-explorer:ameriflux-availability:v1`) to avoid calling the availability endpoint on every page load.

### AmeriFlux download identity and static deployments

- AmeriFlux **availability** is public and is always queried in-browser.
- AmeriFlux **download requests** require user identity fields (`user_id`, `user_email`), so this site does **not** embed personal credentials in shipped JavaScript.
- Hardcoded identity values must never be committed.

For static/public deployments (for example GitHub Pages):

- AmeriFlux-only rows show `Copy AmeriFlux curl command`.
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

- Shuttle bulk tools apply only to Shuttle-backed rows (including `AmeriFlux-shuttle` overlap rows).
- AmeriFlux-only rows are handled by a separate AmeriFlux bulk shell-script workflow.
- AmeriFlux rows are not mixed into Shuttle links files or Shuttle CLI helper outputs.

Generated bulk artifacts are source-specific:

- `shuttle_selected_sites.txt`
- `shuttle_links.txt`
- `download_shuttle_selected.sh`
- `shuttle_selected_manifest.csv`
- `ameriflux_selected_sites.txt`
- `download_ameriflux_selected.sh`

AmeriFlux bulk script behavior:

- Iterates selected AmeriFlux-only site IDs.
- POSTs to `https://amfcdn.lbl.gov/api/v1/data_download` per site with `FLUXNET` / `FULLSET` / `CCBY4.0`.
- Uses `intended_use: "QED Lab FLUXNET Data Explorer"`.
- Parses `data_urls[].url` dynamically and downloads files.
- Uses query-strip filename logic for local naming while keeping the full URL for requests.

## Dev checks

- Unit tests (Node built-in test runner): `node --test tests/shuttle-explorer.test.js`
- AmeriFlux smoke check (counts; optional AR-Bal download URL if credentials are set): `python3 scripts/ameriflux_api_smoke.py`
