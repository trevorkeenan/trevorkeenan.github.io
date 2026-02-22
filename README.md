# trevorkeenan.github.io

## FLUXNET Shuttle snapshot automation

This repo includes a GitHub Actions workflow at `.github/workflows/update-shuttle-snapshot.yml` that refreshes committed FLUXNET Shuttle metadata artifacts daily (UTC) and on manual dispatch.

- Output files: `assets/shuttle_snapshot.csv` and `assets/shuttle_snapshot.json`
- Source: `fluxnet/shuttle` installed from GitHub during the workflow run
- JSON format: compact browser payload with normalized `snake_case` column names (`{"columns":[...],"rows":[...]}`), keeping key discovery/download fields
- Safety behavior: the workflow only stages/commits the generated snapshot files, and it refuses to overwrite the CSV snapshot if `listall()` returns zero rows

This supports a no-backend GitHub Pages setup where the site reads stable snapshot paths directly.
