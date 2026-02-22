# trevorkeenan.github.io

## FLUXNET Shuttle snapshot automation

This repo includes a GitHub Actions workflow at `.github/workflows/update-shuttle-snapshot.yml` that refreshes a committed FLUXNET Shuttle metadata snapshot daily (UTC) and on manual dispatch.

- Output file: `assets/shuttle_snapshot.csv`
- Source: `fluxnet/shuttle` installed from GitHub during the workflow run
- Safety behavior: the workflow only stages/commits `assets/shuttle_snapshot.csv`, and it refuses to overwrite the snapshot if `listall()` returns zero rows

This supports a no-backend GitHub Pages setup where the site reads a stable snapshot path directly.
