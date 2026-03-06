const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const hooks = require('../assets/shuttle-explorer.js');

test('AmeriFlux availability parser filters entries with empty publish_years', () => {
  const payload = {
    values: [
      { site_id: 'AR-Bal', publish_years: [2012, 2013] },
      { site_id: 'US-Empty', publish_years: [] },
      { site_id: 'CA-Mix', publish_years: ['2010', '2011', '2010', null] }
    ]
  };

  const parsed = hooks.parseAmeriFluxAvailabilityPayload(payload);
  assert.equal(parsed.totalSites, 3);
  assert.equal(parsed.sitesWithYears, 2);
  assert.deepEqual(
    parsed.sites.map((site) => site.site_id),
    ['AR-Bal', 'CA-Mix']
  );
  assert.deepEqual(parsed.sites[1].publish_years, [2010, 2011]);
});

test('Merge precedence keeps Shuttle download rows canonical on overlap', () => {
  const shuttleRows = [
    {
      site_id: 'AR-Bal',
      site_name: 'Arroyo',
      country: 'AR',
      data_hub: 'AmeriFlux',
      network: 'AmeriFlux;Fluxnet-Canada',
      source_network: 'AmeriFlux',
      network_display: 'AmeriFlux;Fluxnet-Canada',
      network_tokens: ['AmeriFlux', 'Fluxnet-Canada'],
      vegetation_type: '',
      first_year: 2012,
      last_year: 2013,
      years: '2012-2013',
      download_link: 'https://data.fluxnet.org/shuttle/ar-bal.zip',
      download_mode: 'direct',
      _selection_key: 'shuttle|AR-Bal|1',
      _index: 0,
      source_label: '',
      source_reason: ''
    },
    {
      site_id: 'US-Var',
      site_name: 'Variant',
      country: 'US',
      data_hub: 'AmeriFlux',
      network: 'AmeriFlux;Phenocam',
      source_network: 'AmeriFlux',
      network_display: 'AmeriFlux;Phenocam',
      network_tokens: ['AmeriFlux', 'Phenocam'],
      vegetation_type: '',
      first_year: 2014,
      last_year: 2015,
      years: '2014-2015',
      download_link: 'https://data.fluxnet.org/shuttle/us-var.zip',
      download_mode: 'direct',
      _selection_key: 'shuttle|US-Var|1',
      _index: 1,
      source_label: '',
      source_reason: ''
    }
  ];

  const ameriSites = [
    { site_id: 'AR-Bal', publish_years: [2012, 2013], first_year: 2012, last_year: 2013 },
    { site_id: 'BR-New', publish_years: [2019], first_year: 2019, last_year: 2019 }
  ];

  const merged = hooks.mergeShuttleAndAmeriFluxRows(shuttleRows, ameriSites);
  const overlap = merged.rows.find((row) => row.site_id === 'AR-Bal');
  const ameriOnly = merged.rows.find((row) => row.site_id === 'BR-New');
  const shuttleAmeriFlux = merged.rows.find((row) => row.site_id === 'US-Var');

  assert.equal(merged.amerifluxOverlapSites, 1);
  assert.equal(merged.amerifluxOnlySites, 1);

  assert.equal(overlap.source_label, 'AmeriFlux-shuttle');
  assert.equal(overlap.download_mode, 'direct');
  assert.equal(overlap.download_link, 'https://data.fluxnet.org/shuttle/ar-bal.zip');
  assert.equal(overlap.network_display, 'AmeriFlux');
  assert.deepEqual(overlap.network_tokens, ['AmeriFlux']);

  assert.equal(ameriOnly.source_label, 'AmeriFlux');
  assert.equal(ameriOnly.download_mode, 'ameriflux_api');
  assert.equal(ameriOnly.data_hub, 'AmeriFlux');
  assert.equal(shuttleAmeriFlux.network_display, 'AmeriFlux');
  assert.deepEqual(shuttleAmeriFlux.network_tokens, ['AmeriFlux']);
});

test('Bulk partition routes overlap rows to Shuttle and AmeriFlux-only rows to AmeriFlux bulk set', () => {
  const selectedRows = [
    { site_id: 'US-Ton', download_mode: 'direct', source_label: '' },
    { site_id: 'AR-Bal', download_mode: 'direct', source_label: 'AmeriFlux-shuttle' },
    { site_id: 'BR-New', download_mode: 'ameriflux_api', source_label: 'AmeriFlux' }
  ];

  const partition = hooks.partitionRowsByBulkSource(selectedRows);
  assert.deepEqual(partition.shuttleRows.map((row) => row.site_id), ['US-Ton', 'AR-Bal']);
  assert.deepEqual(partition.ameriFluxRows.map((row) => row.site_id), ['BR-New']);
});

test('Bulk section visibility helper reflects selected source mix', () => {
  const mixed = hooks.summarizeBulkSelection([
    { site_id: 'US-Ton', download_mode: 'direct' },
    { site_id: 'BR-New', download_mode: 'ameriflux_api' }
  ]);
  assert.equal(mixed.showShuttleSection, true);
  assert.equal(mixed.showAmeriFluxSection, true);

  const shuttleOnly = hooks.summarizeBulkSelection([
    { site_id: 'US-Ton', download_mode: 'direct' }
  ]);
  assert.equal(shuttleOnly.showShuttleSection, true);
  assert.equal(shuttleOnly.showAmeriFluxSection, false);

  const ameriOnly = hooks.summarizeBulkSelection([
    { site_id: 'BR-New', download_mode: 'ameriflux_api' }
  ]);
  assert.equal(ameriOnly.showShuttleSection, false);
  assert.equal(ameriOnly.showAmeriFluxSection, true);
});

test('Filename helper strips URL query strings', () => {
  const url = 'https://amfcdn.lbl.gov/path/AMF_AR-Bal_FLUXNET_FULLSET_2012-2013_3-7.zip?=username';
  assert.equal(hooks.stripUrlQueryForFilename(url), 'https://amfcdn.lbl.gov/path/AMF_AR-Bal_FLUXNET_FULLSET_2012-2013_3-7.zip');
  assert.equal(hooks.filenameFromUrl(url), 'AMF_AR-Bal_FLUXNET_FULLSET_2012-2013_3-7.zip');
});

test('AmeriFlux-only download returns manual fallback when trusted credentials are unavailable', async () => {
  const source = hooks.createAmeriFluxSource({
    trustedRuntime: false,
    userId: '',
    userEmail: ''
  });

  const result = await source.get_download_urls('AR-Bal', 'FULLSET', 'CCBY4.0');

  assert.equal(result.mode, 'manual');
  assert.equal(result.manual_download_required, true);
  assert.equal(result.site_id, 'AR-Bal');
  assert.equal(Array.isArray(result.data_urls), true);
  assert.equal(result.data_urls.length, 0);
});

test('AmeriFlux curl command generator includes site ID and cleaned filename logic', () => {
  const command = hooks.buildAmeriFluxCurlCommand('AR-Bal', 'FULLSET', 'CCBY4.0');

  assert.match(command, /"site_ids": \[\s*"AR-Bal"\s*\]/);
  assert.match(command, /"description": "Download FLUXNET for AR-Bal"/);
  assert.match(command, /"intended_use": "QED Lab FLUXNET Data Explorer"/);
  assert.equal(command.includes('clean_url="${url%%\\?*}"'), true);
  assert.equal(command.includes('filename="$(basename "$clean_url")"'), true);
  assert.equal(command.includes('curl -L "$url" -o "$filename"'), true);
});

test('AmeriFlux bulk script generator includes required payload fields and filename cleanup', () => {
  const script = hooks.buildAmeriFluxBulkScriptText(['AR-Bal', 'BR-New']);

  assert.equal(script.includes('\\"data_product\\": \\"FLUXNET\\"'), true);
  assert.equal(script.includes('\\"data_variant\\": \\"FULLSET\\"'), true);
  assert.equal(script.includes('\\"data_policy\\": \\"CCBY4.0\\"'), true);
  assert.equal(script.includes('\\"intended_use\\": \\"QED Lab FLUXNET Data Explorer\\"'), true);
  assert.equal(script.includes('clean_url="${url%%\\?*}"'), true);
  assert.equal(script.includes('filename="$(basename "$clean_url")"'), true);
  assert.equal(script.includes('while IFS= read -r SITE_ID; do'), true);
});

test('Browser-facing explorer files do not include hardcoded AmeriFlux identity', () => {
  const explorerJs = fs.readFileSync(path.join(__dirname, '..', 'assets', 'shuttle-explorer.js'), 'utf8');
  const dataHtml = fs.readFileSync(path.join(__dirname, '..', 'data.html'), 'utf8');

  assert.equal(explorerJs.includes('trevorkeenan@berkeley.edu'), false);
  assert.equal(dataHtml.includes('trevorkeenan@berkeley.edu'), false);
  assert.equal(dataHtml.includes('data-ameriflux-user-id='), false);
  assert.equal(dataHtml.includes('data-ameriflux-user-email='), false);
});
