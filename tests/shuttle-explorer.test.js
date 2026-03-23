const test = require('node:test');
const assert = require('node:assert/strict');
const childProcess = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const hooks = require('../assets/shuttle-explorer.js');

const BASH_PATH = childProcess.execFileSync('bash', ['-lc', 'command -v bash'], { encoding: 'utf8' }).trim();
const SCRIPT_RUNTIME_COMMANDS = ['basename', 'cat', 'mkdir', 'tee', 'uname'];

function resolveCommandPath(command) {
  return childProcess.execFileSync('bash', ['-lc', 'command -v ' + command], { encoding: 'utf8' }).trim();
}

function writeExecutable(filePath, text) {
  fs.writeFileSync(filePath, text, { mode: 0o755 });
}

function buildScriptRuntimeBin(tempDir, options) {
  const opts = options || {};
  const binDir = path.join(tempDir, 'bin');
  fs.mkdirSync(binDir);

  SCRIPT_RUNTIME_COMMANDS.forEach((command) => {
    fs.symlinkSync(resolveCommandPath(command), path.join(binDir, command));
  });

  if (opts.includePython3) {
    fs.symlinkSync(resolveCommandPath('python3'), path.join(binDir, 'python3'));
  }

  writeExecutable(
    path.join(binDir, 'curl'),
    [
      '#!' + BASH_PATH,
      'set -euo pipefail',
      '',
      'if [ "${1:-}" = "-sS" ]; then',
      '  printf \'%s\' \'{"data_urls":[{"url":"https://example.org/mock.zip?download=1"}]}\'',
      '  exit 0',
      'fi',
      '',
      'if [ "${1:-}" = "-L" ]; then',
      '  url="${2:-}"',
      '  shift 2',
      '  if [ "${1:-}" != "-o" ] || [ -z "${2:-}" ]; then',
      '    echo "unexpected curl download args: $*" >&2',
      '    exit 1',
      '  fi',
      '  printf \'downloaded:%s\\n\' "$url" > "$2"',
      '  exit 0',
      'fi',
      '',
      'echo "unexpected curl args: $*" >&2',
      'exit 1',
      ''
    ].join('\n')
  );

  return binDir;
}

function makeCatalogRow(overrides) {
  return Object.assign(
    {
      site_id: 'XX-Test',
      site_name: 'Test Site',
      country: 'US',
      data_hub: 'ICOS',
      network: 'FLX',
      source_network: 'FLX',
      network_display: 'FLX',
      network_tokens: ['FLX'],
      vegetation_type: '',
      first_year: 2010,
      last_year: 2011,
      years: '2010-2011',
      download_link: 'https://example.org/test.zip',
      download_mode: 'direct',
      source_label: 'ICOS',
      source_reason: 'Available directly from the ICOS Carbon Portal FLUXNET archive.',
      source_origin: 'icos_direct'
    },
    overrides || {}
  );
}

function expectedJqGuidancePattern() {
  if (process.platform === 'darwin') {
    return /macOS: brew install jq/;
  }

  if (process.platform === 'win32') {
    return /Windows shells:\s+choco install jq\s+scoop install jq\s+winget install jqlang\.jq/s;
  }

  if (process.platform === 'linux') {
    var distroId = '';
    if (fs.existsSync('/etc/os-release')) {
      var osReleaseText = fs.readFileSync('/etc/os-release', 'utf8');
      var idMatch = osReleaseText.match(/^ID="?([^"\n]+)"?/m);
      if (idMatch) {
        distroId = idMatch[1];
      }
    }

    if (distroId === 'debian' || distroId === 'ubuntu') {
      return /Debian\/Ubuntu: sudo apt-get install jq/;
    }

    if (distroId === 'fedora' || distroId === 'rhel') {
      return /Fedora\/RHEL: sudo dnf install jq/;
    }

    if (distroId === 'arch') {
      return /Arch: sudo pacman -S jq/;
    }
  }

  return /See https:\/\/jqlang\.github\.io\/jq\/download\//;
}

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

test('FLUXNET2015 availability payload uses the same filtering rules', () => {
  const payload = {
    values: [
      { site_id: 'US-Leg', publish_years: [2001, 2002] },
      { site_id: 'US-None', publish_years: [] },
      { site_id: 'CA-Old', publish_years: [1999] }
    ]
  };

  const parsed = hooks.parseAmeriFluxAvailabilityPayload(payload, 'fluxnet2015');
  assert.equal(parsed.totalSites, 3);
  assert.equal(parsed.sitesWithYears, 2);
  assert.deepEqual(
    parsed.sites.map((site) => site.site_id),
    ['CA-Old', 'US-Leg']
  );
  assert.match(parsed.freshnessKey, /^fluxnet2015:/);
});

test('Merge precedence is Shuttle > ICOS > AmeriFlux > FLUXNET2015 with no duplicates', () => {
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

  const icosRows = [
    {
      site_id: 'AR-Bal',
      site_name: 'Arroyo ICOS',
      country: 'AR',
      data_hub: 'ICOS',
      network: 'FLX',
      source_network: 'FLX',
      network_display: 'FLX',
      network_tokens: ['FLX'],
      vegetation_type: '',
      first_year: 2012,
      last_year: 2014,
      years: '2012-2014',
      download_link: 'https://data.icos-cp.eu/licence_accept?ids=%5B%22obj-ar-bal%22%5D&fileName=FLX_AR-Bal_FLUXNET2015_FULLSET_2012-2014_beta-3.zip',
      download_mode: 'direct',
      source_label: 'ICOS',
      source_reason: 'Available directly from the ICOS Carbon Portal FLUXNET archive.',
      source_origin: 'icos_direct',
      object_id: 'obj-ar-bal',
      file_name: 'FLX_AR-Bal_FLUXNET2015_FULLSET_2012-2014_beta-3.zip',
      direct_download_url: 'https://data.icos-cp.eu/licence_accept?ids=%5B%22obj-ar-bal%22%5D&fileName=FLX_AR-Bal_FLUXNET2015_FULLSET_2012-2014_beta-3.zip'
    },
    {
      site_id: 'BE-Bra',
      site_name: 'Brasschaat',
      country: 'BE',
      data_hub: 'ICOS',
      network: 'FLX',
      source_network: 'FLX',
      network_display: 'FLX',
      network_tokens: ['FLX'],
      vegetation_type: '',
      first_year: 1996,
      last_year: 2020,
      years: '1996-2020',
      download_link: 'https://data.icos-cp.eu/licence_accept?ids=%5B%225BCT4nKCoGaYQh77DW6OW7gs%22%5D&fileName=FLX_BE-Bra_FLUXNET2015_FULLSET_1996-2020_beta-3.zip',
      download_mode: 'direct',
      source_label: 'ICOS',
      source_reason: 'Available directly from the ICOS Carbon Portal FLUXNET archive.',
      source_origin: 'icos_direct',
      object_id: '5BCT4nKCoGaYQh77DW6OW7gs',
      file_name: 'FLX_BE-Bra_FLUXNET2015_FULLSET_1996-2020_beta-3.zip',
      direct_download_url: 'https://data.icos-cp.eu/licence_accept?ids=%5B%225BCT4nKCoGaYQh77DW6OW7gs%22%5D&fileName=FLX_BE-Bra_FLUXNET2015_FULLSET_1996-2020_beta-3.zip'
    }
  ];

  const ameriSites = [
    { site_id: 'AR-Bal', publish_years: [2012, 2013], first_year: 2012, last_year: 2013 },
    { site_id: 'BE-Bra', publish_years: [1996, 2020], first_year: 1996, last_year: 2020 },
    { site_id: 'BR-New', publish_years: [2019], first_year: 2019, last_year: 2019 }
  ];

  const fluxnet2015Sites = [
    { site_id: 'AR-Bal', publish_years: [2012], first_year: 2012, last_year: 2012 },
    { site_id: 'BE-Bra', publish_years: [1996, 2020], first_year: 1996, last_year: 2020 },
    { site_id: 'BR-New', publish_years: [2019], first_year: 2019, last_year: 2019 },
    { site_id: 'CL-Old', publish_years: [2005], first_year: 2005, last_year: 2005 }
  ];

  const merged = hooks.mergeCatalogRows(shuttleRows, icosRows, ameriSites, fluxnet2015Sites);
  const overlap = merged.rows.find((row) => row.site_id === 'AR-Bal');
  const icosOnly = merged.rows.find((row) => row.site_id === 'BE-Bra');
  const ameriOnly = merged.rows.find((row) => row.site_id === 'BR-New');
  const fluxnet2015Only = merged.rows.find((row) => row.site_id === 'CL-Old');
  const shuttleAmeriFlux = merged.rows.find((row) => row.site_id === 'US-Var');

  assert.equal(merged.icosDirectTotalSites, 2);
  assert.equal(merged.icosDirectSuppressedByShuttle, 1);
  assert.equal(merged.icosDirectOnlySites, 1);
  assert.equal(merged.amerifluxOverlapSites, 1);
  assert.equal(merged.amerifluxOnlySites, 1);
  assert.equal(merged.fluxnet2015OnlySites, 1);
  assert.equal(merged.rows.filter((row) => row.site_id === 'AR-Bal').length, 1);
  assert.equal(merged.rows.filter((row) => row.site_id === 'BE-Bra').length, 1);
  assert.equal(merged.rows.filter((row) => row.site_id === 'BR-New').length, 1);

  assert.equal(overlap.source_label, 'AmeriFlux-shuttle');
  assert.equal(overlap.download_mode, 'direct');
  assert.equal(overlap.download_link, 'https://data.fluxnet.org/shuttle/ar-bal.zip');
  assert.equal(overlap.country, 'Argentina');
  assert.equal(overlap.network_display, 'AmeriFlux');
  assert.deepEqual(overlap.network_tokens, ['AmeriFlux']);

  assert.equal(ameriOnly.source_label, 'AmeriFlux');
  assert.equal(ameriOnly.download_mode, 'ameriflux_api');
  assert.equal(ameriOnly.data_hub, 'AmeriFlux');
  assert.equal(ameriOnly.api_data_product, 'FLUXNET');
  assert.equal(ameriOnly.length_years, 1);

  assert.equal(icosOnly.source_label, 'ICOS');
  assert.equal(icosOnly.download_mode, 'direct');
  assert.equal(icosOnly.data_hub, 'ICOS');
  assert.equal(icosOnly.download_link.includes('licence_accept'), true);
  assert.equal(icosOnly.object_id, '5BCT4nKCoGaYQh77DW6OW7gs');
  assert.equal(icosOnly.file_name, 'FLX_BE-Bra_FLUXNET2015_FULLSET_1996-2020_beta-3.zip');
  assert.equal(icosOnly.source_priority, 300);
  assert.equal(icosOnly.is_icos, true);
  assert.equal(icosOnly.country, 'Belgium');

  assert.equal(fluxnet2015Only.source_label, 'FLUXNET2015');
  assert.equal(fluxnet2015Only.download_mode, 'ameriflux_api');
  assert.equal(fluxnet2015Only.api_data_product, 'FLUXNET2015');
  assert.equal(fluxnet2015Only.data_hub, 'AmeriFlux');
  assert.equal(fluxnet2015Only.network, 'FLUXNET2015');
  assert.equal(fluxnet2015Only.source_network, 'FLUXNET2015');
  assert.equal(fluxnet2015Only.network_display, 'FLUXNET2015');
  assert.deepEqual(fluxnet2015Only.network_tokens, ['FLUXNET2015']);
  assert.equal(fluxnet2015Only.length_years, 1);

  assert.equal(shuttleAmeriFlux.network_display, 'AmeriFlux');
  assert.deepEqual(shuttleAmeriFlux.network_tokens, ['AmeriFlux']);
});

test('Coverage length helper counts inclusive years and returns null for incomplete ranges', () => {
  assert.equal(hooks.calculateCoverageLength(2001, 2005), 5);
  assert.equal(hooks.calculateCoverageLength(2010, 2010), 1);
  assert.equal(hooks.calculateCoverageLength(2010, null), null);
  assert.equal(hooks.calculateCoverageLength(null, 2010), null);
});

test('Country helpers map ISO-2 codes case-insensitively and preserve full names', () => {
  assert.equal(hooks.countryCodeToName(' ar '), 'Argentina');
  assert.equal(hooks.normalizeCountryName('us'), 'USA');
  assert.equal(hooks.normalizeCountryName('United States'), 'USA');
  assert.equal(hooks.normalizeCountryName('United States of America'), 'USA');
  assert.equal(hooks.normalizeCountryName('Argentina'), 'Argentina');
  assert.equal(hooks.deriveCountry('CN-Du3', ''), 'China');
  assert.equal(hooks.deriveCountry('ZZ-Test', ''), 'ZZ');
});

test('Network helpers normalize selected short codes to display names and dedupe mixed tokens', () => {
  assert.equal(hooks.normalizeNetworkToken('CNF'), 'ChinaFlux');
  assert.equal(hooks.normalizeNetworkToken('euf'), 'EuroFlux');
  assert.equal(hooks.normalizeNetworkToken('JPF'), 'JapanFlux');
  assert.equal(hooks.normalizeNetworkToken('KOF'), 'KoreaFlux');
  assert.equal(hooks.normalizeNetworkToken('AmeriFlux'), 'AmeriFlux');
  assert.equal(hooks.normalizeNetworkToken('FLX'), 'FLX');
  assert.deepEqual(
    hooks.normalizeNetworkTokens('CNF;ChinaFlux;AmeriFlux;KOF'),
    ['ChinaFlux', 'AmeriFlux', 'KoreaFlux']
  );
  assert.equal(
    hooks.normalizeNetworkDisplayValue('CNF;ChinaFlux;EUF'),
    'ChinaFlux;EuroFlux'
  );
});

test('Vegetation helper maps ICOS full-name variants to canonical IGBP codes and preserves codes', () => {
  assert.equal(hooks.normalizeVegetationType('Grasslands'), 'GRA');
  assert.equal(hooks.normalizeVegetationType(' grasslands '), 'GRA');
  assert.equal(hooks.normalizeVegetationType('Evergreen needleleaf forests'), 'ENF');
  assert.equal(hooks.normalizeVegetationType('Urban and Built-up lands'), 'URB');
  assert.equal(hooks.normalizeVegetationType('Permanent wetlands'), 'WET');
  assert.equal(hooks.normalizeVegetationType('GRA'), 'GRA');
  assert.equal(hooks.normalizeVegetationType('DNF'), 'DNF');
});

test('Merged Shuttle rows expose normalized network display names for filter tokens', () => {
  const merged = hooks.mergeCatalogRows([
    {
      site_id: 'CN-Test',
      site_name: 'China Test',
      country: 'CN',
      data_hub: 'TERN',
      network: 'CNF;JPF',
      source_network: '',
      network_display: 'CNF;JPF',
      network_tokens: ['CNF', 'JPF'],
      vegetation_type: '',
      first_year: 2010,
      last_year: 2011,
      years: '2010-2011',
      download_link: 'https://example.org/cn-test.zip',
      download_mode: 'direct',
      _selection_key: 'TERN|CN-Test|1',
      _index: 0,
      source_label: '',
      source_reason: ''
    }
  ], [], []);

  assert.equal(merged.rows[0].network_display, 'ChinaFlux;JapanFlux');
  assert.deepEqual(merged.rows[0].network_tokens, ['ChinaFlux', 'JapanFlux']);
});

test('Merged rows normalize mixed code and full-name vegetation values across sources', () => {
  const merged = hooks.mergeCatalogRows(
    [
      makeCatalogRow({
        site_id: 'US-Code',
        data_hub: 'AmeriFlux',
        network: 'AmeriFlux',
        source_network: 'AMF',
        network_display: 'AmeriFlux',
        network_tokens: ['AmeriFlux'],
        vegetation_type: 'GRA',
        download_link: 'https://example.org/us-code.zip',
        source_label: '',
        source_reason: '',
        source_origin: 'shuttle'
      })
    ],
    [
      makeCatalogRow({
        site_id: 'AT-Full',
        vegetation_type: 'Grasslands',
        download_link: 'https://example.org/at-full.zip'
      }),
      makeCatalogRow({
        site_id: 'BE-Full',
        vegetation_type: 'Evergreen Needleleaf Forests',
        download_link: 'https://example.org/be-full.zip'
      })
    ],
    [],
    []
  );

  assert.equal(merged.rows.find((row) => row.site_id === 'US-Code').vegetation_type, 'GRA');
  assert.equal(merged.rows.find((row) => row.site_id === 'AT-Full').vegetation_type, 'GRA');
  assert.equal(merged.rows.find((row) => row.site_id === 'BE-Full').vegetation_type, 'ENF');
});

test('ICOS-style vegetation full names normalize robustly for case and spacing variants', () => {
  const merged = hooks.mergeCatalogRows(
    [],
    [
      makeCatalogRow({
        site_id: 'AT-Neu',
        vegetation_type: ' grasslands ',
        download_link: 'https://example.org/at-neu.zip'
      }),
      makeCatalogRow({
        site_id: 'SE-Nor',
        vegetation_type: 'Evergreen needleleaf forests',
        download_link: 'https://example.org/se-nor.zip'
      }),
      makeCatalogRow({
        site_id: 'US-Urb',
        vegetation_type: 'Urban and Built-up lands',
        download_link: 'https://example.org/us-urb.zip'
      })
    ],
    [],
    []
  );

  assert.equal(merged.rows.find((row) => row.site_id === 'AT-Neu').vegetation_type, 'GRA');
  assert.equal(merged.rows.find((row) => row.site_id === 'SE-Nor').vegetation_type, 'ENF');
  assert.equal(merged.rows.find((row) => row.site_id === 'US-Urb').vegetation_type, 'URB');
});

test('AmeriFlux download helpers route FLUXNET to v2 and FLUXNET2015 to v1', () => {
  assert.equal(
    hooks.getDownloadEndpointForProduct('FLUXNET'),
    'https://amfcdn.lbl.gov/api/v2/data_download'
  );
  assert.equal(
    hooks.getDownloadEndpointForProduct('FLUXNET2015'),
    'https://amfcdn.lbl.gov/api/v1/data_download'
  );

  const v2Payload = hooks.buildV2DownloadPayload(
    ['AR-Bal'],
    'FULLSET',
    'CCBY4.0',
    { user_id: 'user', user_email: 'user@example.org' },
    'FLUXNET'
  );
  assert.equal(v2Payload.intended_use, 'other_research');
  assert.equal(v2Payload.description.includes('Q.E.D. Lab FLUXNET Data Explorer'), true);
  assert.equal(Object.prototype.hasOwnProperty.call(v2Payload, 'agree_policy'), false);
  assert.equal(Object.prototype.hasOwnProperty.call(v2Payload, 'is_test'), false);

  const v1Payload = hooks.buildV1DownloadPayload(
    ['CL-Old'],
    'FULLSET',
    'CCBY4.0',
    { user_id: 'user', user_email: 'user@example.org' },
    'FLUXNET2015'
  );
  assert.equal(v1Payload.intended_use, 'QED Lab FLUXNET Data Explorer');
  assert.equal(v1Payload.agree_policy, true);
  assert.equal(Object.prototype.hasOwnProperty.call(v1Payload, 'is_test'), false);
});

test('Bulk tools action helper only activates for multi-site selections', () => {
  assert.equal(hooks.shouldEnableBulkToolsActions(0), false);
  assert.equal(hooks.shouldEnableBulkToolsActions(1), false);
  assert.equal(hooks.shouldEnableBulkToolsActions(2), true);
  assert.equal(hooks.formatSelectedSiteCount(1), '1 selected site');
  assert.equal(hooks.formatSelectedSiteCount(3), '3 selected sites');
});

test('Table clipboard export includes visible headers, preserves row order, and normalizes cell text', () => {
  const text = hooks.buildTableClipboardText([
    {
      site_id: 'AR-Bal',
      site_name: 'Balcarce\nBA',
      country: 'Argentina',
      data_hub: 'AmeriFlux',
      vegetation_type: 'Grassland  ',
      years: '2012-2013',
      length_years: 2
    },
    {
      site_id: 'US-Blank',
      site_name: '',
      country: 'USA',
      data_hub: 'Shuttle',
      vegetation_type: null,
      years: '2010-2010',
      length_years: 1
    }
  ]);

  assert.equal(
    text,
    [
      'Site ID\tSite Name\tCountry\tHub\tVeg Type\tYears\tLength',
      'AR-Bal\tBalcarce BA\tArgentina\tAmeriFlux\tGrassland\t2012-2013\t2',
      'US-Blank\t—\tUSA\tShuttle\t—\t2010-2010\t1',
      ''
    ].join('\n')
  );
});

test('AmeriFlux bulk identity helper prefers explicit input values and otherwise falls back to defaults', () => {
  assert.deepEqual(
    hooks.resolveAmeriFluxBulkIdentity('', ''),
    {
      enteredUserId: '',
      enteredUserEmail: '',
      user_id: 'trevorkeenan',
      user_email: 'trevorkeenan@berkeley.edu'
    }
  );
  assert.deepEqual(
    hooks.resolveAmeriFluxBulkIdentity(' custom-user ', ' custom@example.org '),
    {
      enteredUserId: 'custom-user',
      enteredUserEmail: 'custom@example.org',
      user_id: 'custom-user',
      user_email: 'custom@example.org'
    }
  );
});

test('Snapshot updated date helper prefers committed metadata fields and falls back cleanly', () => {
  assert.equal(
    hooks.extractSnapshotUpdatedDate({
      snapshot_updated_date: '2026-03-11',
      snapshot_updated_at: '2026-03-12T08:15:00Z'
    }),
    '2026-03-11'
  );
  assert.equal(
    hooks.extractSnapshotUpdatedDate({
      snapshot_updated_at: '2026-03-12T08:15:00Z'
    }),
    '2026-03-12'
  );
  assert.equal(hooks.extractSnapshotUpdatedDate({}), '');
  assert.equal(hooks.snapshotUpdatedDateDisplayText(''), 'unavailable');
});

test('Attribution text includes the contact sentence and uses the shared snapshot date source', () => {
  assert.match(
    hooks.buildAttributionText('2026-03-11'),
    /Contact TF Keenan \(trevorkeenan@berkeley\.edu\) with any questions/
  );
  assert.match(
    hooks.buildAttributionText('2026-03-11'),
    /Available data is updated as of: 2026-03-11\./
  );
  assert.match(
    hooks.buildAttributionText(''),
    /Available data is updated as of: unavailable\./
  );
});

test('Bulk partition routes overlap rows to Shuttle and AmeriFlux API rows to the AmeriFlux bulk set', () => {
  const selectedRows = [
    { site_id: 'US-Ton', download_mode: 'direct', source_label: '' },
    { site_id: 'AR-Bal', download_mode: 'direct', source_label: 'AmeriFlux-shuttle' },
    { site_id: 'BR-New', download_mode: 'ameriflux_api', source_label: 'AmeriFlux', api_data_product: 'FLUXNET' },
    { site_id: 'CL-Old', download_mode: 'ameriflux_api', source_label: 'FLUXNET2015', api_data_product: 'FLUXNET2015' }
  ];

  const partition = hooks.partitionRowsByBulkSource(selectedRows);
  assert.deepEqual(partition.shuttleRows.map((row) => row.site_id), ['US-Ton', 'AR-Bal']);
  assert.deepEqual(partition.ameriFluxRows.map((row) => row.site_id), ['BR-New', 'CL-Old']);
});

test('Bulk section visibility helper reflects selected source mix', () => {
  const mixed = hooks.summarizeBulkSelection([
    { site_id: 'US-Ton', download_mode: 'direct' },
    { site_id: 'BR-New', download_mode: 'ameriflux_api' }
  ]);
  assert.equal(mixed.showAllSelectedActions, true);
  assert.equal(mixed.showShuttleSection, true);
  assert.equal(mixed.showAmeriFluxSection, true);

  const shuttleOnly = hooks.summarizeBulkSelection([
    { site_id: 'US-Ton', download_mode: 'direct' }
  ]);
  assert.equal(shuttleOnly.showAllSelectedActions, true);
  assert.equal(shuttleOnly.showShuttleSection, true);
  assert.equal(shuttleOnly.showAmeriFluxSection, false);

  const ameriOnly = hooks.summarizeBulkSelection([
    { site_id: 'BR-New', download_mode: 'ameriflux_api' }
  ]);
  assert.equal(ameriOnly.showAllSelectedActions, true);
  assert.equal(ameriOnly.showShuttleSection, false);
  assert.equal(ameriOnly.showAmeriFluxSection, true);

  const noneSelected = hooks.summarizeBulkSelection([]);
  assert.equal(noneSelected.showAllSelectedActions, false);
  assert.equal(noneSelected.showShuttleSection, false);
  assert.equal(noneSelected.showAmeriFluxSection, false);
});

test('Source filter options include Shuttle, AmeriFlux, AmeriFlux-shuttle, and FLUXNET2015', () => {
  const values = hooks.uniqueSourceFilterValues([
    { source_label: '' },
    { source_label: 'AmeriFlux' },
    { source_label: 'AmeriFlux-shuttle' },
    { source_label: 'FLUXNET2015' }
  ]);

  assert.deepEqual(values, ['AmeriFlux', 'AmeriFlux-shuttle', 'FLUXNET2015', 'Shuttle']);
});

test('Vegetation filter values include only canonical codes after mixed-source normalization', () => {
  const merged = hooks.mergeCatalogRows(
    [
      makeCatalogRow({
        site_id: 'US-GRA',
        data_hub: 'AmeriFlux',
        network: 'AmeriFlux',
        source_network: 'AMF',
        network_display: 'AmeriFlux',
        network_tokens: ['AmeriFlux'],
        vegetation_type: 'GRA',
        download_link: 'https://example.org/us-gra.zip',
        source_label: '',
        source_reason: '',
        source_origin: 'shuttle'
      })
    ],
    [
      makeCatalogRow({
        site_id: 'AT-Gra',
        vegetation_type: 'Grasslands',
        download_link: 'https://example.org/at-gra.zip'
      }),
      makeCatalogRow({
        site_id: 'US-Urb',
        vegetation_type: 'Urban and Built-up lands',
        download_link: 'https://example.org/us-urb.zip'
      }),
      makeCatalogRow({
        site_id: 'US-Urb-2',
        vegetation_type: 'urban and built up lands',
        download_link: 'https://example.org/us-urb-2.zip'
      })
    ],
    [],
    []
  );

  assert.deepEqual(hooks.uniqueVegetationFilterValues(merged.rows), ['GRA', 'URB']);
});

test('Coordinate lookup enriches JSON rows from the CSV snapshot without duplicating metadata', () => {
  const csvRows = [
    {
      data_hub: 'AmeriFlux',
      site_id: 'AR-CCg',
      download_link: 'https://example.org/ar-ccg.zip',
      location_lat: '-35.9244',
      location_long: '-61.1855'
    }
  ];
  const jsonRows = [
    {
      data_hub: 'AmeriFlux',
      site_id: 'AR-CCg',
      site_name: 'Carlos Casares grassland',
      download_link: 'https://example.org/ar-ccg.zip'
    },
    {
      data_hub: 'AmeriFlux',
      site_id: 'BR-NoCoord',
      site_name: 'No Coordinates',
      download_link: 'https://example.org/br-nocoord.zip'
    }
  ];

  const lookup = hooks.buildCoordinateLookup(csvRows);
  const enriched = hooks.enrichRowsWithCoordinateLookup(jsonRows, lookup);

  assert.deepEqual(lookup, {
    'AmeriFlux|AR-CCg|https://example.org/ar-ccg.zip': {
      location_lat: -35.9244,
      location_long: -61.1855
    }
  });
  assert.equal(enriched[0].location_lat, -35.9244);
  assert.equal(enriched[0].location_long, -61.1855);
  assert.equal(enriched[1].location_lat, undefined);
  assert.equal(enriched[1].location_long, undefined);
});

test('AmeriFlux site info lookup enriches API availability rows by normalized SITE_ID', () => {
  const rawSiteInfoRows = [
    {
      site_id: 'AR-BAL',
      site_name: 'Balcarce BA',
      country: 'Argentina',
      location_lat: '-37.7596',
      location_long: '-58.3024'
    }
  ];
  const availabilitySites = [
    {
      site_id: 'AR-Bal',
      publish_years: [2012, 2013],
      first_year: 2012,
      last_year: 2013,
      country: 'AR'
    },
    {
      site_id: 'BR-NoMeta',
      publish_years: [2020],
      first_year: 2020,
      last_year: 2020,
      country: 'BR'
    }
  ];

  const lookup = hooks.buildAmeriFluxSiteInfoLookup(rawSiteInfoRows);
  const enriched = hooks.enrichAmeriFluxSitesWithMetadata(availabilitySites, lookup);

  assert.deepEqual(lookup['AR-BAL'], {
    site_id: 'AR-BAL',
    site_name: 'Balcarce BA',
    country: 'Argentina',
    latitude: -37.7596,
    longitude: -58.3024
  });
  assert.equal(enriched[0].site_name, 'Balcarce BA');
  assert.equal(enriched[0].country, 'Argentina');
  assert.equal(enriched[0].latitude, -37.7596);
  assert.equal(enriched[0].longitude, -58.3024);
  assert.equal(enriched[1].site_name, undefined);
  assert.equal(enriched[1].country, 'Brazil');
  assert.equal(enriched[1].latitude, undefined);
  assert.equal(enriched[1].longitude, undefined);
});

test('FLUXNET2015 site info lookup accepts mysitename/lon/lat columns and enriches FLUXNET2015 rows separately', () => {
  const rawSiteInfoRows = [
    {
      mysitename: 'AR-SLu',
      lon: '-66.4598',
      lat: '-33.4648'
    }
  ];
  const availabilitySites = [
    {
      site_id: 'AR-SLu',
      publish_years: [2002, 2003],
      first_year: 2002,
      last_year: 2003,
      country: 'AR'
    },
    {
      site_id: 'ZZ-NoMeta',
      publish_years: [2004],
      first_year: 2004,
      last_year: 2004,
      country: 'ZZ'
    }
  ];

  const lookup = hooks.buildFluxnet2015SiteLookup(rawSiteInfoRows);
  const enriched = hooks.enrichFluxnet2015SitesWithMetadata(availabilitySites, lookup);

  assert.deepEqual(lookup['AR-SLU'], {
    site_id: 'AR-SLU',
    site_name: '',
    country: '',
    latitude: -33.4648,
    longitude: -66.4598
  });
  assert.equal(enriched[0].latitude, -33.4648);
  assert.equal(enriched[0].longitude, -66.4598);
  assert.equal(enriched[0].country, 'Argentina');
  assert.equal(enriched[1].latitude, undefined);
  assert.equal(enriched[1].longitude, undefined);
  assert.equal(enriched[1].country, 'ZZ');
});

test('API-only coordinate coverage summary reports AmeriFlux and FLUXNET2015 counts separately', () => {
  const summary = hooks.summarizeApiOnlyRowCoordinateCoverage([
    { source_label: 'AmeriFlux', site_id: 'AMF-1', latitude: 10, longitude: 20 },
    { source_label: 'AmeriFlux', site_id: 'AMF-2', latitude: null, longitude: null },
    { source_label: 'FLUXNET2015', site_id: 'FLX-1', latitude: 30, longitude: 40 },
    { source_label: 'FLUXNET2015', site_id: 'FLX-2', latitude: null, longitude: null }
  ]);

  assert.deepEqual(summary, {
    amerifluxWithCoordinates: 1,
    amerifluxWithoutCoordinates: 1,
    fluxnet2015WithCoordinates: 1,
    fluxnet2015WithoutCoordinates: 1,
    fluxnet2015MissingSiteIds: ['FLX-2']
  });
});

test('Filename helper strips URL query strings', () => {
  const url = 'https://amfcdn.lbl.gov/path/AMF_AR-Bal_FLUXNET_FULLSET_2012-2013_3-7.zip?=username';
  assert.equal(hooks.stripUrlQueryForFilename(url), 'https://amfcdn.lbl.gov/path/AMF_AR-Bal_FLUXNET_FULLSET_2012-2013_3-7.zip');
  assert.equal(hooks.filenameFromUrl(url), 'AMF_AR-Bal_FLUXNET_FULLSET_2012-2013_3-7.zip');
});

test('AmeriFlux API download returns manual fallback when trusted credentials are unavailable', async () => {
  const source = hooks.createAmeriFluxSource({
    trustedRuntime: false,
    userId: '',
    userEmail: '',
    dataProduct: 'FLUXNET2015',
    sourceLabel: 'FLUXNET2015'
  });

  const result = await source.get_download_urls('AR-Bal', 'FULLSET', 'CCBY4.0');

  assert.equal(result.mode, 'manual');
  assert.equal(result.manual_download_required, true);
  assert.equal(result.site_id, 'AR-Bal');
  assert.equal(result.payload_template.data_product, 'FLUXNET2015');
  assert.equal(result.payload_template.intended_use, 'QED Lab FLUXNET Data Explorer');
  assert.equal(result.payload_template.agree_policy, true);
  assert.equal(source.downloadUrl, 'https://amfcdn.lbl.gov/api/v1/data_download');
  assert.equal(Array.isArray(result.data_urls), true);
  assert.equal(result.data_urls.length, 0);
});

test('AmeriFlux row download stays manual without trusted runtime even with fallback identity override', async () => {
  const source = hooks.createAmeriFluxSource({
    trustedRuntime: false,
    userId: '',
    userEmail: '',
    dataProduct: 'FLUXNET',
    sourceLabel: 'AmeriFlux'
  });
  const identity = hooks.resolveAmeriFluxBulkIdentity('', '');
  const originalFetch = global.fetch;
  let fetchCalled = false;

  global.fetch = async () => {
    fetchCalled = true;
    throw new Error('fetch should not be called for manual AmeriFlux row actions');
  };

  try {
    const result = await source.get_download_urls('AR-Bal', 'FULLSET', 'CCBY4.0', identity);

    assert.equal(result.mode, 'manual');
    assert.equal(result.manual_download_required, true);
    assert.equal(result.payload_template.user_id, 'trevorkeenan');
    assert.equal(result.payload_template.user_email, 'trevorkeenan@berkeley.edu');
    assert.equal(String(result.curl_command || '').includes('YOUR_AMERIFLUX_USERNAME'), false);
    assert.equal(String(result.curl_command || '').includes('YOUR_EMAIL'), false);
    assert.equal(fetchCalled, false);
  } finally {
    global.fetch = originalFetch;
  }
});

test('AmeriFlux row manual content uses custom effective identity override values', async () => {
  const source = hooks.createAmeriFluxSource({
    trustedRuntime: false,
    userId: '',
    userEmail: '',
    dataProduct: 'FLUXNET2015',
    sourceLabel: 'FLUXNET2015'
  });
  const identity = hooks.resolveAmeriFluxBulkIdentity('custom-user', 'custom@example.org');
  const originalFetch = global.fetch;
  let fetchCalled = false;

  global.fetch = async () => {
    fetchCalled = true;
    throw new Error('fetch should not be called for manual AmeriFlux row actions');
  };

  try {
    const result = await source.get_download_urls('AR-Bal', 'FULLSET', 'CCBY4.0', identity);

    assert.equal(result.mode, 'manual');
    assert.equal(result.manual_download_required, true);
    assert.equal(result.payload_template.user_id, 'custom-user');
    assert.equal(result.payload_template.user_email, 'custom@example.org');
    assert.equal(result.payload_template.data_product, 'FLUXNET2015');
    assert.equal(String(result.curl_command || '').includes('YOUR_AMERIFLUX_USERNAME'), false);
    assert.equal(String(result.curl_command || '').includes('YOUR_EMAIL'), false);
    assert.equal(fetchCalled, false);
  } finally {
    global.fetch = originalFetch;
  }
});

test('AmeriFlux curl command generator uses product-specific endpoints and payloads', () => {
  const fluxnetCommand = hooks.buildAmeriFluxCurlCommand('AR-Bal', 'FULLSET', 'CCBY4.0', undefined, 'FLUXNET');
  const fluxnet2015Command = hooks.buildAmeriFluxCurlCommand('AR-Bal', 'FULLSET', 'CCBY4.0', undefined, 'FLUXNET2015');

  assert.match(fluxnetCommand, /https:\/\/amfcdn\.lbl\.gov\/api\/v2\/data_download/);
  assert.match(fluxnetCommand, /"site_ids": \[\s*"AR-Bal"\s*\]/);
  assert.match(fluxnetCommand, /"intended_use": "other_research"/);
  assert.match(fluxnetCommand, /Q\.E\.D\. Lab FLUXNET Data Explorer/);
  assert.equal(fluxnetCommand.includes('"agree_policy"'), false);
  assert.equal(fluxnetCommand.includes('"is_test"'), false);

  assert.match(fluxnet2015Command, /https:\/\/amfcdn\.lbl\.gov\/api\/v1\/data_download/);
  assert.match(fluxnet2015Command, /"description": "Download FLUXNET2015 for AR-Bal"/);
  assert.match(fluxnet2015Command, /"data_product": "FLUXNET2015"/);
  assert.match(fluxnet2015Command, /"intended_use": "QED Lab FLUXNET Data Explorer"/);
  assert.equal(fluxnet2015Command.includes('"is_test"'), false);
  assert.equal(fluxnet2015Command.includes('clean_url="${url%%\\?*}"'), true);
  assert.equal(fluxnet2015Command.includes('filename="$(basename "$clean_url")"'), true);
  assert.equal(fluxnet2015Command.includes('curl -L "$url" -o "$filename"'), true);
});

test('AmeriFlux curl command generator uses effective identity override when provided', () => {
  const fallbackCommand = hooks.buildAmeriFluxCurlCommand(
    'AR-Bal',
    'FULLSET',
    'CCBY4.0',
    undefined,
    'FLUXNET',
    hooks.resolveAmeriFluxBulkIdentity('', '')
  );
  const customCommand = hooks.buildAmeriFluxCurlCommand(
    'AR-Bal',
    'FULLSET',
    'CCBY4.0',
    undefined,
    'FLUXNET2015',
    hooks.resolveAmeriFluxBulkIdentity('custom-user', 'custom@example.org')
  );

  assert.equal(fallbackCommand.includes('YOUR_AMERIFLUX_USERNAME'), false);
  assert.equal(fallbackCommand.includes('YOUR_EMAIL'), false);
  assert.match(fallbackCommand, /"user_id": "trevorkeenan"/);
  assert.match(fallbackCommand, /"user_email": "trevorkeenan@berkeley\.edu"/);

  assert.equal(customCommand.includes('YOUR_AMERIFLUX_USERNAME'), false);
  assert.equal(customCommand.includes('YOUR_EMAIL'), false);
  assert.match(customCommand, /"user_id": "custom-user"/);
  assert.match(customCommand, /"user_email": "custom@example\.org"/);
});

test('AmeriFlux selected-sites export includes source label and data product', () => {
  const text = hooks.buildAmeriFluxSelectedSitesText([
    { site_id: 'AR-Bal', data_product: 'FLUXNET', source_label: 'AmeriFlux' },
    { site_id: 'CL-Old', data_product: 'FLUXNET2015', source_label: 'FLUXNET2015' }
  ]);

  assert.match(text, /^# site_id\tdata_product\tsource_label/m);
  assert.match(text, /^AR-Bal\tFLUXNET\tAmeriFlux$/m);
  assert.match(text, /^CL-Old\tFLUXNET2015\tFLUXNET2015$/m);
});

test('AmeriFlux bulk script generator supports mixed FLUXNET and FLUXNET2015 products and filename cleanup', () => {
  const script = hooks.buildAmeriFluxBulkScriptText([
    { site_id: 'AR-Bal', data_product: 'FLUXNET', source_label: 'AmeriFlux' },
    { site_id: 'CL-Old', data_product: 'FLUXNET2015', source_label: 'FLUXNET2015' }
  ], {
    defaultUserId: 'custom-user',
    defaultUserEmail: 'custom@example.org'
  });

  assert.equal(script.includes('# site_id\tdata_product\tsource_label'), true);
  assert.equal(script.includes('AR-Bal\tFLUXNET\tAmeriFlux'), true);
  assert.equal(script.includes('CL-Old\tFLUXNET2015\tFLUXNET2015'), true);
  assert.equal(script.includes('USER_ID="${AMERIFLUX_USER_ID:-custom-user}"'), true);
  assert.equal(script.includes('USER_EMAIL="${AMERIFLUX_USER_EMAIL:-custom@example.org}"'), true);
  assert.equal(script.includes('V2_DOWNLOAD_URL="${AMERIFLUX_V2_DOWNLOAD_URL:-https://amfcdn.lbl.gov/api/v2/data_download}"'), true);
  assert.equal(script.includes('V1_DOWNLOAD_URL="${AMERIFLUX_V1_DOWNLOAD_URL:-https://amfcdn.lbl.gov/api/v1/data_download}"'), true);
  assert.equal(script.includes('extract_urls() {'), true);
  assert.equal(script.includes("printf '%s' \"$1\" | jq -r '.data_urls[]?.url // empty'"), true);
  assert.equal(script.includes("printf '%s' \"$1\" | python3 -c '"), true);
  assert.equal(script.includes('if ! command -v jq >/dev/null 2>&1 && ! command -v python3 >/dev/null 2>&1; then'), true);
  assert.equal(script.includes('This script requires jq or python3 to parse the AmeriFlux API response.'), true);
  assert.equal(script.includes('macOS: brew install jq'), true);
  assert.equal(script.includes('Debian/Ubuntu: sudo apt-get install jq'), true);
  assert.equal(script.includes('Fedora/RHEL: sudo dnf install jq'), true);
  assert.equal(script.includes('Arch: sudo pacman -S jq'), true);
  assert.equal(script.includes('Windows shells:'), true);
  assert.equal(script.includes('choco install jq'), true);
  assert.equal(script.includes('scoop install jq'), true);
  assert.equal(script.includes('winget install jqlang.jq'), true);
  assert.equal(script.includes('See https://jqlang.github.io/jq/download/'), true);
  assert.equal(script.includes('if [ "$DATA_PRODUCT" = "FLUXNET2015" ]; then'), true);
  assert.equal(script.includes('\\"data_product\\": \\"${DATA_PRODUCT}\\"'), true);
  assert.equal(script.includes('\\"data_variant\\": \\"FULLSET\\"'), true);
  assert.equal(script.includes('\\"data_policy\\": \\"CCBY4.0\\"'), true);
  assert.equal(script.includes('\\"intended_use\\": \\"other_research\\"'), true);
  assert.equal(script.includes('\\"intended_use\\": \\"QED Lab FLUXNET Data Explorer\\"'), true);
  assert.equal(script.includes('\\"is_test\\": false'), false);
  assert.equal(script.includes('while IFS=$\'\\t\' read -r SITE_ID DATA_PRODUCT SOURCE_LABEL; do'), true);
  assert.equal(script.includes('URLS=$(extract_urls "$RESPONSE" 2>/dev/null || true)'), true);
  assert.equal(script.includes('clean_url="${url%%\\?*}"'), true);
  assert.equal(script.includes('filename="$(basename "$clean_url")"'), true);
  assert.equal(script.includes('jq is required but was not found in PATH.'), false);
});

test('AmeriFlux bulk script generator uses internal fallback contact values when no input is provided', () => {
  const script = hooks.buildAmeriFluxBulkScriptText([
    { site_id: 'AR-Bal', data_product: 'FLUXNET', source_label: 'AmeriFlux' }
  ]);

  assert.equal(script.includes('USER_ID="${AMERIFLUX_USER_ID:-trevorkeenan}"'), true);
  assert.equal(script.includes('USER_EMAIL="${AMERIFLUX_USER_EMAIL:-trevorkeenan@berkeley.edu}"'), true);
});

test('Generated AmeriFlux bulk script falls back to python3 when jq is unavailable', () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ameriflux-bulk-script-'));
  const scriptPath = path.join(tempDir, 'download_ameriflux_selected.sh');
  const outDir = path.join(tempDir, 'downloads');
  const sitesFile = path.join(tempDir, 'ameriflux_selected_sites.txt');
  const logFile = path.join(tempDir, 'ameriflux_bulk_download.log');
  const binDir = buildScriptRuntimeBin(tempDir, { includePython3: true });

  writeExecutable(
    scriptPath,
    hooks.buildAmeriFluxBulkScriptText([
      { site_id: 'AR-Bal', data_product: 'FLUXNET', source_label: 'AmeriFlux' }
    ])
  );

  childProcess.execFileSync(BASH_PATH, [scriptPath, outDir, sitesFile, logFile], {
    encoding: 'utf8',
    env: {
      ...process.env,
      PATH: binDir
    }
  });

  assert.equal(fs.existsSync(path.join(outDir, 'mock.zip')), true);
  assert.match(fs.readFileSync(path.join(outDir, 'mock.zip'), 'utf8'), /downloaded:https:\/\/example\.org\/mock\.zip\?download=1/);
  assert.match(fs.readFileSync(logFile, 'utf8'), /Downloading mock\.zip \(AR-Bal, FLUXNET\)/);
  assert.match(fs.readFileSync(sitesFile, 'utf8'), /^# site_id\tdata_product\tsource_label/m);
});

test('Generated AmeriFlux bulk script exits with jq install guidance when neither jq nor python3 is available', () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ameriflux-bulk-script-'));
  const scriptPath = path.join(tempDir, 'download_ameriflux_selected.sh');
  const outDir = path.join(tempDir, 'downloads');
  const sitesFile = path.join(tempDir, 'ameriflux_selected_sites.txt');
  const logFile = path.join(tempDir, 'ameriflux_bulk_download.log');
  const binDir = buildScriptRuntimeBin(tempDir, { includePython3: false });

  writeExecutable(
    scriptPath,
    hooks.buildAmeriFluxBulkScriptText([
      { site_id: 'AR-Bal', data_product: 'FLUXNET', source_label: 'AmeriFlux' }
    ])
  );

  const result = childProcess.spawnSync(BASH_PATH, [scriptPath, outDir, sitesFile, logFile], {
    encoding: 'utf8',
    env: {
      ...process.env,
      PATH: binDir
    }
  });

  assert.equal(result.status, 1);
  assert.match(result.stderr, /This script requires jq or python3 to parse the AmeriFlux API response\./);
  assert.match(result.stderr, expectedJqGuidancePattern());
  assert.equal(fs.existsSync(sitesFile), false);
});

test('Download-all wrapper script delegates to both child scripts when both source partitions exist', () => {
  const script = hooks.buildDownloadAllSelectedScriptText({
    includeShuttle: true,
    includeAmeriFlux: true
  });

  assert.equal(script.includes('# Shuttle is preferred for overlap sites (AmeriFlux-shuttle).'), true);
  assert.equal(script.includes('# AmeriFlux API-backed sites (AmeriFlux and FLUXNET2015) are downloaded via the AmeriFlux API.'), true);
  assert.equal(script.includes('if [ -f "./download_shuttle_selected.sh" ]; then'), true);
  assert.equal(script.includes('bash "./download_shuttle_selected.sh" || {'), true);
  assert.equal(script.includes('if [ -f "./download_ameriflux_selected.sh" ]; then'), true);
  assert.equal(script.includes('bash "./download_ameriflux_selected.sh" || {'), true);
  assert.equal(script.includes('echo "Bulk download complete."'), true);
});

test('Download-all wrapper script can be generated for Shuttle-only selections', () => {
  const script = hooks.buildDownloadAllSelectedScriptText({
    includeShuttle: true,
    includeAmeriFlux: false
  });

  assert.equal(script.includes('bash "./download_shuttle_selected.sh" || {'), true);
  assert.equal(script.includes('echo "No AmeriFlux API-backed selected sites to download."'), true);
  assert.equal(script.includes('bash "./download_ameriflux_selected.sh" || {'), false);
});

test('Download-all wrapper script can be generated for AmeriFlux API-only selections', () => {
  const script = hooks.buildDownloadAllSelectedScriptText({
    includeShuttle: false,
    includeAmeriFlux: true
  });

  assert.equal(script.includes('echo "No Shuttle-backed selected sites to download."'), true);
  assert.equal(script.includes('bash "./download_shuttle_selected.sh" || {'), false);
  assert.equal(script.includes('bash "./download_ameriflux_selected.sh" || {'), true);
});

test('Download-all bundle helper returns the wrapper and both child scripts', () => {
  const files = hooks.buildDownloadAllSelectedFileBundle({
    wrapperText: 'wrapper-script',
    ameriFluxText: 'ameriflux-script',
    shuttleText: 'shuttle-script'
  });

  assert.deepEqual(
    files.map((file) => file.filename),
    [
      'download_all_selected.sh',
      'download_ameriflux_selected.sh',
      'download_shuttle_selected.sh'
    ]
  );
  assert.deepEqual(
    files.map((file) => file.mimeType),
    [
      'text/x-shellscript;charset=utf-8',
      'text/x-shellscript;charset=utf-8',
      'text/x-shellscript;charset=utf-8'
    ]
  );
  assert.equal(files[0].text, 'wrapper-script');
  assert.equal(files[1].text, 'ameriflux-script');
  assert.equal(files[2].text, 'shuttle-script');
});

test('Browser-facing explorer markup does not include hardcoded AmeriFlux identity attributes', () => {
  const explorerJs = fs.readFileSync(path.join(__dirname, '..', 'assets', 'shuttle-explorer.js'), 'utf8');
  const explorerHtml = fs.readFileSync(path.join(__dirname, '..', 'fluxnet-explorer.html'), 'utf8');
  const dataLandingHtml = fs.readFileSync(path.join(__dirname, '..', 'data.html'), 'utf8');

  assert.equal(explorerJs.includes('data-ameriflux-user-id='), false);
  assert.equal(explorerJs.includes('data-ameriflux-user-email='), false);
  assert.equal(explorerHtml.includes('trevorkeenan@berkeley.edu'), false);
  assert.equal(explorerHtml.includes('data-ameriflux-user-id='), false);
  assert.equal(explorerHtml.includes('data-ameriflux-user-email='), false);
  assert.equal(dataLandingHtml.includes('trevorkeenan@berkeley.edu'), false);
  assert.equal(explorerJs.includes('value="trevorkeenan"'), false);
  assert.equal(explorerJs.includes('value="trevorkeenan@berkeley.edu"'), false);
});

test('Vegetation filter markup includes an IGBP info tooltip and external reference link', () => {
  const explorerJs = fs.readFileSync(path.join(__dirname, '..', 'assets', 'shuttle-explorer.js'), 'utf8');

  assert.equal(explorerJs.includes('data-role=\\"vegetation-info-toggle\\"'), true);
  assert.equal(explorerJs.includes('aria-label=\\"About IGBP vegetation codes\\"'), true);
  assert.equal(
    explorerJs.includes(
      'Vegetation codes follow IGBP classifications as outlined <a href=\\"https://fluxnet.org/data/badm-data-templates/igbp-classification/\\" target=\\"_blank\\" rel=\\"noopener noreferrer\\">here</a>'
    ),
    true
  );
});

test('Bulk tools layout keeps Shuttle and AmeriFlux action buttons in the intended order', () => {
  const explorerJs = fs.readFileSync(path.join(__dirname, '..', 'assets', 'shuttle-explorer.js'), 'utf8');
  const shuttleRowStart = explorerJs.indexOf('data-role=\\"show-cli-command\\">Show Shuttle CLI command</button>"');
  const shuttleCopyCommand = explorerJs.indexOf('data-role=\\"copy-command\\">Copy Shuttle CLI command</button>"');
  const shuttleCopyLinks = explorerJs.indexOf('data-role=\\"copy-links\\">Copy Shuttle links</button>"');
  const ameriDownloadScript = explorerJs.indexOf('data-role=\\"download-ameriflux-script\\">Download download_ameriflux_selected.sh</button>"');
  const ameriSecondRowStart = explorerJs.indexOf('"    <div class=\\"shuttle-explorer__bulk-actions\\">",', ameriDownloadScript);
  const ameriCopyScript = explorerJs.indexOf('data-role=\\"copy-ameriflux-script\\">Copy AmeriFlux API shell script</button>"');
  const ameriSitesFile = explorerJs.indexOf('data-role=\\"download-ameriflux-sites-file\\">Download ameriflux_selected_sites.txt</button>"');

  assert.equal(shuttleRowStart > -1, true);
  assert.equal(shuttleCopyCommand > shuttleRowStart, true);
  assert.equal(shuttleCopyLinks > shuttleCopyCommand, true);

  assert.equal(ameriDownloadScript > -1, true);
  assert.equal(ameriSecondRowStart > ameriDownloadScript, true);
  assert.equal(ameriCopyScript > ameriSecondRowStart, true);
  assert.equal(ameriSitesFile > ameriCopyScript, true);
});

test('Explorer page and runtime do not hardcode stale last-updated dates', () => {
  const explorerJs = fs.readFileSync(path.join(__dirname, '..', 'assets', 'shuttle-explorer.js'), 'utf8');
  const explorerHtml = fs.readFileSync(path.join(__dirname, '..', 'fluxnet-explorer.html'), 'utf8');

  assert.equal(/last updated:\s*202\d-\d{2}-\d{2}/.test(explorerJs), false);
  assert.equal(/last updated:\s*202\d-\d{2}-\d{2}/.test(explorerHtml), false);
  assert.equal(/Available data is updated as of:\s*202\d-\d{2}-\d{2}/.test(explorerJs), false);
});
