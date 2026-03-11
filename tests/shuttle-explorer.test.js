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

test('Merge precedence is Shuttle > AmeriFlux > FLUXNET2015 with no duplicates', () => {
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

  const fluxnet2015Sites = [
    { site_id: 'AR-Bal', publish_years: [2012], first_year: 2012, last_year: 2012 },
    { site_id: 'BR-New', publish_years: [2019], first_year: 2019, last_year: 2019 },
    { site_id: 'CL-Old', publish_years: [2005], first_year: 2005, last_year: 2005 }
  ];

  const merged = hooks.mergeCatalogRows(shuttleRows, ameriSites, fluxnet2015Sites);
  const overlap = merged.rows.find((row) => row.site_id === 'AR-Bal');
  const ameriOnly = merged.rows.find((row) => row.site_id === 'BR-New');
  const fluxnet2015Only = merged.rows.find((row) => row.site_id === 'CL-Old');
  const shuttleAmeriFlux = merged.rows.find((row) => row.site_id === 'US-Var');

  assert.equal(merged.amerifluxOverlapSites, 1);
  assert.equal(merged.amerifluxOnlySites, 1);
  assert.equal(merged.fluxnet2015OnlySites, 1);
  assert.equal(merged.rows.filter((row) => row.site_id === 'AR-Bal').length, 1);
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

test('Attribution text includes the contact sentence', () => {
  assert.match(
    hooks.buildAttributionText(),
    /Contact TF Keenan \(trevorkeenan@berkeley\.edu\) with any questions/
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
  ]);

  assert.equal(script.includes('# site_id\tdata_product\tsource_label'), true);
  assert.equal(script.includes('AR-Bal\tFLUXNET\tAmeriFlux'), true);
  assert.equal(script.includes('CL-Old\tFLUXNET2015\tFLUXNET2015'), true);
  assert.equal(script.includes('V2_DOWNLOAD_URL="${AMERIFLUX_V2_DOWNLOAD_URL:-https://amfcdn.lbl.gov/api/v2/data_download}"'), true);
  assert.equal(script.includes('V1_DOWNLOAD_URL="${AMERIFLUX_V1_DOWNLOAD_URL:-https://amfcdn.lbl.gov/api/v1/data_download}"'), true);
  assert.equal(script.includes('if [ "$DATA_PRODUCT" = "FLUXNET2015" ]; then'), true);
  assert.equal(script.includes('\\"data_product\\": \\"${DATA_PRODUCT}\\"'), true);
  assert.equal(script.includes('\\"data_variant\\": \\"FULLSET\\"'), true);
  assert.equal(script.includes('\\"data_policy\\": \\"CCBY4.0\\"'), true);
  assert.equal(script.includes('\\"intended_use\\": \\"other_research\\"'), true);
  assert.equal(script.includes('\\"intended_use\\": \\"QED Lab FLUXNET Data Explorer\\"'), true);
  assert.equal(script.includes('\\"is_test\\": false'), false);
  assert.equal(script.includes('while IFS=$\'\\t\' read -r SITE_ID DATA_PRODUCT SOURCE_LABEL; do'), true);
  assert.equal(script.includes('clean_url="${url%%\\?*}"'), true);
  assert.equal(script.includes('filename="$(basename "$clean_url")"'), true);
});

test('Download-all wrapper script delegates to both child scripts when both source partitions exist', () => {
  const script = hooks.buildDownloadAllSelectedScriptText({
    includeShuttle: true,
    includeAmeriFlux: true
  });

  assert.equal(script.includes('# Shuttle is preferred for overlap sites (AmeriFlux-shuttle).'), true);
  assert.equal(script.includes('# AmeriFlux API-backed sites (AmeriFlux and FLUXNET2015) are downloaded via the AmeriFlux API.'), true);
  assert.equal(script.includes('if [ -f "./shuttle_selected_sites.txt" ] && [ -s "./shuttle_selected_sites.txt" ]; then'), true);
  assert.equal(script.includes('bash "./download_shuttle_selected.sh" || {'), true);
  assert.equal(script.includes('if [ -f "./ameriflux_selected_sites.txt" ] && [ -s "./ameriflux_selected_sites.txt" ]; then'), true);
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
});
