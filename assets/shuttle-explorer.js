(function () {
  "use strict";

  var DEFAULT_JSON_URL = "assets/shuttle_snapshot.json";
  var DEFAULT_CSV_URL = "assets/shuttle_snapshot.csv";
  var DEFAULT_ICOS_DIRECT_JSON_URL = "assets/icos_direct_fluxnet.json";
  var DEFAULT_ICOS_DIRECT_CSV_URL = "assets/icos_direct_fluxnet.csv";
  var DEFAULT_JAPANFLUX_DIRECT_JSON_URL = "assets/japanflux_direct_snapshot.json";
  var DEFAULT_JAPANFLUX_DIRECT_CSV_URL = "assets/japanflux_direct_snapshot.csv";
  var DEFAULT_EFD_JSON_URL = "assets/efd_sites_snapshot.json";
  var DEFAULT_EFD_CSV_URL = "assets/efd_sites_snapshot.csv";
  var AMERIFLUX_SITE_INFO_URL = "assets/ameriflux_site_info.csv";
  var FLUXNET2015_SITE_INFO_URL = "assets/siteinfo_fluxnet2015.csv";
  var SITE_NAME_METADATA_URL = "assets/site_name_metadata.csv";
  var SITE_VEGETATION_METADATA_URL = "assets/site_vegetation_metadata.csv";
  var DEFAULT_PAGE_SIZE = 10;
  var MAX_PAGE_BUTTONS = 7;
  var SEARCH_DEBOUNCE_MS = 180;
  var STYLE_ID = "shuttle-explorer-inline-styles";
  var SNAPSHOT_CACHE_SCHEMA_VERSION = 8;
  var SNAPSHOT_CACHE_STORAGE_PREFIX = "shuttle-explorer:snapshot-cache:v8";
  var AMERIFLUX_FLUXNET_AVAILABILITY_URL = "https://amfcdn.lbl.gov/api/v2/data_availability/AmeriFlux/FLUXNET/CCBY4.0";
  var AMERIFLUX_BASE_AVAILABILITY_URL = "https://amfcdn.lbl.gov/api/v2/data_availability/AmeriFlux/BASE-BADM/CCBY4.0";
  var FLUXNET2015_AVAILABILITY_URL = "https://amfcdn.lbl.gov/api/v2/data_availability/FLUXNET/FLUXNET2015/CCBY4.0";
  var AMERIFLUX_V2_DOWNLOAD_URL = "https://amfcdn.lbl.gov/api/v2/data_download";
  var AMERIFLUX_V1_DOWNLOAD_URL = "https://amfcdn.lbl.gov/api/v1/data_download";
  var AMERIFLUX_DEFAULT_VARIANT = "FULLSET";
  var AMERIFLUX_DEFAULT_POLICY = "CCBY4.0";
  var AMERIFLUX_V2_INTENDED_USE = "other_research";
  var AMERIFLUX_V1_INTENDED_USE = "QED Lab FLUXNET Data Explorer";
  var AMERIFLUX_TEMPLATE_USER_ID = "YOUR_AMERIFLUX_USERNAME";
  var AMERIFLUX_TEMPLATE_USER_EMAIL = "YOUR_EMAIL";
  var AMERIFLUX_BULK_FALLBACK_USER_ID = "trevorkeenan";
  var AMERIFLUX_BULK_FALLBACK_USER_EMAIL = "trevorkeenan@berkeley.edu";
  var AMERIFLUX_BULK_IDENTITY_STORAGE_KEY = "shuttle-explorer:ameriflux-bulk-identity:v1";
  var AMERIFLUX_TRUSTED_RUNTIME_FLAG = "amerifluxTrustedRuntime";
  var ICOS_DIRECT_SOURCE_ONLY = "ICOS";
  var AMERIFLUX_SOURCE_ONLY = "AmeriFlux";
  var BASE_SOURCE_ONLY = "BASE";
  var FLUXNET2015_SOURCE_ONLY = "FLUXNET2015";
  var AMERIFLUX_SHUTTLE = "AmeriFlux-Shuttle";
  var SHUTTLE_SOURCE = "Shuttle";
  var SOURCE_FILTER_TAG_AMERIFLUX = "AmeriFlux";
  var SOURCE_FILTER_TAG_AMERIFLUX_SHUTTLE = "AmeriFlux-Shuttle";
  var SOURCE_FILTER_TAG_CHINAFLUX = "ChinaFlux";
  var SOURCE_FILTER_TAG_EFD = "EFD";
  var SOURCE_FILTER_TAG_FLUXNET_2015 = "FLUXNET-2015";
  var SOURCE_FILTER_TAG_FLUXNET_SHUTTLE = "FLUXNET-Shuttle";
  var SOURCE_FILTER_TAG_ICOS = "ICOS";
  var SOURCE_FILTER_TAG_ICOS_SHUTTLE = "ICOS-Shuttle";
  var SOURCE_FILTER_TAG_JAPANFLUX = "JapanFlux";
  var SOURCE_FILTER_TAG_TERN = "TERN";
  var SOURCE_FILTER_TAG_TERN_SHUTTLE = "TERN-Shuttle";
  var SOURCE_FILTER_OPTIONS = [
    SOURCE_FILTER_TAG_AMERIFLUX,
    SOURCE_FILTER_TAG_AMERIFLUX_SHUTTLE,
    SOURCE_FILTER_TAG_CHINAFLUX,
    SOURCE_FILTER_TAG_EFD,
    SOURCE_FILTER_TAG_FLUXNET_2015,
    SOURCE_FILTER_TAG_FLUXNET_SHUTTLE,
    SOURCE_FILTER_TAG_ICOS,
    SOURCE_FILTER_TAG_ICOS_SHUTTLE,
    SOURCE_FILTER_TAG_JAPANFLUX,
    SOURCE_FILTER_TAG_TERN,
    SOURCE_FILTER_TAG_TERN_SHUTTLE
  ];
  var FLUXNET2015_COUNTRY_ALIAS_NORMALIZATIONS = {
    "russian federation": "russia",
    "people s republic of china": "china",
    "peoples republic of china": "china",
    "pr china": "china"
  };
  var FLUXNET2015_AMERICAS_COUNTRY_CODES = [
    "AG", "AI", "AR", "AW", "BB", "BL", "BM", "BO", "BQ", "BR", "BS", "BZ",
    "CA", "CL", "CO", "CR", "CU", "CW", "DM", "DO", "EC", "FK", "GD", "GF",
    "GL", "GP", "GT", "GY", "HN", "HT", "JM", "KN", "KY", "LC", "MF", "MQ",
    "MS", "MX", "NI", "PA", "PE", "PM", "PR", "PY", "SR", "SV", "SX", "TC",
    "TT", "US", "UY", "VC", "VE", "VG", "VI"
  ];
  var FLUXNET2015_ICOS_COUNTRY_CODES = [
    "AD", "AL", "AO", "AT", "BE", "BF", "BG", "BI", "BJ", "BW", "BY", "CD",
    "CF", "CG", "CH", "CI", "CM", "CV", "CY", "CZ", "DE", "DJ", "DK", "DZ",
    "EE", "EG", "EH", "ER", "ES", "ET", "FI", "FO", "FR", "GA", "GB", "GH",
    "GI", "GM", "GN", "GQ", "GR", "GW", "HR", "HU", "IE", "IM", "IS", "IT",
    "KE", "KM", "XK", "LI", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC",
    "MD", "ME", "MG", "MK", "ML", "MT", "MU", "MW", "MZ", "NA", "NE", "NG",
    "NL", "NO", "PL", "PT", "RE", "RO", "RS", "RU", "RW", "SC", "SD", "SE",
    "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SS", "ST", "SZ", "TD",
    "TG", "TN", "TZ", "UA", "UG", "UK", "VA", "YT", "ZA", "ZM", "ZW"
  ];
  var FLUXNET2015_TERN_COUNTRY_CODES = ["AU", "NZ"];
  var FLUXNET2015_CHINA_COUNTRY_CODES = ["CN"];
  var SHUTTLE_SOURCE_ORIGIN = "shuttle";
  var ICOS_DIRECT_SOURCE_ORIGIN = "icos_direct";
  var JAPANFLUX_DIRECT_SOURCE_ORIGIN = "japanflux_direct";
  var EFD_SOURCE_ORIGIN = "efd";
  var AMERIFLUX_API_SOURCE_ORIGIN = "ameriflux_api";
  var SHUTTLE_SOURCE_PRIORITY = 400;
  var ICOS_DIRECT_SOURCE_PRIORITY = 300;
  var JAPANFLUX_DIRECT_SOURCE_PRIORITY = 250;
  var AMERIFLUX_SOURCE_PRIORITY = 200;
  var FLUXNET2015_SOURCE_PRIORITY = 100;
  var EFD_SOURCE_PRIORITY = 50;
  var AMERIFLUX_FLUXNET_PRODUCT = "FLUXNET";
  var AMERIFLUX_BASE_PRODUCT = "BASE-BADM";
  var FLUXNET2015_PRODUCT = "FLUXNET2015";
  var AMERIFLUX_DATA_HUB = "AmeriFlux";
  var LANDING_PAGE_DOWNLOAD_MODE = "landing_page";
  var REQUEST_PAGE_DOWNLOAD_MODE = "request_page";
  var AMERIFLUX_FLUXNET_AVAILABILITY_CACHE_KEY = "shuttle-explorer:ameriflux-fluxnet-availability:v1";
  var AMERIFLUX_BASE_AVAILABILITY_CACHE_KEY = "shuttle-explorer:ameriflux-base-availability:v1";
  var FLUXNET2015_AVAILABILITY_CACHE_KEY = "shuttle-explorer:fluxnet2015-availability:v1";
  var AMERIFLUX_AVAILABILITY_CACHE_MAX_AGE_MS = 6 * 60 * 60 * 1000;
  var PRODUCT_FAMILY_FLUXNET = "FLUXNET";
  var PRODUCT_FAMILY_BASE = "BASE";
  var PRODUCT_FAMILY_ICOS_ETC = "ICOS_ETC";
  var PROCESSING_LINEAGE_ONEFLUX = "oneflux";
  var PROCESSING_LINEAGE_OTHER = "other_processed";
  var ICOS_CLASSIC_ARCHIVE_SPEC_URI = "http://meta.icos-cp.eu/resources/cpmeta/miscFluxnetArchiveProduct";
  var ICOS_PRODUCT_SPEC_URI = "http://meta.icos-cp.eu/resources/cpmeta/miscFluxnetProduct";
  var ICOS_ETC_ARCHIVE_SPEC_URI = "http://meta.icos-cp.eu/resources/cpmeta/etcArchiveProduct";
  var ICOS_RELEASE_VERSION_RE = /_(\d{4})_(\d+)-(\d+)(?:\.|_|$)/i;
  var ICOS_CURRENT_VERSION_RE = /_v(\d+(?:\.\d+)*)_r(\d+)/i;
  var ICOS_BETA_VERSION_RE = /_beta[-_]?(\d+)/i;
  var ICOS_RESOLUTION_PRODUCT_RE = /_(HH|HR|DD|WW|MM|YY|NRT)_/i;
  var SURFACED_CLASSIFICATION_FLUXNET_PROCESSED = "fluxnet_processed";
  var SURFACED_CLASSIFICATION_OTHER_PROCESSED = "other_processed";
  var SURFACED_CLASSIFICATION_FLUXNET_AND_OTHER = "fluxnet_and_other_processed";
  var FILTER_LABEL_FLUXNET_PROCESSED = "FLUXNET processed";
  var FILTER_LABEL_OTHER_PROCESSED = "Other processed";
  var FILTER_LABEL_FLUXNET_AND_OTHER = "Sites with both FLUXNET and additional processed years";
  var TABLE_LABEL_FLUXNET_AND_OTHER = "Sites with both FLUXNET and additional processed years";
  var MAX_HTTP_RETRIES = 3;
  var RETRY_BASE_DELAY_MS = 500;
  var COPY_TABLE_BUTTON_LABEL = "Copy table to clipboard";
  var COPY_TABLE_SUCCESS_LABEL = "Copied!";
  var COPY_TABLE_FAILURE_LABEL = "Copy failed";
  var COPY_TABLE_FEEDBACK_MS = 1800;
  var COUNTRY_CODE_TO_NAME = {
    AR: "Argentina",
    AT: "Austria",
    AU: "Australia",
    BE: "Belgium",
    BR: "Brazil",
    CA: "Canada",
    CG: "Republic of the Congo",
    CH: "Switzerland",
    CL: "Chile",
    CN: "China",
    CO: "Colombia",
    CR: "Costa Rica",
    CZ: "Czech Republic",
    DE: "Germany",
    DK: "Denmark",
    ES: "Spain",
    FI: "Finland",
    FR: "France",
    GF: "French Guiana",
    GH: "Ghana",
    GL: "Greenland",
    ID: "Indonesia",
    IT: "Italy",
    JP: "Japan",
    KH: "Cambodia",
    MN: "Mongolia",
    MX: "Mexico",
    MY: "Malaysia",
    NL: "Netherlands",
    PA: "Panama",
    PE: "Peru",
    PR: "Puerto Rico",
    RU: "Russia",
    SD: "Sudan",
    SE: "Sweden",
    SJ: "Svalbard and Jan Mayen",
    SN: "Senegal",
    TH: "Thailand",
    UK: "United Kingdom",
    US: "USA",
    XK: "Kosovo",
    ZA: "South Africa",
    ZM: "Zambia"
  };
  var COUNTRY_NAME_ALIASES = {
    "u s": "USA",
    "u s a": "USA",
    "usa": "USA",
    "united states": "USA",
    "united states of america": "USA"
  };
  var COUNTRY_DISPLAY_NAMES = null;
  var NETWORK_TOKEN_DISPLAY_NAMES = {
    CNF: "ChinaFlux",
    EUF: "EuroFlux",
    JPF: "JapanFlux",
    KOF: "KoreaFlux"
  };
  var VEGETATION_IGBP_DISPLAY_NAMES = {
    BSV: "Barren or Sparsely Vegetated",
    CRO: "Croplands",
    CSH: "Closed Shrublands",
    CVM: "Cropland/Natural Vegetation Mosaics",
    DBF: "Deciduous Broadleaf Forests",
    DNF: "Deciduous Needleleaf Forests",
    EBF: "Evergreen Broadleaf Forests",
    ENF: "Evergreen Needleleaf Forests",
    GRA: "Grasslands",
    MF: "Mixed Forests",
    OSH: "Open Shrublands",
    SAV: "Savannas",
    SNO: "Snow and Ice",
    URB: "Urban and Built-Up Lands",
    WAT: "Water Bodies",
    WET: "Permanent Wetlands",
    WSA: "Woody Savannas"
  };
  var VEGETATION_IGBP_LABEL_ALIASES = {
    BSV: ["Barren or Sparsely Vegetated Lands", "Barren or Sparsely Vegetated Land"],
    CRO: ["Cropland"],
    CSH: ["Closed Shrubland"],
    CVM: ["Cropland/Natural Vegetation Mosaic", "Cropland Natural Vegetation Mosaic", "Cropland Natural Vegetation Mosaics"],
    DBF: ["Deciduous Broadleaf Forest"],
    DNF: ["Deciduous Needleleaf Forest"],
    EBF: ["Evergreen Broadleaf Forest"],
    ENF: ["Evergreen Needleleaf Forest"],
    GRA: ["Grassland"],
    MF: ["Mixed Forest"],
    OSH: ["Open Shrubland"],
    SAV: ["Savanna"],
    URB: ["Urban and Built Up Lands", "Urban and Built Up Land", "Urban and Built-up Lands", "Urban and Built-up Land"],
    WAT: ["Water Body"],
    WET: ["Permanent Wetland"],
    WSA: ["Woody Savanna"]
  };

  var SORT_COLUMNS = [
    { key: "site_id", label: "Site ID", type: "string" },
    { key: "site_name", label: "Site Name", type: "string" },
    { key: "country", label: "Country", type: "string" },
    { key: "data_hub", label: "Hub", type: "string" },
    { key: "vegetation_type", label: "Veg Type", type: "string" },
    { key: "years", label: "Years", type: "years" },
    { key: "length_years", label: "Length", type: "number" }
  ];

  function bySelector(root, selector) {
    return root ? root.querySelector(selector) : null;
  }

  function qsa(root, selector) {
    return root ? Array.prototype.slice.call(root.querySelectorAll(selector)) : [];
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function toSnakeCase(name) {
    return String(name || "")
      .replace(/^\uFEFF/, "")
      .trim()
      .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
      .replace(/[^A-Za-z0-9]+/g, "_")
      .replace(/_+/g, "_")
      .replace(/^_+|_+$/g, "")
      .toLowerCase();
  }

  function normalizeSiteId(siteId) {
    return String(siteId || "").trim().toUpperCase();
  }

  function firstDefinedString(raw, keys) {
    var i;
    if (!raw || typeof raw !== "object") {
      return "";
    }
    for (i = 0; i < keys.length; i += 1) {
      var value = raw[keys[i]];
      if (value == null) {
        continue;
      }
      value = String(value).trim();
      if (value) {
        return value;
      }
    }
    return "";
  }

  function ameriFluxSiteInfoSiteId(raw) {
    return normalizeSiteId(firstDefinedString(raw, ["site_id", "site", "site_code", "siteid"]));
  }

  function fluxnet2015SiteInfoSiteId(raw) {
    return normalizeSiteId(firstDefinedString(raw, ["site_id", "mysitename", "site_code", "siteid", "site"]));
  }

  function vegetationMetadataSiteId(raw) {
    return normalizeSiteId(firstDefinedString(raw, ["site_id", "mysitename", "site_code", "siteid", "site"]));
  }

  function siteNameMetadataSiteId(raw) {
    return normalizeSiteId(firstDefinedString(raw, ["site_id", "mysitename", "site_code", "siteid", "site"]));
  }

  function siteInfoSiteName(raw) {
    return firstDefinedString(raw, ["site_name", "site_title", "site_label", "name", "sitename"]);
  }

  function siteInfoCountry(raw) {
    return firstDefinedString(raw, ["country", "country_name", "country_code"]);
  }

  function extractVegetationMetadataValue(raw) {
    return firstDefinedString(raw, ["vegetation_type", "igbp", "veg_type"]);
  }

  function countryCodeToName(code) {
    var normalized = String(code || "").trim().toUpperCase();
    var displayName;
    if (!normalized || !/^[A-Z]{2}$/.test(normalized)) {
      return "";
    }
    if (COUNTRY_CODE_TO_NAME[normalized]) {
      return COUNTRY_CODE_TO_NAME[normalized];
    }
    if (COUNTRY_DISPLAY_NAMES == null && typeof Intl !== "undefined" && typeof Intl.DisplayNames === "function") {
      try {
        COUNTRY_DISPLAY_NAMES = new Intl.DisplayNames(["en"], { type: "region" });
      } catch (e) {
        COUNTRY_DISPLAY_NAMES = false;
      }
    }
    if (COUNTRY_DISPLAY_NAMES && typeof COUNTRY_DISPLAY_NAMES.of === "function") {
      try {
        displayName = COUNTRY_DISPLAY_NAMES.of(normalized);
      } catch (err) {
        displayName = "";
      }
      if (displayName && displayName !== normalized && String(displayName).toLowerCase().indexOf("unknown") !== 0) {
        return displayName;
      }
    }
    return "";
  }

  function normalizeCountryName(value) {
    var raw = String(value || "").trim();
    var aliasKey = raw.toLowerCase().replace(/\./g, "").replace(/[^a-z]+/g, " ").trim();
    var mapped = countryCodeToName(raw);
    if (aliasKey && COUNTRY_NAME_ALIASES[aliasKey]) {
      return COUNTRY_NAME_ALIASES[aliasKey];
    }
    return mapped || raw;
  }

  function countryLookupKey(value) {
    var normalized = normalizeCountryName(value);
    var key = String(normalized || value || "")
      .trim()
      .toLowerCase()
      .replace(/&/g, " and ")
      .replace(/\./g, "")
      .replace(/[^a-z]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    return FLUXNET2015_COUNTRY_ALIAS_NORMALIZATIONS[key] || key;
  }

  function buildCountryLookup(codes) {
    var lookup = {};
    (Array.isArray(codes) ? codes : []).forEach(function (code) {
      var rawCode = String(code || "").trim().toUpperCase();
      var normalizedName = countryLookupKey(rawCode);
      if (!rawCode) {
        return;
      }
      lookup[rawCode.toLowerCase()] = true;
      if (normalizedName) {
        lookup[normalizedName] = true;
      }
    });
    return lookup;
  }

  var FLUXNET2015_AMERICAS_COUNTRY_LOOKUP = buildCountryLookup(FLUXNET2015_AMERICAS_COUNTRY_CODES);
  var FLUXNET2015_ICOS_COUNTRY_LOOKUP = buildCountryLookup(FLUXNET2015_ICOS_COUNTRY_CODES);
  var FLUXNET2015_TERN_COUNTRY_LOOKUP = buildCountryLookup(FLUXNET2015_TERN_COUNTRY_CODES);
  var FLUXNET2015_CHINA_COUNTRY_LOOKUP = buildCountryLookup(FLUXNET2015_CHINA_COUNTRY_CODES);

  function inferFluxnet2015NetworkFromCountry(country) {
    var key = countryLookupKey(country);
    if (!key) {
      return null;
    }
    if (FLUXNET2015_CHINA_COUNTRY_LOOKUP[key]) {
      return SOURCE_FILTER_TAG_CHINAFLUX;
    }
    if (FLUXNET2015_TERN_COUNTRY_LOOKUP[key]) {
      return SOURCE_FILTER_TAG_TERN;
    }
    if (FLUXNET2015_AMERICAS_COUNTRY_LOOKUP[key]) {
      return SOURCE_FILTER_TAG_AMERIFLUX;
    }
    if (FLUXNET2015_ICOS_COUNTRY_LOOKUP[key]) {
      return SOURCE_FILTER_TAG_ICOS;
    }
    return null;
  }

  function buildSiteInfoEntry(siteId, raw) {
    return {
      site_id: siteId,
      site_name: siteInfoSiteName(raw),
      country: normalizeCountryName(siteInfoCountry(raw)),
      latitude: extractRawLatitude(raw),
      longitude: extractRawLongitude(raw)
    };
  }

  function parseIntOrNull(value) {
    var s = String(value == null ? "" : value).trim();
    if (!s || !/^-?\d+$/.test(s)) {
      return null;
    }
    var n = parseInt(s, 10);
    return isFinite(n) ? n : null;
  }

  function formatIsoDate(date) {
    if (!(date instanceof Date) || !isFinite(date.getTime())) {
      return "";
    }
    var y = date.getFullYear();
    var m = String(date.getMonth() + 1).padStart(2, "0");
    var d = String(date.getDate()).padStart(2, "0");
    return y + "-" + m + "-" + d;
  }

  function normalizeSnapshotUpdatedDate(value) {
    var raw = String(value || "").trim();
    if (!raw) {
      return "";
    }
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
      return raw;
    }
    return formatIsoDate(new Date(raw));
  }

  function extractSnapshotUpdatedDate(meta) {
    if (!meta || typeof meta !== "object" || Array.isArray(meta)) {
      return "";
    }
    return normalizeSnapshotUpdatedDate(meta.snapshot_updated_date) ||
      normalizeSnapshotUpdatedDate(meta.snapshot_updated_at);
  }

  function snapshotUpdatedDateDisplayText(snapshotUpdatedDate) {
    return normalizeSnapshotUpdatedDate(snapshotUpdatedDate) || "unavailable";
  }

  function gaEvent(name, params) {
    if (typeof window.gtag === "function") {
      window.gtag("event", name, params || {});
    }
  }

  function safeJsonParse(text) {
    if (!text) {
      return null;
    }
    try {
      return JSON.parse(text);
    } catch (e) {
      return null;
    }
  }

  function getLocalStorageSafe() {
    try {
      return window.localStorage || null;
    } catch (e) {
      return null;
    }
  }

  function normalizeOptionalInputValue(value) {
    return String(value == null ? "" : value).trim();
  }

  function resolveAmeriFluxBulkIdentity(userIdInput, userEmailInput) {
    var enteredUserId = normalizeOptionalInputValue(userIdInput);
    var enteredUserEmail = normalizeOptionalInputValue(userEmailInput);
    return {
      enteredUserId: enteredUserId,
      enteredUserEmail: enteredUserEmail,
      user_id: enteredUserId || AMERIFLUX_BULK_FALLBACK_USER_ID,
      user_email: enteredUserEmail || AMERIFLUX_BULK_FALLBACK_USER_EMAIL
    };
  }

  function resolveAmeriFluxIdentityOverride(identityOverride) {
    if (!identityOverride || typeof identityOverride !== "object") {
      return null;
    }
    return resolveAmeriFluxBulkIdentity(
      firstDefinedString(identityOverride, ["enteredUserId", "user_id", "userId"]),
      firstDefinedString(identityOverride, ["enteredUserEmail", "user_email", "userEmail"])
    );
  }

  function readAmeriFluxBulkIdentityPreferences() {
    var storage = getLocalStorageSafe();
    var stored;
    if (!storage) {
      return { userId: "", userEmail: "" };
    }
    try {
      stored = safeJsonParse(storage.getItem(AMERIFLUX_BULK_IDENTITY_STORAGE_KEY));
    } catch (e) {
      stored = null;
    }
    if (!stored || typeof stored !== "object" || Array.isArray(stored)) {
      return { userId: "", userEmail: "" };
    }
    return {
      userId: normalizeOptionalInputValue(stored.userId),
      userEmail: normalizeOptionalInputValue(stored.userEmail)
    };
  }

  function writeAmeriFluxBulkIdentityPreferences(userIdInput, userEmailInput) {
    var storage = getLocalStorageSafe();
    var userId = normalizeOptionalInputValue(userIdInput);
    var userEmail = normalizeOptionalInputValue(userEmailInput);
    if (!storage) {
      return;
    }
    try {
      if (!userId && !userEmail) {
        storage.removeItem(AMERIFLUX_BULK_IDENTITY_STORAGE_KEY);
        return;
      }
      storage.setItem(AMERIFLUX_BULK_IDENTITY_STORAGE_KEY, JSON.stringify({
        userId: userId,
        userEmail: userEmail
      }));
    } catch (e) {
      return;
    }
  }

  function snapshotCacheBaseKey(jsonUrl, csvUrl) {
    return [
      SNAPSHOT_CACHE_STORAGE_PREFIX,
      encodeURIComponent(String(jsonUrl || "")),
      encodeURIComponent(String(csvUrl || ""))
    ].join("|");
  }

  function snapshotCacheIndexKey(baseKey) {
    return baseKey + "|index";
  }

  function snapshotCacheRecordKey(baseKey, freshnessKey) {
    return baseKey + "|record|" + encodeURIComponent(String(freshnessKey || "unknown"));
  }

  function readSnapshotCache(jsonUrl, csvUrl) {
    var storage = getLocalStorageSafe();
    if (!storage) {
      return null;
    }
    var baseKey = snapshotCacheBaseKey(jsonUrl, csvUrl);
    var indexKey = snapshotCacheIndexKey(baseKey);

    try {
      var index = safeJsonParse(storage.getItem(indexKey));
      if (!index || index.schema !== SNAPSHOT_CACHE_SCHEMA_VERSION || !index.recordKey) {
        return null;
      }
      var record = safeJsonParse(storage.getItem(String(index.recordKey)));
      if (!record || record.schema !== SNAPSHOT_CACHE_SCHEMA_VERSION || !Array.isArray(record.rows)) {
        return null;
      }
      return record;
    } catch (e) {
      return null;
    }
  }

  function writeSnapshotCache(jsonUrl, csvUrl, entry) {
    var storage = getLocalStorageSafe();
    if (!storage || !entry || !Array.isArray(entry.rows)) {
      return;
    }
    var baseKey = snapshotCacheBaseKey(jsonUrl, csvUrl);
    var indexKey = snapshotCacheIndexKey(baseKey);
    var freshnessKey = String(entry.freshnessKey || "unknown");
    var recordKey = snapshotCacheRecordKey(baseKey, freshnessKey);

    var record = {
      schema: SNAPSHOT_CACHE_SCHEMA_VERSION,
      freshnessKey: freshnessKey,
      source: entry.source || "",
      sourceUrl: entry.sourceUrl || "",
      warning: entry.warning || "",
      snapshotUpdatedDate: entry.snapshotUpdatedDate || "",
      rows: entry.rows,
      droppedRows: entry.droppedRows || 0,
      amerifluxTotalSites: entry.amerifluxTotalSites || 0,
      amerifluxSitesWithYears: entry.amerifluxSitesWithYears || 0,
      amerifluxOverlapSites: entry.amerifluxOverlapSites || 0,
      amerifluxOnlySites: entry.amerifluxOnlySites || 0,
      fluxnet2015TotalSites: entry.fluxnet2015TotalSites || 0,
      fluxnet2015SitesWithYears: entry.fluxnet2015SitesWithYears || 0,
      fluxnet2015OnlySites: entry.fluxnet2015OnlySites || 0,
      cachedAt: new Date().toISOString()
    };

    try {
      var oldIndex = safeJsonParse(storage.getItem(indexKey));
      storage.setItem(recordKey, JSON.stringify(record));
      storage.setItem(indexKey, JSON.stringify({
        schema: SNAPSHOT_CACHE_SCHEMA_VERSION,
        recordKey: recordKey,
        freshnessKey: freshnessKey
      }));
      if (oldIndex && oldIndex.recordKey && oldIndex.recordKey !== recordKey) {
        storage.removeItem(String(oldIndex.recordKey));
      }
    } catch (e) {
      try {
        storage.removeItem(indexKey);
      } catch (e2) {
        // Ignore localStorage cleanup failures.
      }
    }
  }

  function splitNetworks(value) {
    var s = String(value || "").trim();
    if (!s) {
      return [];
    }
    var parts = s.split(";").map(function (item) {
      return item.trim();
    }).filter(Boolean);
    return parts.length ? parts : [s];
  }

  function normalizeNetworkToken(value) {
    var token = String(value || "").trim();
    var mapped;
    if (!token) {
      return "";
    }
    mapped = NETWORK_TOKEN_DISPLAY_NAMES[token.toUpperCase()];
    return mapped || token;
  }

  function normalizeNetworkTokens(value) {
    var seen = {};
    var tokens = [];
    splitNetworks(value).forEach(function (token) {
      var normalized = normalizeNetworkToken(token);
      var key = String(normalized || "").toLowerCase();
      if (!normalized || seen[key]) {
        return;
      }
      seen[key] = true;
      tokens.push(normalized);
    });
    return tokens;
  }

  function normalizeNetworkDisplayValue(value) {
    return normalizeNetworkTokens(value).join(";");
  }

  function vegetationLookupKey(value) {
    return String(value || "")
      .trim()
      .toLowerCase()
      .replace(/&/g, " and ")
      .replace(/[^a-z0-9]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function buildVegetationNameToCodeMap() {
    var lookup = {};
    Object.keys(VEGETATION_IGBP_DISPLAY_NAMES).forEach(function (code) {
      var names = [VEGETATION_IGBP_DISPLAY_NAMES[code]].concat(VEGETATION_IGBP_LABEL_ALIASES[code] || []);
      names.forEach(function (name) {
        var key = vegetationLookupKey(name);
        if (key) {
          lookup[key] = code;
        }
      });
    });
    return lookup;
  }

  var VEGETATION_NAME_TO_IGBP_CODE = buildVegetationNameToCodeMap();

  function normalizeVegetationType(value) {
    var raw = String(value == null ? "" : value).trim();
    var canonicalCode;
    if (!raw) {
      return "";
    }
    canonicalCode = VEGETATION_NAME_TO_IGBP_CODE[vegetationLookupKey(raw)];
    return canonicalCode || raw;
  }

  function vegetationDisplayLabel(value) {
    var raw = String(value == null ? "" : value).trim();
    var canonicalCode;
    if (!raw) {
      return "";
    }
    canonicalCode = String(normalizeVegetationType(raw) || "").trim().toUpperCase();
    return VEGETATION_IGBP_DISPLAY_NAMES[canonicalCode] || raw;
  }

  function hasNetworkTag(value, expected) {
    var target = String(expected || "").trim().toLowerCase();
    if (!target) {
      return false;
    }
    return splitNetworks(value).some(function (token) {
      return String(token || "").toLowerCase() === target;
    });
  }

  function normalizeNetworkDisplay(row) {
    if (!row || typeof row !== "object") {
      return row;
    }
    var sourceLabel = String(row.source_label || "").trim();
    var dataProduct = String(row.api_data_product || "").trim().toUpperCase();
    var networkDisplay = String(row.network_display || row.network || row.source_network || "").trim();
    var reliableFluxnet2015Tokens;
    var inferredFluxnet2015Network;
    if (
      sourceLabel === FLUXNET2015_SOURCE_ONLY ||
      dataProduct === FLUXNET2015_PRODUCT ||
      hasNetworkTag(row.network_display, FLUXNET2015_SOURCE_ONLY) ||
      hasNetworkTag(row.network, FLUXNET2015_SOURCE_ONLY) ||
      hasNetworkTag(row.source_network, FLUXNET2015_SOURCE_ONLY)
    ) {
      reliableFluxnet2015Tokens = rowNetworkTokens(row).filter(function (token) {
        return String(token || "").trim() && String(token || "").trim() !== FLUXNET2015_SOURCE_ONLY;
      });
      inferredFluxnet2015Network = inferFluxnet2015NetworkFromCountry(row.country);
      networkDisplay = reliableFluxnet2015Tokens.length
        ? reliableFluxnet2015Tokens.join(";")
        : (inferredFluxnet2015Network || "");
      row.network = networkDisplay;
      row.source_network = networkDisplay;
    } else if (
      sourceLabel === BASE_SOURCE_ONLY ||
      dataProduct === AMERIFLUX_BASE_PRODUCT ||
      String(row.data_hub || "").toLowerCase() === "ameriflux" ||
      sourceLabel === AMERIFLUX_SOURCE_ONLY ||
      sourceLabel === AMERIFLUX_SHUTTLE ||
      hasNetworkTag(row.network_display, AMERIFLUX_SOURCE_ONLY) ||
      hasNetworkTag(row.network, AMERIFLUX_SOURCE_ONLY) ||
      hasNetworkTag(row.source_network, AMERIFLUX_SOURCE_ONLY)
    ) {
      networkDisplay = AMERIFLUX_SOURCE_ONLY;
    }
    row.network_display = normalizeNetworkDisplayValue(networkDisplay);
    row.network_tokens = normalizeNetworkTokens(networkDisplay);
    return row;
  }

  function deriveCountry(siteId, fallback) {
    var fb = normalizeCountryName(fallback);
    if (fb) {
      return fb;
    }
    var s = String(siteId || "").trim();
    var code = "";
    if (!s) {
      return "";
    }
    var idx = s.indexOf("-");
    if (idx > 0) {
      code = s.slice(0, idx).toUpperCase();
    } else {
      idx = s.indexOf("_");
      if (idx > 0) {
        code = s.slice(0, idx).toUpperCase();
      } else {
        code = s.slice(0, 2).toUpperCase();
      }
    }
    return countryCodeToName(code) || code;
  }

  function isIcosRow(row) {
    var url = String(row.download_link || "").toLowerCase();
    return resolveSourceOrigin(row) === ICOS_DIRECT_SOURCE_ORIGIN ||
      String(row && row.source_label || "").trim() === ICOS_DIRECT_SOURCE_ONLY ||
      /data\.icos-cp\.eu\/licence_accept/.test(url);
  }

  function normalizeProductFamily(value) {
    var normalized = String(value || "").trim().toUpperCase();
    if (normalized === PRODUCT_FAMILY_BASE) {
      return PRODUCT_FAMILY_BASE;
    }
    if (normalized === PRODUCT_FAMILY_ICOS_ETC || normalized === "ICOS ETC") {
      return PRODUCT_FAMILY_ICOS_ETC;
    }
    return PRODUCT_FAMILY_FLUXNET;
  }

  function normalizeVersionReference(value) {
    return String(value || "").trim().replace(/\/+$/, "");
  }

  function inferRowProductFamily(row) {
    var explicit = String(row && (row.product_family || row.productFamily) || "").trim();
    var objectSpec = String(row && row.object_spec || "").trim();
    var fileName = String(row && row.file_name || "").trim().toUpperCase();
    var dataProduct = String(row && (row.api_data_product || row.apiDataProduct) || "").trim().toUpperCase();

    if (explicit) {
      return normalizeProductFamily(explicit);
    }
    if (dataProduct === AMERIFLUX_BASE_PRODUCT) {
      return PRODUCT_FAMILY_BASE;
    }
    if (
      objectSpec === ICOS_ETC_ARCHIVE_SPEC_URI ||
      fileName.indexOf("ICOSETC_") === 0 ||
      fileName.indexOf("ARCHIVE_L2") !== -1
    ) {
      return PRODUCT_FAMILY_ICOS_ETC;
    }
    return PRODUCT_FAMILY_FLUXNET;
  }

  function isIcosEtcRow(row) {
    return isIcosRow(row) && inferRowProductFamily(row) === PRODUCT_FAMILY_ICOS_ETC;
  }

  function isIcosResolutionProduct(fileName) {
    var upper = String(fileName || "").trim().toUpperCase();
    return /\.CSV\.ZIP$/.test(upper) || ICOS_RESOLUTION_PRODUCT_RE.test(upper);
  }

  function isPreferredIcosClassicArchiveRow(row) {
    var fileName = String(row && row.file_name || "").trim();
    var upper = fileName.toUpperCase();
    if (inferRowProductFamily(row) !== PRODUCT_FAMILY_FLUXNET) {
      return false;
    }
    if (String(row && row.object_spec || "").trim() === ICOS_CLASSIC_ARCHIVE_SPEC_URI) {
      return true;
    }
    if (isIcosResolutionProduct(fileName)) {
      return false;
    }
    if (upper.indexOf("FULLSET") !== -1) {
      return true;
    }
    return upper.indexOf("_FLUXNET_") !== -1;
  }

  function parseIcosVersionRank(fileName) {
    var release = ICOS_RELEASE_VERSION_RE.exec(String(fileName || ""));
    var current;
    var beta;

    if (release) {
      return [3, [parseInt(release[1], 10), parseInt(release[2], 10), parseInt(release[3], 10)], 0];
    }

    current = ICOS_CURRENT_VERSION_RE.exec(String(fileName || ""));
    if (current) {
      return [
        2,
        current[1].split(".").map(function (part) { return parseInt(part, 10) || 0; }),
        parseInt(current[2], 10) || 0
      ];
    }

    beta = ICOS_BETA_VERSION_RE.exec(String(fileName || ""));
    if (beta) {
      return [1, [parseInt(beta[1], 10) || 0], 0];
    }

    return [0, [], 0];
  }

  function icosCoverageRank(row) {
    var firstYear = parseIntOrNull(row && row.first_year);
    var lastYear = parseIntOrNull(row && row.last_year);
    var length = firstYear != null && lastYear != null && lastYear >= firstYear ? (lastYear - firstYear) + 1 : 0;
    var earlierStartBonus = firstYear != null ? (9999 - firstYear) : 0;
    return [lastYear || 0, length, earlierStartBonus];
  }

  function icosCanonicalNameBonus(row) {
    var upper = String(row && row.file_name || "").trim().toUpperCase();
    if (inferRowProductFamily(row) === PRODUCT_FAMILY_ICOS_ETC) {
      return [
        upper.indexOf("ICOSETC_") === 0 ? 1 : 0,
        upper.indexOf("ARCHIVE_L2") !== -1 ? 1 : 0
      ];
    }
    return [
      upper.indexOf("FLX_") === 0 ? 1 : 0,
      upper.indexOf("FULLSET") !== -1 ? 1 : 0
    ];
  }

  function icosFamilyRank(row) {
    return inferRowProductFamily(row) === PRODUCT_FAMILY_ICOS_ETC ? 1 : 2;
  }

  function icosMetadataVersionRank(row) {
    var metadataUrl = normalizeVersionReference(row && row.metadata_url);
    var latestVersionUrl = normalizeVersionReference(row && row.latest_version_url);
    return [metadataUrl && latestVersionUrl && metadataUrl === latestVersionUrl ? 1 : 0, 0];
  }

  function compareRankValues(left, right) {
    var leftIsArray = Array.isArray(left);
    var rightIsArray = Array.isArray(right);
    var index;
    var cmp;
    var leftValue;
    var rightValue;

    if (leftIsArray || rightIsArray) {
      leftValue = leftIsArray ? left : [left];
      rightValue = rightIsArray ? right : [right];
      for (index = 0; index < Math.max(leftValue.length, rightValue.length); index += 1) {
        cmp = compareRankValues(
          index < leftValue.length ? leftValue[index] : 0,
          index < rightValue.length ? rightValue[index] : 0
        );
        if (cmp) {
          return cmp;
        }
      }
      return 0;
    }

    leftValue = left == null ? "" : left;
    rightValue = right == null ? "" : right;
    if (leftValue === rightValue) {
      return 0;
    }
    return leftValue > rightValue ? -1 : 1;
  }

  function compareIcosRows(left, right) {
    var rankedPairs = [
      [icosFamilyRank(left), icosFamilyRank(right)],
      [isPreferredIcosClassicArchiveRow(left) ? 1 : 0, isPreferredIcosClassicArchiveRow(right) ? 1 : 0],
      [icosMetadataVersionRank(left), icosMetadataVersionRank(right)],
      [parseIcosVersionRank(left && left.file_name), parseIcosVersionRank(right && right.file_name)],
      [icosCoverageRank(left), icosCoverageRank(right)],
      [icosCanonicalNameBonus(left), icosCanonicalNameBonus(right)],
      [String(left && left.production_end || ""), String(right && right.production_end || "")],
      [String(left && left.coverage_end || ""), String(right && right.coverage_end || "")]
    ];
    var index;
    var cmp;
    var leftTail;
    var rightTail;

    for (index = 0; index < rankedPairs.length; index += 1) {
      cmp = compareRankValues(rankedPairs[index][0], rankedPairs[index][1]);
      if (cmp) {
        return cmp;
      }
    }

    leftTail = [String(left && left.file_name || ""), String(left && left.object_id || "")];
    rightTail = [String(right && right.file_name || ""), String(right && right.object_id || "")];
    if (leftTail[0] < rightTail[0]) {
      return -1;
    }
    if (leftTail[0] > rightTail[0]) {
      return 1;
    }
    if (leftTail[1] < rightTail[1]) {
      return -1;
    }
    if (leftTail[1] > rightTail[1]) {
      return 1;
    }
    return 0;
  }

  function dedupeIcosDirectRows(rows) {
    var grouped = {};
    var siteIds;

    (Array.isArray(rows) ? rows : []).forEach(function (row) {
      var siteId = normalizeSiteId(row && row.site_id);
      if (!siteId) {
        return;
      }
      if (!grouped[siteId]) {
        grouped[siteId] = [];
      }
      grouped[siteId].push(row);
    });

    siteIds = Object.keys(grouped).sort();
    return siteIds.map(function (siteId) {
      return grouped[siteId].slice().sort(compareIcosRows)[0];
    });
  }

  function dedupeSiteLevelRows(rows) {
    var grouped = {};
    var siteIds;

    (Array.isArray(rows) ? rows : []).forEach(function (row) {
      var siteId = normalizeSiteId(row && row.site_id);
      if (!siteId || grouped[siteId]) {
        return;
      }
      grouped[siteId] = row;
    });

    siteIds = Object.keys(grouped).sort();
    return siteIds.map(function (siteId) {
      return grouped[siteId];
    });
  }

  function isJapanFluxSourceRow(row) {
    var sourceLabel = String(row && row.source_label || "").trim();
    var sourceOrigin = resolveSourceOrigin(row);
    var dataHub = String(row && row.data_hub || "").trim().toLowerCase();
    return sourceOrigin === JAPANFLUX_DIRECT_SOURCE_ORIGIN ||
      sourceLabel === SOURCE_FILTER_TAG_JAPANFLUX ||
      dataHub === "japanflux";
  }

  function isEfdSourceRow(row) {
    var sourceLabel = String(row && row.source_label || "").trim();
    var sourceOrigin = resolveSourceOrigin(row);
    var dataHub = String(row && row.data_hub || "").trim().toLowerCase();
    return sourceOrigin === EFD_SOURCE_ORIGIN ||
      sourceLabel === SOURCE_FILTER_TAG_EFD ||
      dataHub === "efd";
  }

  function isRequestOnlyRow(row) {
    return String(row && row.download_mode || "").trim() === REQUEST_PAGE_DOWNLOAD_MODE;
  }

  function resolveSourceOrigin(row) {
    var explicit = String(row && row.source_origin || "").trim();
    if (explicit) {
      return explicit;
    }
    if (String(row && row.download_mode || "").trim() === "ameriflux_api") {
      return AMERIFLUX_API_SOURCE_ORIGIN;
    }
    if (String(row && row.source_label || "").trim() === SOURCE_FILTER_TAG_JAPANFLUX) {
      return JAPANFLUX_DIRECT_SOURCE_ORIGIN;
    }
    if (String(row && row.source_label || "").trim() === SOURCE_FILTER_TAG_EFD) {
      return EFD_SOURCE_ORIGIN;
    }
    if (String(row && row.source_label || "").trim() === ICOS_DIRECT_SOURCE_ONLY) {
      return ICOS_DIRECT_SOURCE_ORIGIN;
    }
    return SHUTTLE_SOURCE_ORIGIN;
  }

  function resolveSourcePriority(row) {
    var explicit = parseIntOrNull(row && row.source_priority);
    var sourceLabel;
    var dataProduct;
    var sourceOrigin;
    if (explicit != null) {
      return explicit;
    }
    sourceLabel = String(row && row.source_label || "").trim();
    dataProduct = String(row && row.api_data_product || "").trim().toUpperCase();
    sourceOrigin = resolveSourceOrigin(row);
    if (sourceOrigin === ICOS_DIRECT_SOURCE_ORIGIN || sourceLabel === ICOS_DIRECT_SOURCE_ONLY) {
      return ICOS_DIRECT_SOURCE_PRIORITY;
    }
    if (sourceOrigin === JAPANFLUX_DIRECT_SOURCE_ORIGIN || sourceLabel === SOURCE_FILTER_TAG_JAPANFLUX) {
      return JAPANFLUX_DIRECT_SOURCE_PRIORITY;
    }
    if (sourceOrigin === EFD_SOURCE_ORIGIN || sourceLabel === SOURCE_FILTER_TAG_EFD) {
      return EFD_SOURCE_PRIORITY;
    }
    if (dataProduct === FLUXNET2015_PRODUCT || sourceLabel === FLUXNET2015_SOURCE_ONLY) {
      return FLUXNET2015_SOURCE_PRIORITY;
    }
    if (
      sourceOrigin === AMERIFLUX_API_SOURCE_ORIGIN ||
      sourceLabel === AMERIFLUX_SOURCE_ONLY ||
      sourceLabel === BASE_SOURCE_ONLY ||
      dataProduct === AMERIFLUX_BASE_PRODUCT
    ) {
      return AMERIFLUX_SOURCE_PRIORITY;
    }
    return SHUTTLE_SOURCE_PRIORITY;
  }

  function isShuttleCatalogRow(row) {
    return resolveSourceOrigin(row) === SHUTTLE_SOURCE_ORIGIN && String(row && row.download_mode || "").trim() !== "ameriflux_api";
  }

  function yearRangeLabel(firstYear, lastYear) {
    if (firstYear && lastYear) {
      return String(firstYear) + "-" + String(lastYear);
    }
    if (firstYear) {
      return String(firstYear) + "-";
    }
    if (lastYear) {
      return "-" + String(lastYear);
    }
    return "\u2014";
  }

  function calculateCoverageLength(firstYear, lastYear) {
    var start = parseIntOrNull(firstYear);
    var end = parseIntOrNull(lastYear);
    if (start == null || end == null || end < start) {
      return null;
    }
    return (end - start) + 1;
  }

  function buildRowSearchText(row) {
    var networkDisplay = String(row && (row.network_display || row.network || row.source_network) || "").trim();
    var surfacedSummary = buildSurfacedCoverageSummary(row && row.surfacedProducts);
    var classification = String(row && row.surfacedProductClassification || "").trim();
    var availabilityLabels = Array.isArray(row && row.availability_filter_labels) ? row.availability_filter_labels.join(" ") : "";
    var sourceTags = Array.isArray(row && row.source_filter_tags) ? row.source_filter_tags.join(" ") : "";
    var fluxList = String(row && row.flux_list || "").trim();
    var accessLabel = String(row && row.access_label || "").trim();
    var dataUseLabel = String(row && row.data_use_label || "").trim();
    return (
      String(row && row.site_id || "") + " " +
      String(row && row.site_name || "") + " " +
      String(row && row.country || "") + " " +
      networkDisplay + " " +
      String(row && row.source_network || "") + " " +
      String(row && row.vegetation_type || "") + " " +
      String(row && row.source_label || "") + " " +
      String(row && row.primarySourceLabel || "") + " " +
      String(row && row.source_filter || "") + " " +
      sourceTags + " " +
      availabilityLabels + " " +
      fluxList + " " +
      accessLabel + " " +
      dataUseLabel + " " +
      String(row && row.years || "") + " " +
      surfacedSummary + " " +
      classification
    ).toLowerCase();
  }

  function applyRowSourceFilterState(row) {
    if (!row || typeof row !== "object") {
      return row;
    }
    row.primarySourceLabel = primarySourceLabel(row);
    row.source_provenance_filter = row.primarySourceLabel;
    row.source_filter = row.primarySourceLabel;
    row.source_filter_tags = computeSourceFilterTags(row);
    row.availability_filter_labels = availabilityFilterLabels(row);
    return row;
  }

  function finalizeRowComputedState(row) {
    if (!row || typeof row !== "object") {
      return row;
    }
    row.country = deriveCountry(row.site_id, row.country);
    row.vegetation_type = normalizeVegetationType(row.vegetation_type);
    row.first_year = parseIntOrNull(row.first_year);
    row.last_year = parseIntOrNull(row.last_year);
    row.years = yearRangeLabel(row.first_year, row.last_year);
    row.length_years = calculateCoverageLength(row.first_year, row.last_year);
    normalizeNetworkDisplay(row);
    row.source_origin = resolveSourceOrigin(row);
    row.source_priority = resolveSourcePriority(row);
    row.product_family = inferRowProductFamily(row);
    row.processing_lineage = resolveProcessingLineage(row);
    applyRowSourceFilterState(row);
    row.is_icos = isIcosRow(row);
    row.has_coordinates = parseCoordinate(row.latitude, -90, 90) != null &&
      parseCoordinate(row.longitude, -180, 180) != null;
    row.search_text = buildRowSearchText(row);
    return row;
  }

  function stripUrlQueryForFilename(url) {
    return String(url || "").split("?")[0];
  }

  function filenameFromUrl(url) {
    var cleanUrl = stripUrlQueryForFilename(url).trim();
    if (!cleanUrl) {
      return "";
    }
    var idx = cleanUrl.lastIndexOf("/");
    if (idx < 0) {
      return cleanUrl;
    }
    return cleanUrl.slice(idx + 1);
  }

  function shellSingleQuote(value) {
    return String(value == null ? "" : value).replace(/'/g, "'\"'\"'");
  }

  function shellDoubleQuote(value) {
    return String(value == null ? "" : value)
      .replace(/\\/g, "\\\\")
      .replace(/"/g, "\\\"")
      .replace(/\$/g, "\\$")
      .replace(/`/g, "\\`");
  }

  function rowNetworkTokens(row) {
    var seen = {};
    var tokens = [];

    function addTokens(value) {
      normalizeNetworkTokens(value).forEach(function (token) {
        var key = String(token || "").toLowerCase();
        if (!token || seen[key]) {
          return;
        }
        seen[key] = true;
        tokens.push(token);
      });
    }

    if (Array.isArray(row && row.network_tokens)) {
      row.network_tokens.forEach(function (token) {
        addTokens(token);
      });
    }
    addTokens(row && row.network_display);
    addTokens(row && row.network);
    addTokens(row && row.source_network);
    return tokens;
  }

  function rowHasNetworkToken(row, expected) {
    var target = String(expected || "").trim().toLowerCase();
    if (!target) {
      return false;
    }
    return rowNetworkTokens(row).some(function (token) {
      return String(token || "").toLowerCase() === target;
    });
  }

  function isFluxnet2015SupplementalRow(row) {
    var sourceLabel = String(row && row.source_label || "").trim();
    var dataProduct = getApiRowDataProduct(row);
    return sourceLabel === FLUXNET2015_SOURCE_ONLY ||
      dataProduct === FLUXNET2015_PRODUCT ||
      rowHasNetworkToken(row, FLUXNET2015_SOURCE_ONLY);
  }

  function isAmeriFluxNetworkRow(row) {
    var sourceLabel = String(row && row.source_label || "").trim();
    var sourceOrigin = resolveSourceOrigin(row);
    var dataHub = String(row && row.data_hub || "").trim().toLowerCase();
    var dataProduct = getApiRowDataProduct(row);
    if (
      sourceLabel === AMERIFLUX_SOURCE_ONLY ||
      sourceLabel === AMERIFLUX_SHUTTLE ||
      sourceLabel === BASE_SOURCE_ONLY ||
      dataProduct === AMERIFLUX_BASE_PRODUCT ||
      rowHasNetworkToken(row, AMERIFLUX_SOURCE_ONLY)
    ) {
      return true;
    }
    if (isFluxnet2015SupplementalRow(row)) {
      return false;
    }
    return dataHub === "ameriflux" || sourceOrigin === AMERIFLUX_API_SOURCE_ORIGIN;
  }

  function isIcosNetworkRow(row) {
    return rowHasNetworkToken(row, ICOS_DIRECT_SOURCE_ONLY) ||
      String(row && row.source_label || "").trim() === ICOS_DIRECT_SOURCE_ONLY ||
      String(row && row.data_hub || "").trim().toLowerCase() === "icos" ||
      isIcosRow(row);
  }

  function isTernNetworkRow(row) {
    return rowHasNetworkToken(row, SOURCE_FILTER_TAG_TERN) ||
      String(row && row.source_label || "").trim() === SOURCE_FILTER_TAG_TERN ||
      String(row && row.data_hub || "").trim().toLowerCase() === "tern";
  }

  function isChinaFluxNetworkRow(row) {
    return rowHasNetworkToken(row, SOURCE_FILTER_TAG_CHINAFLUX) ||
      String(row && row.source_label || "").trim() === SOURCE_FILTER_TAG_CHINAFLUX ||
      String(row && row.data_hub || "").trim().toLowerCase() === "chinaflux";
  }

  function computeSourceFilterTags(row) {
    var tags = [];
    var seen = {};
    var shuttleAvailable = isShuttleCatalogRow(row);
    var ameriFluxNetwork = isAmeriFluxNetworkRow(row);
    var chinaFluxNetwork = isChinaFluxNetworkRow(row);
    var icosNetwork = isIcosNetworkRow(row);
    var ternNetwork = isTernNetworkRow(row);
    var fluxnet2015Supplemental = isFluxnet2015SupplementalRow(row);

    function addTag(tag) {
      if (!tag || seen[tag]) {
        return;
      }
      seen[tag] = true;
      tags.push(tag);
    }

    if (isEfdSourceRow(row)) {
      addTag(SOURCE_FILTER_TAG_EFD);
      return tags;
    }

    if (ameriFluxNetwork) {
      addTag(SOURCE_FILTER_TAG_AMERIFLUX);
      if (shuttleAvailable) {
        addTag(SOURCE_FILTER_TAG_AMERIFLUX_SHUTTLE);
      }
    }
    if (chinaFluxNetwork) {
      addTag(SOURCE_FILTER_TAG_CHINAFLUX);
    }
    if (icosNetwork) {
      addTag(SOURCE_FILTER_TAG_ICOS);
      if (shuttleAvailable) {
        addTag(SOURCE_FILTER_TAG_ICOS_SHUTTLE);
      }
    }
    if (isJapanFluxSourceRow(row)) {
      addTag(SOURCE_FILTER_TAG_JAPANFLUX);
    }
    if (ternNetwork) {
      addTag(SOURCE_FILTER_TAG_TERN);
      if (shuttleAvailable) {
        addTag(SOURCE_FILTER_TAG_TERN_SHUTTLE);
      }
    }
    if (fluxnet2015Supplemental) {
      addTag(SOURCE_FILTER_TAG_FLUXNET_2015);
    }
    if (shuttleAvailable) {
      addTag(SOURCE_FILTER_TAG_FLUXNET_SHUTTLE);
    }
    return tags;
  }

  function sourceFilterTags(row) {
    if (Array.isArray(row && row.source_filter_tags) && row.source_filter_tags.length) {
      return row.source_filter_tags.slice();
    }
    return computeSourceFilterTags(row);
  }

  function primarySourceLabel(row) {
    var explicit = String(row && (row.primarySourceLabel || row.source_provenance_filter) || "").trim();
    var shuttleAvailable;
    if (explicit) {
      return explicit;
    }
    shuttleAvailable = isShuttleCatalogRow(row);
    if (isEfdSourceRow(row)) {
      return SOURCE_FILTER_TAG_EFD;
    }
    if (isAmeriFluxNetworkRow(row)) {
      return shuttleAvailable ? SOURCE_FILTER_TAG_AMERIFLUX_SHUTTLE : SOURCE_FILTER_TAG_AMERIFLUX;
    }
    if (isChinaFluxNetworkRow(row)) {
      return SOURCE_FILTER_TAG_CHINAFLUX;
    }
    if (isIcosNetworkRow(row)) {
      return shuttleAvailable ? SOURCE_FILTER_TAG_ICOS_SHUTTLE : SOURCE_FILTER_TAG_ICOS;
    }
    if (isJapanFluxSourceRow(row)) {
      return SOURCE_FILTER_TAG_JAPANFLUX;
    }
    if (isTernNetworkRow(row)) {
      return shuttleAvailable ? SOURCE_FILTER_TAG_TERN_SHUTTLE : SOURCE_FILTER_TAG_TERN;
    }
    if (isFluxnet2015SupplementalRow(row)) {
      return SOURCE_FILTER_TAG_FLUXNET_2015;
    }
    if (shuttleAvailable) {
      return SOURCE_FILTER_TAG_FLUXNET_SHUTTLE;
    }
    return String(row && row.source_label || "").trim() || SHUTTLE_SOURCE;
  }

  function sourceFilterValue(row) {
    return primarySourceLabel(row);
  }

  function uniqueSourceFilterValues() {
    return SOURCE_FILTER_OPTIONS.slice();
  }

  function normalizeProcessingLineage(lineage) {
    var normalized = String(lineage || "").trim().toLowerCase();
    if (normalized === PROCESSING_LINEAGE_ONEFLUX) {
      return PROCESSING_LINEAGE_ONEFLUX;
    }
    if (normalized === PROCESSING_LINEAGE_OTHER || normalized === "other") {
      return PROCESSING_LINEAGE_OTHER;
    }
    return "";
  }

  function resolveProcessingLineage(fields) {
    // Prefer explicit snapshot lineage. The source/product checks below are a
    // temporary compatibility fallback for legacy payloads that predate the field.
    var explicit = normalizeProcessingLineage(fields && (fields.processingLineage || fields.processing_lineage));
    var sourceOrigin = String(fields && (fields.sourceOrigin || fields.source_origin || fields.source) || "").trim();
    var sourceLabel = String(fields && (fields.sourceLabel || fields.source_label) || "").trim();
    var dataHub = String(fields && (fields.dataHub || fields.data_hub) || "").trim();
    var dataProduct = normalizeDownloadProduct(fields && (fields.apiDataProduct || fields.api_data_product));
    var productFamily = String(fields && (fields.productFamily || fields.product_family) || "").trim().toUpperCase();
    var downloadMode = String(fields && (fields.downloadMode || fields.download_mode) || "").trim();
    if (explicit) {
      return explicit;
    }
    if (sourceOrigin === EFD_SOURCE_ORIGIN || sourceLabel === SOURCE_FILTER_TAG_EFD || dataHub === SOURCE_FILTER_TAG_EFD || downloadMode === REQUEST_PAGE_DOWNLOAD_MODE) {
      return "";
    }
    if (
      sourceOrigin === JAPANFLUX_DIRECT_SOURCE_ORIGIN ||
      sourceLabel === SOURCE_FILTER_TAG_JAPANFLUX ||
      dataHub === SOURCE_FILTER_TAG_JAPANFLUX ||
      dataProduct === AMERIFLUX_BASE_PRODUCT ||
      productFamily === PRODUCT_FAMILY_BASE ||
      productFamily === PRODUCT_FAMILY_ICOS_ETC
    ) {
      return PROCESSING_LINEAGE_OTHER;
    }
    return PROCESSING_LINEAGE_ONEFLUX;
  }

  function processingLineageForProduct(product) {
    return normalizeProcessingLineage(product && (product.processingLineage || product.processing_lineage)) ||
      resolveProcessingLineage(product || {});
  }

  function buildProductYearLookup(products, targetLineage) {
    var years = {};
    (Array.isArray(products) ? products : []).forEach(function (product) {
      if (processingLineageForProduct(product) !== targetLineage) {
        return;
      }
      normalizedExactYears(
        product && (product.exactYears || product.exact_years),
        product && (product.firstYear || product.first_year),
        product && (product.lastYear || product.last_year)
      ).forEach(function (year) {
        years[year] = true;
      });
    });
    return years;
  }

  function hasAdditionalOtherProcessedYears(products) {
    var oneFluxYears = buildProductYearLookup(products, PROCESSING_LINEAGE_ONEFLUX);
    var otherYears = buildProductYearLookup(products, PROCESSING_LINEAGE_OTHER);
    var hasOneFlux = Object.keys(oneFluxYears).length > 0;
    var hasOther = Object.keys(otherYears).length > 0;
    if (!hasOneFlux || !hasOther) {
      return false;
    }
    return Object.keys(otherYears).some(function (year) {
      return !oneFluxYears[year];
    });
  }

  function classifySurfacedProducts(products) {
    var lineages = {};
    (Array.isArray(products) ? products : []).forEach(function (product) {
      var lineage = processingLineageForProduct(product);
      if (lineage) {
        lineages[lineage] = true;
      }
    });

    if (lineages[PROCESSING_LINEAGE_ONEFLUX] && lineages[PROCESSING_LINEAGE_OTHER]) {
      return hasAdditionalOtherProcessedYears(products)
        ? SURFACED_CLASSIFICATION_FLUXNET_AND_OTHER
        : SURFACED_CLASSIFICATION_FLUXNET_PROCESSED;
    }
    if (lineages[PROCESSING_LINEAGE_ONEFLUX]) {
      return SURFACED_CLASSIFICATION_FLUXNET_PROCESSED;
    }
    if (lineages[PROCESSING_LINEAGE_OTHER]) {
      return SURFACED_CLASSIFICATION_OTHER_PROCESSED;
    }
    return "";
  }

  function availabilityFilterLabels(row) {
    var classification = String(row && row.surfacedProductClassification || "").trim();
    if (classification === SURFACED_CLASSIFICATION_FLUXNET_PROCESSED) {
      return [FILTER_LABEL_FLUXNET_PROCESSED];
    }
    if (classification === SURFACED_CLASSIFICATION_OTHER_PROCESSED) {
      return [FILTER_LABEL_OTHER_PROCESSED];
    }
    if (classification === SURFACED_CLASSIFICATION_FLUXNET_AND_OTHER) {
      return [FILTER_LABEL_FLUXNET_AND_OTHER];
    }
    return [];
  }

  function uniqueAvailabilityFilterValues(rows) {
    var available = {};
    (rows || []).forEach(function (row) {
      availabilityFilterLabels(row).forEach(function (label) {
        available[label] = true;
      });
    });
    return [
      FILTER_LABEL_FLUXNET_PROCESSED,
      FILTER_LABEL_OTHER_PROCESSED,
      FILTER_LABEL_FLUXNET_AND_OTHER
    ].filter(function (label) {
      return !!available[label];
    });
  }

  function hasFluxnetAvailability(row) {
    var classification = String(row && row.surfacedProductClassification || "").trim();
    if (typeof (row && row.hasFluxnetAvailable) === "boolean") {
      return row.hasFluxnetAvailable;
    }
    return classification === SURFACED_CLASSIFICATION_FLUXNET_PROCESSED ||
      classification === SURFACED_CLASSIFICATION_FLUXNET_AND_OTHER;
  }

  function availabilityFilterMatches(row, selectedAvailability) {
    var selected = String(selectedAvailability || "").trim();
    if (!selected) {
      return true;
    }
    if (selected === FILTER_LABEL_FLUXNET_PROCESSED) {
      return hasFluxnetAvailability(row);
    }
    return availabilityFilterLabels(row).indexOf(selected) !== -1;
  }

  function rowMatchesExplorerFilters(row, filters) {
    var opts = filters || {};
    var search = String(opts.search || "").trim().toLowerCase();
    var selectedNetwork = String(opts.selectedNetwork || "");
    var selectedSource = String(opts.selectedSource || "");
    var selectedAvailability = String(opts.selectedAvailability || "");
    var selectedCountry = String(opts.selectedCountry || "");
    var selectedVegetation = String(opts.selectedVegetation || "");
    var selectedHubs = opts.selectedHubs && typeof opts.selectedHubs === "object" ? opts.selectedHubs : {};

    if (search && String(row && row.search_text || "").indexOf(search) === -1) {
      return false;
    }
    if (selectedNetwork && (!row || row.network_tokens.indexOf(selectedNetwork) === -1)) {
      return false;
    }
    if (selectedSource && sourceFilterTags(row).indexOf(selectedSource) === -1) {
      return false;
    }
    if (!availabilityFilterMatches(row, selectedAvailability)) {
      return false;
    }
    if (selectedCountry && String(row && row.country || "") !== selectedCountry) {
      return false;
    }
    if (selectedVegetation && String(row && row.vegetation_type || "") !== selectedVegetation) {
      return false;
    }
    if (Object.keys(selectedHubs).length && !selectedHubs[row.data_hub]) {
      return false;
    }
    return true;
  }

  function uniqueVegetationFilterValues(rows) {
    var seen = {};
    var values = [];
    (rows || []).forEach(function (row) {
      var value = String(row && row.vegetation_type || "").trim();
      if (!value || seen[value]) {
        return;
      }
      seen[value] = true;
      values.push(value);
    });
    return values.sort();
  }

  function buildVegetationFilterOptions(rows) {
    return uniqueVegetationFilterValues(rows)
      .map(function (vegetationType) {
        return {
          value: vegetationType,
          label: vegetationDisplayLabel(vegetationType)
        };
      })
      .sort(function (a, b) {
        var byLabel = a.label.localeCompare(b.label);
        return byLabel || a.value.localeCompare(b.value);
      });
  }

  function getApiRowDataProduct(row) {
    var value = String(row && row.api_data_product || "").trim().toUpperCase();
    if (value === AMERIFLUX_BASE_PRODUCT || value === PRODUCT_FAMILY_BASE) {
      return AMERIFLUX_BASE_PRODUCT;
    }
    if (value === FLUXNET2015_PRODUCT) {
      return FLUXNET2015_PRODUCT;
    }
    return AMERIFLUX_FLUXNET_PRODUCT;
  }

  function apiProductDisplayName(dataProduct) {
    var normalized = normalizeDownloadProduct(dataProduct);
    if (normalized === AMERIFLUX_BASE_PRODUCT) {
      return BASE_SOURCE_ONLY;
    }
    if (normalized === FLUXNET2015_PRODUCT) {
      return FLUXNET2015_SOURCE_ONLY;
    }
    return PRODUCT_FAMILY_FLUXNET;
  }

  function productFamilyDisplayName(productFamily, useLongLabel) {
    var family = normalizeProductFamily(productFamily);
    if (family === PRODUCT_FAMILY_BASE) {
      return useLongLabel ? "BASE (standardized observations)" : PRODUCT_FAMILY_BASE;
    }
    if (family === PRODUCT_FAMILY_ICOS_ETC) {
      return useLongLabel ? "ICOS ETC L2 archive" : "ICOS ETC";
    }
    return useLongLabel ? "FLUXNET (ONEFlux-derived)" : PRODUCT_FAMILY_FLUXNET;
  }

  function surfacedProductDisplayName(product, useLongLabel) {
    if (String(product && product.sourceLabel || "").trim() === SOURCE_FILTER_TAG_JAPANFLUX ||
      String(product && product.source_label || "").trim() === SOURCE_FILTER_TAG_JAPANFLUX ||
      String(product && product.sourceOrigin || product && product.source_origin || "").trim() === JAPANFLUX_DIRECT_SOURCE_ORIGIN) {
      return useLongLabel ? "JapanFlux2024" : SOURCE_FILTER_TAG_JAPANFLUX;
    }
    return productFamilyDisplayName(product && product.productFamily, useLongLabel);
  }

  function getApiActionCopyLabel(dataProduct) {
    return "Copy " + apiProductDisplayName(dataProduct) + " curl command";
  }

  function getApiActionRequestLabel(dataProduct) {
    return "Request " + apiProductDisplayName(dataProduct) + " URL";
  }

  function getApiActionPreparingLabel(dataProduct, canDirectDownload) {
    var productName = apiProductDisplayName(dataProduct);
    return canDirectDownload
      ? ("Requesting " + productName + " URL…")
      : ("Preparing " + productName + " command…");
  }

  function normalizeDownloadProduct(dataProduct) {
    var normalized = String(dataProduct || "").trim().toUpperCase();
    if (normalized === FLUXNET2015_PRODUCT) {
      return FLUXNET2015_PRODUCT;
    }
    if (normalized === AMERIFLUX_BASE_PRODUCT || normalized === PRODUCT_FAMILY_BASE || normalized === BASE_SOURCE_ONLY) {
      return AMERIFLUX_BASE_PRODUCT;
    }
    return AMERIFLUX_FLUXNET_PRODUCT;
  }

  // AmeriFlux currently supports FLUXNET downloads through v2, while FLUXNET2015
  // remains on the legacy v1 download API.
  function getDownloadEndpointForProduct(dataProduct) {
    return normalizeDownloadProduct(dataProduct) === FLUXNET2015_PRODUCT
      ? AMERIFLUX_V1_DOWNLOAD_URL
      : AMERIFLUX_V2_DOWNLOAD_URL;
  }

  function buildAmeriFluxDownloadDescription(dataProduct, siteIds) {
    var product = normalizeDownloadProduct(dataProduct);
    var sites = Array.isArray(siteIds) ? siteIds.filter(Boolean) : [];
    var siteLabel = sites.length ? sites.join(", ") : "SITE_ID_HERE";
    return "Request " + product + " download for " + siteLabel + " via the Q.E.D. Lab FLUXNET Data Explorer for Keenan Group research workflows.";
  }

  function buildV2DownloadPayload(siteIds, variant, policy, identity, dataProduct) {
    var sites = Array.isArray(siteIds) ? siteIds.filter(Boolean) : [];
    var product = normalizeDownloadProduct(dataProduct);
    return {
      user_id: String(identity && identity.user_id || "").trim(),
      user_email: String(identity && identity.user_email || "").trim(),
      data_policy: String(policy || AMERIFLUX_DEFAULT_POLICY),
      data_product: product,
      data_variant: String(variant || AMERIFLUX_DEFAULT_VARIANT),
      site_ids: sites,
      intended_use: AMERIFLUX_V2_INTENDED_USE,
      description: buildAmeriFluxDownloadDescription(product, sites)
    };
  }

  function buildV1DownloadPayload(siteIds, variant, policy, identity, dataProduct) {
    var sites = Array.isArray(siteIds) ? siteIds.filter(Boolean) : [];
    var product = normalizeDownloadProduct(dataProduct);
    return {
      user_id: String(identity && identity.user_id || "").trim(),
      user_email: String(identity && identity.user_email || "").trim(),
      data_product: product,
      data_variant: String(variant || AMERIFLUX_DEFAULT_VARIANT),
      data_policy: String(policy || AMERIFLUX_DEFAULT_POLICY),
      site_ids: sites,
      intended_use: AMERIFLUX_V1_INTENDED_USE,
      description: "Download " + product + " for " + sites.join(", "),
      agree_policy: true
    };
  }

  function buildDownloadPayloadForProduct(siteIds, variant, policy, identity, dataProduct) {
    return normalizeDownloadProduct(dataProduct) === FLUXNET2015_PRODUCT
      ? buildV1DownloadPayload(siteIds, variant, policy, identity, dataProduct)
      : buildV2DownloadPayload(siteIds, variant, policy, identity, dataProduct);
  }

  function buildAmeriFluxCurlCommand(siteId, variant, policy, endpointUrl, dataProduct, identityOverride) {
    var site = String(siteId || "").trim();
    var product = normalizeDownloadProduct(dataProduct);
    var resolvedIdentity = resolveAmeriFluxIdentityOverride(identityOverride);
    var payload = buildDownloadPayloadForProduct(
      [site || "SITE_ID_HERE"],
      variant,
      policy,
      resolvedIdentity
        ? {
          user_id: resolvedIdentity.user_id,
          user_email: resolvedIdentity.user_email
        }
        : {
          user_id: AMERIFLUX_TEMPLATE_USER_ID,
          user_email: AMERIFLUX_TEMPLATE_USER_EMAIL
        },
      product
    );
    var payloadJson = JSON.stringify(payload, null, 2);
    return [
      "curl -sS -X POST \"" + String(endpointUrl || getDownloadEndpointForProduct(product)) + "\" \\",
      "  -H \"Content-Type: application/json\" \\",
      "  -H \"accept: application/json\" \\",
      "  --data-binary '" + shellSingleQuote(payloadJson) + "' \\",
      "| tee download_response.json \\",
      "| jq -r '.data_urls[].url' | while read -r url; do",
      "  clean_url=\"${url%%\\?*}\"",
      "  filename=\"$(basename \"$clean_url\")\"",
      "  curl -L \"$url\" -o \"$filename\"",
      "done"
    ].join("\n");
  }

  function uniqueSiteIdsFromRows(rows) {
    var seen = {};
    var siteIds = [];
    (rows || []).forEach(function (row) {
      var siteId = String(row && row.site_id || "").trim();
      if (!siteId || seen[siteId]) {
        return;
      }
      seen[siteId] = true;
      siteIds.push(siteId);
    });
    return siteIds;
  }

  function selectedSiteIdsText(siteIds) {
    var ids = Array.isArray(siteIds) ? siteIds.filter(Boolean) : [];
    return ids.join("\n") + "\n";
  }

  function normalizeAmeriFluxBulkEntries(entries) {
    var seen = {};
    var out = [];
    (Array.isArray(entries) ? entries : []).forEach(function (entry) {
      var siteId;
      var dataProduct;
      var sourceLabel;
      var dedupeKey;
      if (typeof entry === "string") {
        siteId = String(entry || "").trim();
        dataProduct = AMERIFLUX_FLUXNET_PRODUCT;
        sourceLabel = AMERIFLUX_SOURCE_ONLY;
      } else {
        siteId = String(entry && (entry.site_id || entry.siteId) || "").trim();
        dataProduct = String(entry && (entry.data_product || entry.dataProduct || entry.api_data_product || entry.apiDataProduct) || AMERIFLUX_FLUXNET_PRODUCT).trim().toUpperCase();
        sourceLabel = String(entry && (entry.source_label || entry.sourceLabel) || "").trim();
      }
      if (!siteId) {
        return;
      }
      if (dataProduct !== FLUXNET2015_PRODUCT && dataProduct !== AMERIFLUX_BASE_PRODUCT) {
        dataProduct = AMERIFLUX_FLUXNET_PRODUCT;
      }
      if (!sourceLabel) {
        sourceLabel = dataProduct === FLUXNET2015_PRODUCT
          ? FLUXNET2015_SOURCE_ONLY
          : (dataProduct === AMERIFLUX_BASE_PRODUCT ? BASE_SOURCE_ONLY : AMERIFLUX_SOURCE_ONLY);
      }
      dedupeKey = siteId + "|" + dataProduct;
      if (seen[dedupeKey]) {
        return;
      }
      seen[dedupeKey] = true;
      out.push({
        site_id: siteId,
        data_product: dataProduct,
        source_label: sourceLabel
      });
    });
    return out;
  }

  function buildAmeriFluxSelectedSitesText(entries) {
    var normalized = normalizeAmeriFluxBulkEntries(entries);
    var lines = ["# site_id\tdata_product\tsource_label"];
    if (!normalized.length) {
      lines.push("# AR-Bal\tFLUXNET\tAmeriFlux");
    } else {
      normalized.forEach(function (entry) {
        lines.push([
          String(entry.site_id || ""),
          String(entry.data_product || AMERIFLUX_FLUXNET_PRODUCT),
          String(entry.source_label || "")
        ].join("\t"));
      });
    }
    return lines.join("\n") + "\n";
  }

  function buildManualActionProduct(row) {
    if (!row) {
      return null;
    }
    return {
      displayLabel: String(row.source_label || row.data_hub || "Request").trim() || "Request",
      downloadMode: String(row.download_mode || "").trim(),
      download_mode: String(row.download_mode || "").trim(),
      downloadLink: String(row.download_link || "").trim(),
      download_link: String(row.download_link || "").trim(),
      sourceLabel: String(row.source_label || "").trim(),
      source_label: String(row.source_label || "").trim(),
      sourceOrigin: resolveSourceOrigin(row),
      source_origin: resolveSourceOrigin(row),
      sourceReason: String(row.source_reason || "").trim(),
      source_reason: String(row.source_reason || "").trim(),
      siteId: String(row.site_id || "").trim(),
      site_id: String(row.site_id || "").trim(),
      coverageLabel: String(row.years || "").trim(),
      years: String(row.years || "").trim()
    };
  }

  function getRowActionProducts(row) {
    var products = getSurfacedProductsForRow(row);
    if (products.length) {
      return products;
    }
    if (row && String(row.download_mode || "").trim() && String(row.download_mode || "").trim() !== "direct") {
      return [buildManualActionProduct(row)].filter(Boolean);
    }
    return [];
  }

  function flattenActionProducts(rows) {
    var products = [];
    (rows || []).forEach(function (row) {
      getRowActionProducts(row).forEach(function (product) {
        products.push(Object.assign({}, product, {
          parentSelectionKey: String(row && row._selection_key || "")
        }));
      });
    });
    return products;
  }

  function partitionRowsByBulkSource(rows) {
    var shuttleRows = [];
    var shuttleDownloadRows = [];
    var manualLandingPageRows = [];
    var requestOnlyRows = [];
    var ameriFluxRows = [];
    flattenActionProducts(rows).forEach(function (product) {
      if (product.download_mode === "ameriflux_api") {
        ameriFluxRows.push(product);
        return;
      }
      if (product.download_mode === REQUEST_PAGE_DOWNLOAD_MODE) {
        requestOnlyRows.push(product);
        return;
      }
      if (product.download_mode === LANDING_PAGE_DOWNLOAD_MODE) {
        manualLandingPageRows.push(product);
        shuttleRows.push(product);
        return;
      }
      shuttleDownloadRows.push(product);
      shuttleRows.push(product);
    });
    return {
      shuttleRows: shuttleRows,
      shuttleDownloadRows: shuttleDownloadRows,
      manualLandingPageRows: manualLandingPageRows,
      requestOnlyRows: requestOnlyRows,
      ameriFluxRows: ameriFluxRows
    };
  }

  function summarizeBulkSelection(rows) {
    var partition = partitionRowsByBulkSource(rows);
    var shuttleCount = uniqueSiteIdsFromRows(partition.shuttleRows).length;
    var shuttleDownloadCount = uniqueSiteIdsFromRows(partition.shuttleDownloadRows).length;
    var manualLandingPageCount = uniqueSiteIdsFromRows(partition.manualLandingPageRows).length;
    var requestOnlyCount = uniqueSiteIdsFromRows(partition.requestOnlyRows).length;
    var ameriFluxCount = uniqueSiteIdsFromRows(partition.ameriFluxRows).length;
    return {
      shuttleRows: partition.shuttleRows,
      shuttleDownloadRows: partition.shuttleDownloadRows,
      manualLandingPageRows: partition.manualLandingPageRows,
      requestOnlyRows: partition.requestOnlyRows,
      ameriFluxRows: partition.ameriFluxRows,
      shuttleCount: shuttleCount,
      shuttleDownloadCount: shuttleDownloadCount,
      manualLandingPageCount: manualLandingPageCount,
      requestOnlyCount: requestOnlyCount,
      ameriFluxCount: ameriFluxCount,
      showAllSelectedActions: shuttleDownloadCount > 0 || ameriFluxCount > 0,
      showShuttleSection: shuttleCount > 0,
      showAmeriFluxSection: ameriFluxCount > 0
    };
  }

  function buildAmeriFluxBulkScriptText(siteEntries, options) {
    var opts = options || {};
    var entries = normalizeAmeriFluxBulkEntries(siteEntries);
    var embeddedSites = buildAmeriFluxSelectedSitesText(entries).replace(/\n$/, "");
    var defaultUserId = shellDoubleQuote(String(opts.defaultUserId || AMERIFLUX_BULK_FALLBACK_USER_ID));
    var defaultUserEmail = shellDoubleQuote(String(opts.defaultUserEmail || AMERIFLUX_BULK_FALLBACK_USER_EMAIL));
    var v2DownloadUrl = shellDoubleQuote(String(opts.v2DownloadUrl || AMERIFLUX_V2_DOWNLOAD_URL));
    var v1DownloadUrl = shellDoubleQuote(String(opts.v1DownloadUrl || AMERIFLUX_V1_DOWNLOAD_URL));
    var variant = shellDoubleQuote(String(opts.variant || AMERIFLUX_DEFAULT_VARIANT));
    var policy = shellDoubleQuote(String(opts.policy || AMERIFLUX_DEFAULT_POLICY));
    var v2IntendedUse = shellDoubleQuote(String(opts.v2IntendedUse || AMERIFLUX_V2_INTENDED_USE));
    var v1IntendedUse = shellDoubleQuote(String(opts.v1IntendedUse || AMERIFLUX_V1_INTENDED_USE));

    return [
      "#!/usr/bin/env bash",
      "set -euo pipefail",
      "",
      "OUTDIR=\"${1:-ameriflux_downloads}\"",
      "SITES_FILE=\"${2:-ameriflux_selected_sites.txt}\"",
      "LOGFILE=\"${3:-ameriflux_bulk_download.log}\"",
      "USER_ID=\"${AMERIFLUX_USER_ID:-" + defaultUserId + "}\"",
      "USER_EMAIL=\"${AMERIFLUX_USER_EMAIL:-" + defaultUserEmail + "}\"",
      "V2_DOWNLOAD_URL=\"${AMERIFLUX_V2_DOWNLOAD_URL:-" + v2DownloadUrl + "}\"",
      "V1_DOWNLOAD_URL=\"${AMERIFLUX_V1_DOWNLOAD_URL:-" + v1DownloadUrl + "}\"",
      "",
      "mkdir -p \"$OUTDIR\"",
      "cd \"$OUTDIR\"",
      ": > \"$LOGFILE\"",
      "",
      "print_jq_install_guidance() {",
      "  local kernel distro",
      "  kernel=\"$(uname -s 2>/dev/null || printf '')\"",
      "  case \"$kernel\" in",
      "    Darwin)",
      "      echo \"macOS: brew install jq\" >&2",
      "      return",
      "      ;;",
      "    MINGW*|MSYS*|CYGWIN*)",
      "      echo \"Windows shells:\" >&2",
      "      echo \"  choco install jq\" >&2",
      "      echo \"  scoop install jq\" >&2",
      "      echo \"  winget install jqlang.jq\" >&2",
      "      return",
      "      ;;",
      "    Linux)",
      "      distro=\"\"",
      "      if [ -r /etc/os-release ]; then",
      "        distro=\"$(. /etc/os-release && printf '%s' \"${ID:-}\")\"",
      "      fi",
      "      case \"$distro\" in",
      "        debian|ubuntu)",
      "          echo \"Debian/Ubuntu: sudo apt-get install jq\" >&2",
      "          return",
      "          ;;",
      "        fedora|rhel)",
      "          echo \"Fedora/RHEL: sudo dnf install jq\" >&2",
      "          return",
      "          ;;",
      "        arch)",
      "          echo \"Arch: sudo pacman -S jq\" >&2",
      "          return",
      "          ;;",
      "      esac",
      "      ;;",
      "  esac",
      "  echo \"See https://jqlang.github.io/jq/download/\" >&2",
      "}",
      "",
      "extract_urls() {",
      "  if command -v jq >/dev/null 2>&1; then",
      "    printf '%s' \"$1\" | jq -r '.data_urls[]?.url // empty'",
      "    return",
      "  fi",
      "",
      "  if command -v python3 >/dev/null 2>&1; then",
      "    printf '%s' \"$1\" | python3 -c '",
      "import json",
      "import sys",
      "data = json.load(sys.stdin)",
      "for item in data.get(\"data_urls\", []):",
      "    url = item.get(\"url\")",
      "    if url:",
      "        print(url)",
      "'",
      "    return",
      "  fi",
      "",
      "  echo \"This script requires jq or python3 to parse the AmeriFlux API response.\" >&2",
      "  print_jq_install_guidance",
      "  return 1",
      "}",
      "",
      "if ! command -v jq >/dev/null 2>&1 && ! command -v python3 >/dev/null 2>&1; then",
      "  echo \"This script requires jq or python3 to parse the AmeriFlux API response.\" >&2",
      "  print_jq_install_guidance",
      "  exit 1",
      "fi",
      "",
      "if [ ! -f \"$SITES_FILE\" ]; then",
      "  cat > \"$SITES_FILE\" <<'AMERIFLUX_SITES'",
      embeddedSites,
      "AMERIFLUX_SITES",
      "fi",
      "",
      "if [ ! -s \"$SITES_FILE\" ]; then",
      "  echo \"No AmeriFlux API-backed sites provided in $SITES_FILE.\" | tee -a \"$LOGFILE\"",
      "  exit 0",
      "fi",
      "",
      "while IFS=$'\\t' read -r SITE_ID DATA_PRODUCT SOURCE_LABEL; do",
      "  SITE_ID=\"${SITE_ID%$'\\r'}\"",
      "  DATA_PRODUCT=\"${DATA_PRODUCT%$'\\r'}\"",
      "  SOURCE_LABEL=\"${SOURCE_LABEL%$'\\r'}\"",
      "  [ -n \"$SITE_ID\" ] || continue",
      "  case \"$SITE_ID\" in",
      "    \\#*) continue ;;",
      "  esac",
      "  if [ -z \"$DATA_PRODUCT\" ]; then",
      "    DATA_PRODUCT=\"" + AMERIFLUX_FLUXNET_PRODUCT + "\"",
      "  fi",
      "  if [ -z \"$SOURCE_LABEL\" ]; then",
      "    SOURCE_LABEL=\"" + AMERIFLUX_SOURCE_ONLY + "\"",
      "  fi",
      "  echo \"Requesting ${DATA_PRODUCT} URLs for ${SITE_ID} (${SOURCE_LABEL})...\" | tee -a \"$LOGFILE\"",
      "  REQUEST_URL=\"$V2_DOWNLOAD_URL\"",
      "  REQUEST_BODY=\"{",
      "      \\\"user_id\\\": \\\"${USER_ID}\\\",",
      "      \\\"user_email\\\": \\\"${USER_EMAIL}\\\",",
      "      \\\"data_policy\\\": \\\"" + policy + "\\\",",
      "      \\\"data_product\\\": \\\"${DATA_PRODUCT}\\\",",
      "      \\\"data_variant\\\": \\\"" + variant + "\\\",",
      "      \\\"site_ids\\\": [\\\"${SITE_ID}\\\"],",
      "      \\\"intended_use\\\": \\\"" + v2IntendedUse + "\\\",",
      "      \\\"description\\\": \\\"Request ${DATA_PRODUCT} download for ${SITE_ID} via the Q.E.D. Lab FLUXNET Data Explorer for Keenan Group research workflows.\\\"",
      "    }\"",
      "  if [ \"$DATA_PRODUCT\" = \"" + FLUXNET2015_PRODUCT + "\" ]; then",
      "    REQUEST_URL=\"$V1_DOWNLOAD_URL\"",
      "    REQUEST_BODY=\"{",
      "      \\\"user_id\\\": \\\"${USER_ID}\\\",",
      "      \\\"user_email\\\": \\\"${USER_EMAIL}\\\",",
      "      \\\"data_product\\\": \\\"${DATA_PRODUCT}\\\",",
      "      \\\"data_variant\\\": \\\"" + variant + "\\\",",
      "      \\\"data_policy\\\": \\\"" + policy + "\\\",",
      "      \\\"site_ids\\\": [\\\"${SITE_ID}\\\"],",
      "      \\\"intended_use\\\": \\\"" + v1IntendedUse + "\\\",",
      "      \\\"description\\\": \\\"Download ${DATA_PRODUCT} for ${SITE_ID}\\\",",
      "      \\\"agree_policy\\\": true",
      "    }\"",
      "  fi",
      "",
      "  RESPONSE=$(curl -sS -X POST \"$REQUEST_URL\" \\",
      "    -H \"Content-Type: application/json\" \\",
      "    -H \"accept: application/json\" \\",
      "    --data-binary \"$REQUEST_BODY\") || {",
      "      echo \"Request failed for ${SITE_ID}; skipping.\" | tee -a \"$LOGFILE\"",
      "      continue",
      "    }",
      "",
      "  URLS=$(extract_urls \"$RESPONSE\" 2>/dev/null || true)",
      "  if [ -z \"$URLS\" ]; then",
      "    echo \"No data_urls returned for ${SITE_ID} (${DATA_PRODUCT}); continuing.\" | tee -a \"$LOGFILE\"",
      "    continue",
      "  fi",
      "",
      "  while IFS= read -r url; do",
      "    [ -n \"$url\" ] || continue",
      "    clean_url=\"${url%%\\?*}\"",
      "    filename=\"$(basename \"$clean_url\")\"",
      "    echo \"Downloading ${filename} (${SITE_ID}, ${DATA_PRODUCT})\" | tee -a \"$LOGFILE\"",
      "    curl -L \"$url\" -o \"$filename\" || echo \"Download failed for ${SITE_ID} (${DATA_PRODUCT}): $url\" | tee -a \"$LOGFILE\"",
      "  done <<< \"$URLS\"",
      "done < \"$SITES_FILE\"",
      "",
      "echo \"AmeriFlux API bulk download complete.\" | tee -a \"$LOGFILE\""
    ].join("\n");
  }

  function buildDownloadAllSelectedScriptText(options) {
    var opts = options || {};
    var includeShuttle = opts.includeShuttle !== false;
    var includeAmeriFlux = opts.includeAmeriFlux !== false;
    var shuttleScript = String(opts.shuttleScript || "./download_shuttle_selected.sh");
    var ameriFluxScript = String(opts.ameriFluxScript || "./download_ameriflux_selected.sh");
    var lines = [
      "#!/usr/bin/env bash",
      "set -euo pipefail",
      "",
      "# Bulk download wrapper for surfaced products from selected FLUXNET sites",
      "# Validated direct links are handled by download_shuttle_selected.sh.",
      "# AmeriFlux API-backed surfaced products (FLUXNET, BASE, and FLUXNET2015) are downloaded via the AmeriFlux API.",
      "",
      "SCRIPT_DIR=\"$(cd \"$(dirname \"${BASH_SOURCE[0]}\")\" && pwd)\"",
      "cd \"$SCRIPT_DIR\"",
      "",
      "echo \"Starting bulk download for selected FLUXNET sites...\""
    ];

    if (includeShuttle) {
      lines.push(
        "",
        "if [ -f \"" + shuttleScript + "\" ]; then",
        "  echo \"Running Shuttle bulk download...\"",
        "  bash \"" + shuttleScript + "\" || {",
        "    echo \"Shuttle bulk download failed.\" >&2",
        "    exit 1",
        "  }",
        "else",
        "  echo \"Expected " + shuttleScript + " but it was not found.\" >&2",
        "  exit 1",
        "fi"
      );
    } else {
      lines.push("", "echo \"No Shuttle-backed selected sites to download.\"");
    }

    if (includeAmeriFlux) {
      lines.push(
        "",
        "if [ -f \"" + ameriFluxScript + "\" ]; then",
        "  echo \"Running AmeriFlux bulk download...\"",
        "  bash \"" + ameriFluxScript + "\" || {",
        "    echo \"AmeriFlux bulk download failed.\" >&2",
        "    exit 1",
        "  }",
        "else",
        "  echo \"Expected " + ameriFluxScript + " but it was not found.\" >&2",
        "  exit 1",
        "fi"
      );
    } else {
      lines.push("", "echo \"No AmeriFlux API-backed selected sites to download.\"");
    }

    lines.push("", "echo \"Bulk download complete.\"");
    return lines.join("\n");
  }

  function buildDownloadAllSelectedFileBundle(options) {
    var opts = options || {};
    return [
      {
        filename: "download_all_selected.sh",
        mimeType: "text/x-shellscript;charset=utf-8",
        text: String(opts.wrapperText || "")
      },
      {
        filename: "download_ameriflux_selected.sh",
        mimeType: "text/x-shellscript;charset=utf-8",
        text: String(opts.ameriFluxText || "")
      },
      {
        filename: "download_shuttle_selected.sh",
        mimeType: "text/x-shellscript;charset=utf-8",
        text: String(opts.shuttleText || "")
      }
    ];
  }

  function delayMs(ms) {
    return new Promise(function (resolve) {
      setTimeout(resolve, Math.max(0, ms || 0));
    });
  }

  function fetchWithRetry(url, options, retryCount, baseDelayMs) {
    var retries = Math.max(0, parseInt(retryCount, 10) || 0);
    var delayBase = Math.max(50, parseInt(baseDelayMs, 10) || RETRY_BASE_DELAY_MS);

    function attempt(attemptNo) {
      return fetch(url, options || {})
        .then(function (res) {
          if (res.ok) {
            return res;
          }
          return res.text().then(function (bodyText) {
            var err = new Error("HTTP " + res.status + " for " + url + (bodyText ? (": " + String(bodyText).slice(0, 260)) : ""));
            err.status = res.status;
            err.responseBody = bodyText || "";
            err.noRetry = res.status >= 400 && res.status < 500;
            throw err;
          });
        })
        .catch(function (error) {
          if (error && error.noRetry) {
            throw error;
          }
          if (attemptNo >= retries) {
            throw error;
          }
          var jitter = Math.floor(Math.random() * 120);
          var waitMs = (Math.pow(2, attemptNo) * delayBase) + jitter;
          return delayMs(waitMs).then(function () {
            return attempt(attemptNo + 1);
          });
        });
    }

    return attempt(0);
  }

  function fetchJsonWithRetry(url, options, retryCount, baseDelayMs) {
    return fetchWithRetry(url, options, retryCount, baseDelayMs).then(function (res) {
      return res.json().then(function (payload) {
        return {
          payload: payload,
          lastModified: res.headers && res.headers.get ? (res.headers.get("last-modified") || "") : ""
        };
      }).catch(function (error) {
        var parseErr = new Error("Failed to parse JSON response from " + url + ": " + (error && error.message ? error.message : String(error)));
        parseErr.noRetry = true;
        throw parseErr;
      });
    });
  }

  function stableHashString(value) {
    var s = String(value || "");
    var h = 2166136261;
    var i;
    for (i = 0; i < s.length; i += 1) {
      h ^= s.charCodeAt(i);
      h += (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24);
    }
    return "h" + ((h >>> 0).toString(16));
  }

  function readAvailabilityCache(cacheKey) {
    var storage = getLocalStorageSafe();
    var key = String(cacheKey || "").trim();
    if (!storage || !key) {
      return null;
    }
    try {
      var payload = safeJsonParse(storage.getItem(key));
      if (!payload || payload.schema !== 1 || !Array.isArray(payload.sites)) {
        return null;
      }
      var cachedAt = parseInt(payload.cachedAt || "0", 10) || 0;
      var ageMs = Date.now() - cachedAt;
      payload.isFresh = ageMs >= 0 && ageMs <= AMERIFLUX_AVAILABILITY_CACHE_MAX_AGE_MS;
      return payload;
    } catch (e) {
      return null;
    }
  }

  function writeAvailabilityCache(cacheKey, parsed) {
    var storage = getLocalStorageSafe();
    var key = String(cacheKey || "").trim();
    if (!storage || !key || !parsed || !Array.isArray(parsed.sites)) {
      return;
    }
    var payload = {
      schema: 1,
      cachedAt: String(Date.now()),
      totalSites: parsed.totalSites || 0,
      sitesWithYears: parsed.sitesWithYears || 0,
      sites: parsed.sites,
      freshnessKey: parsed.freshnessKey || ""
    };
    try {
      storage.setItem(key, JSON.stringify(payload));
    } catch (e) {
      // Ignore localStorage quota issues.
    }
  }

  function normalizePublishYears(values) {
    var out = [];
    var seen = {};
    if (!Array.isArray(values)) {
      return out;
    }
    values.forEach(function (value) {
      var year = parseIntOrNull(value);
      if (year == null) {
        return;
      }
      if (seen[year]) {
        return;
      }
      seen[year] = true;
      out.push(year);
    });
    out.sort(function (a, b) {
      return a - b;
    });
    return out;
  }

  function buildContiguousYearArray(firstYear, lastYear) {
    var start = parseIntOrNull(firstYear);
    var end = parseIntOrNull(lastYear);
    var years = [];
    var year;
    if (start == null || end == null || end < start) {
      return years;
    }
    for (year = start; year <= end; year += 1) {
      years.push(year);
    }
    return years;
  }

  function normalizedExactYears(values, firstYear, lastYear) {
    var years = normalizePublishYears(values);
    if (years.length) {
      return years;
    }
    return buildContiguousYearArray(firstYear, lastYear);
  }

  function yearCoverageSegmentLabel(startYear, endYear) {
    return startYear === endYear ? String(startYear) : (String(startYear) + "-" + String(endYear));
  }

  function exactYearCoverageLabel(values, firstYear, lastYear) {
    var years = normalizedExactYears(values, firstYear, lastYear);
    var labels = [];
    var rangeStart;
    var previousYear;

    if (!years.length) {
      return yearRangeLabel(firstYear, lastYear);
    }

    rangeStart = years[0];
    previousYear = years[0];

    years.slice(1).forEach(function (year) {
      if (year === previousYear + 1) {
        previousYear = year;
        return;
      }
      labels.push(yearCoverageSegmentLabel(rangeStart, previousYear));
      rangeStart = year;
      previousYear = year;
    });

    labels.push(yearCoverageSegmentLabel(rangeStart, previousYear));
    return labels.join(", ");
  }

  function exactYearSetKey(values, firstYear, lastYear) {
    return normalizedExactYears(values, firstYear, lastYear).join(",");
  }

  function exactYearSetsMatch(aYears, bYears, aFirstYear, aLastYear, bFirstYear, bLastYear) {
    return exactYearSetKey(aYears, aFirstYear, aLastYear) === exactYearSetKey(bYears, bFirstYear, bLastYear);
  }

  function buildSiteAvailabilityLookup(sites) {
    var lookup = {};
    (Array.isArray(sites) ? sites : []).forEach(function (site) {
      var siteId = normalizeSiteId(site && site.site_id);
      if (!siteId) {
        return;
      }
      lookup[siteId] = Object.assign({}, site, {
        site_id: String(site && site.site_id || "").trim() || siteId,
        publish_years: normalizedExactYears(site && site.publish_years, site && site.first_year, site && site.last_year)
      });
    });
    return lookup;
  }

  function parseAmeriFluxAvailabilityPayload(payload, freshnessNamespace) {
    var values = payload && Array.isArray(payload.values) ? payload.values : [];
    var sites = [];

    values.forEach(function (entry) {
      var siteId = String(entry && entry.site_id || "").trim();
      if (!siteId) {
        return;
      }
      var publishYears = normalizePublishYears(entry && entry.publish_years);
      if (!publishYears.length) {
        return;
      }
      sites.push({
        site_id: siteId,
        publish_years: publishYears,
        first_year: publishYears[0],
        last_year: publishYears[publishYears.length - 1],
        years: exactYearCoverageLabel(publishYears, publishYears[0], publishYears[publishYears.length - 1]),
        country: deriveCountry(siteId, "")
      });
    });

    sites.sort(function (a, b) {
      var aa = String(a.site_id || "").toLowerCase();
      var bb = String(b.site_id || "").toLowerCase();
      if (aa < bb) {
        return -1;
      }
      if (aa > bb) {
        return 1;
      }
      return 0;
    });

    var canonical = JSON.stringify(sites.map(function (item) {
      return [item.site_id, item.first_year, item.last_year, item.publish_years.join(",")];
    }));

    return {
      totalSites: values.length,
      sitesWithYears: sites.length,
      sites: sites,
      freshnessKey: String(freshnessNamespace || "ameriflux") + ":" + stableHashString(canonical)
    };
  }

  function buildAmeriFluxApiRow(site, index, options) {
    var opts = options || {};
    var siteId = String(site && site.site_id || "").trim();
    var siteName = String(site && site.site_name || "").trim();
    var country = deriveCountry(siteId, site && site.country ? site.country : "");
    var publishYears = normalizedExactYears(site && site.publish_years, site && site.first_year, site && site.last_year);
    var firstYear = publishYears.length ? publishYears[0] : parseIntOrNull(site && site.first_year);
    var lastYear = publishYears.length ? publishYears[publishYears.length - 1] : parseIntOrNull(site && site.last_year);
    var years = exactYearCoverageLabel(publishYears, firstYear, lastYear);
    var keySuffix = years === "\u2014" ? "unknown" : years;
    var latitude = parseCoordinate(site && site.latitude, -90, 90);
    var longitude = parseCoordinate(site && site.longitude, -180, 180);
    var dataProduct = String(opts.dataProduct || AMERIFLUX_FLUXNET_PRODUCT).trim().toUpperCase();
    var networkLabel;
    if (dataProduct !== FLUXNET2015_PRODUCT && dataProduct !== AMERIFLUX_BASE_PRODUCT) {
      dataProduct = AMERIFLUX_FLUXNET_PRODUCT;
    }
    networkLabel = dataProduct === FLUXNET2015_PRODUCT
      ? (inferFluxnet2015NetworkFromCountry(country) || "")
      : AMERIFLUX_SOURCE_ONLY;
    var sourceLabel = String(opts.sourceLabel || "").trim() || (dataProduct === FLUXNET2015_PRODUCT
      ? FLUXNET2015_SOURCE_ONLY
      : (dataProduct === AMERIFLUX_BASE_PRODUCT ? BASE_SOURCE_ONLY : AMERIFLUX_SOURCE_ONLY));
    var sourceReason = String(opts.sourceReason || "").trim() || (sourceLabel === FLUXNET2015_SOURCE_ONLY
      ? "Only available from AmeriFlux API FLUXNET2015 fallback."
      : "Only available from AmeriFlux API.");
    var processingLineage = dataProduct === AMERIFLUX_BASE_PRODUCT
      ? PROCESSING_LINEAGE_OTHER
      : PROCESSING_LINEAGE_ONEFLUX;
    var row = {
      _index: index,
      _selection_key: "ameriflux_api|" + dataProduct + "|" + siteId + "|" + keySuffix,
      site_id: siteId,
      site_name: siteName,
      country: country,
      data_hub: AMERIFLUX_DATA_HUB,
      network: networkLabel,
      source_network: networkLabel,
      network_display: networkLabel,
      network_tokens: networkLabel ? [networkLabel] : [],
      vegetation_type: firstDefinedString(site, ["vegetation_type", "igbp", "veg_type"]),
      first_year: firstYear,
      last_year: lastYear,
      years: years,
      length_years: calculateCoverageLength(firstYear, lastYear),
      latitude: latitude,
      longitude: longitude,
      download_link: "",
      download_mode: "ameriflux_api",
      processing_lineage: processingLineage,
      source_label: sourceLabel,
      source_reason: sourceReason,
      source_origin: AMERIFLUX_API_SOURCE_ORIGIN,
      api_data_product: dataProduct,
      publish_years: publishYears
    };
    return finalizeRowComputedState(row);
  }

  function inferProcessedDataProduct(row) {
    var explicit = getApiRowDataProduct(row);
    var fileName = String(row && (row.file_name || row.download_link) || "").toUpperCase();
    var sourceLabel = String(row && row.source_label || "").trim();
    if (explicit === FLUXNET2015_PRODUCT || sourceLabel === FLUXNET2015_SOURCE_ONLY || fileName.indexOf("FLUXNET2015") !== -1) {
      return FLUXNET2015_PRODUCT;
    }
    return AMERIFLUX_FLUXNET_PRODUCT;
  }

  function buildSurfacedProductMetadata(fields) {
    var sourceOrigin = String(fields && (fields.sourceOrigin || fields.source_origin || fields.source) || "").trim();
    var siteId = String(fields && (fields.siteId || fields.site_id) || "").trim();
    var canonicalSiteId = normalizeSiteId(siteId);
    var exactYears = normalizedExactYears(
      fields && (fields.exactYears || fields.exact_years || fields.publish_years),
      fields && (fields.firstYear || fields.first_year),
      fields && (fields.lastYear || fields.last_year)
    );
    var firstYear = exactYears.length ? exactYears[0] : parseIntOrNull(fields && (fields.firstYear || fields.first_year));
    var lastYear = exactYears.length ? exactYears[exactYears.length - 1] : parseIntOrNull(fields && (fields.lastYear || fields.last_year));
    var productFamily = normalizeProductFamily(fields && (fields.productFamily || fields.product_family) || PRODUCT_FAMILY_FLUXNET);
    var sourceLabel = String(fields && (fields.sourceLabel || fields.source_label) || "").trim();
    var downloadMode = String(fields && (fields.downloadMode || fields.download_mode) || "").trim() || "direct";
    var apiDataProduct = normalizeDownloadProduct(fields && (fields.apiDataProduct || fields.api_data_product));
    var downloadLink = String(fields && (fields.downloadLink || fields.download_link) || "").trim();
    var dataHub = String(fields && (fields.dataHub || fields.data_hub) || "").trim();
    var siteName = String(fields && (fields.siteName || fields.site_name) || "").trim();
    var country = deriveCountry(canonicalSiteId || siteId, fields && (fields.country || fields.country_name) ? (fields.country || fields.country_name) : "");
    var vegetationType = normalizeVegetationType(fields && (fields.vegetationType || fields.vegetation_type || fields.igbp));
    var latitude = parseCoordinate(fields && (fields.latitude || fields.location_lat), -90, 90);
    var longitude = parseCoordinate(fields && (fields.longitude || fields.location_long || fields.location_lon), -180, 180);
    var coverageLabel = exactYearCoverageLabel(exactYears, firstYear, lastYear);
    var sourceReason = String(fields && (fields.sourceReason || fields.source_reason) || "").trim();
    var objectId = String(fields && (fields.objectId || fields.object_id) || "").trim();
    var fileName = String(fields && (fields.fileName || fields.file_name) || "").trim();
    var directDownloadUrl = String(fields && (fields.directDownloadUrl || fields.direct_download_url) || "").trim();
    var metadataUrl = String(fields && (fields.metadataUrl || fields.metadata_url) || "").trim();
    var accessUrl = String(fields && (fields.accessUrl || fields.access_url) || "").trim();
    var citation = String(fields && fields.citation || "").trim();
    var isIcos = !!(fields && (fields.isIcos || fields.is_icos));
    var network = String(fields && fields.network || "").trim();
    var sourceNetwork = String(fields && (fields.sourceNetwork || fields.source_network) || "").trim();
    var networkDisplay = String(fields && (fields.networkDisplay || fields.network_display) || network || sourceNetwork).trim();
    var processingLineage = resolveProcessingLineage(fields || {});

    return {
      productFamily: productFamily,
      processingLineage: processingLineage,
      source: sourceOrigin || resolveSourceOrigin(fields || {}),
      sourceLabel: sourceLabel,
      siteId: siteId || canonicalSiteId,
      exactYears: exactYears,
      coverageLabel: coverageLabel,
      firstYear: firstYear,
      lastYear: lastYear,
      lengthYears: exactYears.length || calculateCoverageLength(firstYear, lastYear),
      downloadMode: downloadMode,
      downloadLink: downloadLink,
      apiDataProduct: downloadMode === "ameriflux_api" ? apiDataProduct : inferProcessedDataProduct(fields || {}),
      dataHub: dataHub,
      sourceOrigin: sourceOrigin || resolveSourceOrigin(fields || {}),
      sourceReason: sourceReason,
      siteName: siteName,
      country: country,
      vegetationType: vegetationType,
      latitude: latitude,
      longitude: longitude,
      isIcos: isIcos,
      objectId: objectId,
      fileName: fileName,
      directDownloadUrl: directDownloadUrl,
      metadataUrl: metadataUrl,
      accessUrl: accessUrl,
      citation: citation,
      network: network,
      sourceNetwork: sourceNetwork,
      networkDisplay: networkDisplay,
      processing_lineage: processingLineage,
      site_id: siteId || canonicalSiteId,
      source_label: sourceLabel,
      download_mode: downloadMode,
      download_link: downloadLink,
      api_data_product: downloadMode === "ameriflux_api" ? apiDataProduct : inferProcessedDataProduct(fields || {}),
      data_hub: dataHub,
      source_origin: sourceOrigin || resolveSourceOrigin(fields || {}),
      source_reason: sourceReason,
      site_name: siteName,
      vegetation_type: vegetationType,
      network: network,
      source_network: sourceNetwork,
      network_display: networkDisplay,
      first_year: firstYear,
      last_year: lastYear,
      years: coverageLabel,
      length_years: exactYears.length || calculateCoverageLength(firstYear, lastYear),
      is_icos: isIcos,
      object_id: objectId,
      file_name: fileName,
      direct_download_url: directDownloadUrl,
      metadata_url: metadataUrl,
      access_url: accessUrl,
      display_label: surfacedProductDisplayName(fields || {}, true)
    };
  }

  function resolveProcessedExactYears(row, availabilityLookups) {
    var siteId = normalizeSiteId(row && row.site_id);
    var explicitYears = normalizePublishYears(row && row.publish_years);
    var lookups = availabilityLookups || {};
    var ameriFluxLookup = lookups.ameriFluxFluxnet || {};
    var fluxnet2015Lookup = lookups.fluxnet2015 || {};
    var baseLookup = lookups.ameriFluxBase || {};
    var dataHub = String(row && row.data_hub || "").trim();
    var sourceLabel = String(row && row.source_label || "").trim();
    var inferredProduct = inferProcessedDataProduct(row);

    if (explicitYears.length) {
      return explicitYears;
    }
    if (siteId && inferredProduct === FLUXNET2015_PRODUCT && fluxnet2015Lookup[siteId]) {
      return normalizedExactYears(
        fluxnet2015Lookup[siteId].publish_years,
        fluxnet2015Lookup[siteId].first_year,
        fluxnet2015Lookup[siteId].last_year
      );
    }
    if (siteId && baseLookup[siteId] && ameriFluxLookup[siteId]) {
      return normalizedExactYears(
        ameriFluxLookup[siteId].publish_years,
        ameriFluxLookup[siteId].first_year,
        ameriFluxLookup[siteId].last_year
      );
    }
    if (
      siteId &&
      ameriFluxLookup[siteId] &&
      (
        dataHub === AMERIFLUX_DATA_HUB ||
        sourceLabel === AMERIFLUX_SOURCE_ONLY ||
        sourceLabel === AMERIFLUX_SHUTTLE ||
        hasNetworkTag(row && row.network_display, AMERIFLUX_SOURCE_ONLY) ||
        hasNetworkTag(row && row.network, AMERIFLUX_SOURCE_ONLY) ||
        hasNetworkTag(row && row.source_network, AMERIFLUX_SOURCE_ONLY)
      )
    ) {
      return normalizedExactYears(
        ameriFluxLookup[siteId].publish_years,
        ameriFluxLookup[siteId].first_year,
        ameriFluxLookup[siteId].last_year
      );
    }
    return buildContiguousYearArray(row && row.first_year, row && row.last_year);
  }

  function buildPrimaryProcessedProduct(row, availabilityLookups) {
    var sourceLabel = String(row && row.source_label || "").trim();
    var dataProduct = getApiRowDataProduct(row);
    if (!row || sourceLabel === BASE_SOURCE_ONLY || dataProduct === AMERIFLUX_BASE_PRODUCT || isRequestOnlyRow(row) || isEfdSourceRow(row)) {
      return null;
    }
    return buildSurfacedProductMetadata(Object.assign({}, row, {
      productFamily: inferRowProductFamily(row),
      exactYears: resolveProcessedExactYears(row, availabilityLookups)
    }));
  }

  function buildAmeriFluxBaseProduct(site) {
    if (!site) {
      return null;
    }
    return buildSurfacedProductMetadata({
      productFamily: PRODUCT_FAMILY_BASE,
      processingLineage: PROCESSING_LINEAGE_OTHER,
      source: AMERIFLUX_API_SOURCE_ORIGIN,
      sourceLabel: BASE_SOURCE_ONLY,
      siteId: site.site_id,
      exactYears: site.publish_years,
      firstYear: site.first_year,
      lastYear: site.last_year,
      downloadMode: "ameriflux_api",
      downloadLink: "",
      apiDataProduct: AMERIFLUX_BASE_PRODUCT,
      dataHub: AMERIFLUX_DATA_HUB,
      sourceOrigin: AMERIFLUX_API_SOURCE_ORIGIN,
      sourceReason: "Available from AmeriFlux API.",
      siteName: site.site_name,
      country: site.country,
      vegetationType: site.vegetation_type,
      latitude: site.latitude,
      longitude: site.longitude
    });
  }

  function buildSurfacedCoverageSummary(products) {
    return (Array.isArray(products) ? products : []).map(function (product) {
      return surfacedProductDisplayName(product, false) + ": " + String(product && product.coverageLabel || "\u2014");
    }).join(" \u00b7 ");
  }

  function buildSurfacedYearUnion(products) {
    var years = [];
    var seen = {};
    (Array.isArray(products) ? products : []).forEach(function (product) {
      (Array.isArray(product && product.exactYears) ? product.exactYears : []).forEach(function (year) {
        if (seen[year]) {
          return;
        }
        seen[year] = true;
        years.push(year);
      });
    });
    years.sort(function (a, b) {
      return a - b;
    });
    return years;
  }

  function applySurfacedProductsToRow(row, primaryProcessedProduct, ameriFluxBaseProduct, surfacedProducts, classification) {
    var products = Array.isArray(surfacedProducts) ? surfacedProducts.slice() : [];
    var unionYears = buildSurfacedYearUnion(products);
    var rowClassification = String(classification || "").trim() || classifySurfacedProducts(products);

    if (!rowClassification && isEfdSourceRow(row)) {
      // EFD rows are request-only discovery records, but they still belong in the
      // non-FLUXNET "Other processed" availability bucket.
      rowClassification = SURFACED_CLASSIFICATION_OTHER_PROCESSED;
    }

    row.primaryProcessedProduct = primaryProcessedProduct || null;
    row.ameriFluxBaseProduct = ameriFluxBaseProduct || null;
    row.surfacedProducts = products;
    row.surfacedProductClassification = rowClassification;
    row.hasProcessedProduct = !!primaryProcessedProduct;
    row.hasFluxnetAvailable = rowClassification === SURFACED_CLASSIFICATION_FLUXNET_PROCESSED ||
      rowClassification === SURFACED_CLASSIFICATION_FLUXNET_AND_OTHER;

    if (unionYears.length) {
      row.first_year = unionYears[0];
      row.last_year = unionYears[unionYears.length - 1];
      row.years = buildSurfacedCoverageSummary(products);
      row.length_years = unionYears.length;
    } else {
      row.years = isEfdSourceRow(row) || isRequestOnlyRow(row)
        ? "Request via EFD"
        : yearRangeLabel(row.first_year, row.last_year);
      row.length_years = calculateCoverageLength(row.first_year, row.last_year);
    }

    applyRowSourceFilterState(row);
    row.search_text = buildRowSearchText(row);
    return row;
  }

  function getSurfacedProductsForRow(row) {
    if (row && Array.isArray(row.surfacedProducts) && row.surfacedProducts.length) {
      return row.surfacedProducts.slice();
    }
    if (row && row.primaryProcessedProduct) {
      return [row.primaryProcessedProduct];
    }
    if (!row) {
      return [];
    }
    return [buildPrimaryProcessedProduct(row, {})].filter(Boolean);
  }

  function applySurfacedProductSelection(row, availabilityLookups) {
    var siteId = normalizeSiteId(row && row.site_id);
    var lookups = availabilityLookups || {};
    var primaryProcessedProduct = buildPrimaryProcessedProduct(row, lookups);
    var ameriFluxBaseProduct = siteId && lookups.ameriFluxBase && lookups.ameriFluxBase[siteId]
      ? buildAmeriFluxBaseProduct(lookups.ameriFluxBase[siteId])
      : null;
    var surfacedProducts;

    if (!primaryProcessedProduct && ameriFluxBaseProduct) {
      surfacedProducts = [ameriFluxBaseProduct];
    } else if (primaryProcessedProduct && !ameriFluxBaseProduct) {
      surfacedProducts = [primaryProcessedProduct];
    } else if (primaryProcessedProduct && ameriFluxBaseProduct && exactYearSetsMatch(
      primaryProcessedProduct.exactYears,
      ameriFluxBaseProduct.exactYears,
      primaryProcessedProduct.firstYear,
      primaryProcessedProduct.lastYear,
      ameriFluxBaseProduct.firstYear,
      ameriFluxBaseProduct.lastYear
    )) {
      surfacedProducts = [primaryProcessedProduct];
    } else {
      surfacedProducts = [primaryProcessedProduct, ameriFluxBaseProduct].filter(Boolean);
    }

    return applySurfacedProductsToRow(
      row,
      primaryProcessedProduct,
      ameriFluxBaseProduct,
      surfacedProducts,
      classifySurfacedProducts(surfacedProducts)
    );
  }

  function mergeCatalogRows(shuttleRows, icosDirectRows, japanFluxRows, ameriFluxSites, fluxnet2015Sites, ameriFluxBaseSites, efdRows) {
    if (arguments.length < 6) {
      var legacyAmeriFluxBaseSites = arguments.length >= 5 ? fluxnet2015Sites : [];
      fluxnet2015Sites = ameriFluxSites;
      ameriFluxSites = japanFluxRows;
      japanFluxRows = [];
      ameriFluxBaseSites = legacyAmeriFluxBaseSites;
    }
    var mergedRows = (Array.isArray(shuttleRows) ? shuttleRows : []).map(function (row) {
      return Object.assign({}, row);
    });
    var icosSites = dedupeIcosDirectRows((Array.isArray(icosDirectRows) ? icosDirectRows : []).map(function (row) {
      return Object.assign({}, row);
    }));
    var japanFluxSites = (Array.isArray(japanFluxRows) ? japanFluxRows : []).map(function (row) {
      return Object.assign({}, row);
    });
    var ameriSites = Array.isArray(ameriFluxSites) ? ameriFluxSites : [];
    var fluxnet2015 = Array.isArray(fluxnet2015Sites) ? fluxnet2015Sites : [];
    var ameriFluxBase = Array.isArray(ameriFluxBaseSites) ? ameriFluxBaseSites : [];
    var efdSites = dedupeSiteLevelRows((Array.isArray(efdRows) ? efdRows : []).map(function (row) {
      return Object.assign({}, row);
    }));
    var shuttleBySite = {};
    var canonicalSiteIds = {};
    var icosSuppressedByShuttle = 0;
    var icosDirectOnlySites = 0;
    var japanFluxSuppressedByHigherPrecedence = 0;
    var japanFluxOnlySites = 0;
    var overlapSites = 0;
    var ameriOnlySites = 0;
    var fluxnet2015OnlySites = 0;
    var baseOnlySites = 0;
    var additionalBaseYearsSites = 0;
    var efdSuppressedByHigherPrecedence = 0;
    var efdOnlySites = 0;

    // Keep precedence centralized here: Shuttle > ICOS-direct > JapanFlux-direct > AmeriFlux FLUXNET > FLUXNET2015 > BASE > EFD.
    mergedRows.forEach(function (row) {
      var siteId = String(row && row.site_id || "").trim();
      if (!siteId) {
        return;
      }
      if (!shuttleBySite[siteId]) {
        shuttleBySite[siteId] = [];
      }
      if (!row.download_mode) {
        row.download_mode = "direct";
      }
      if (!row.source_label) {
        row.source_label = "";
      }
      if (!row.source_reason) {
        row.source_reason = "";
      }
      if (!row.source_origin) {
        row.source_origin = SHUTTLE_SOURCE_ORIGIN;
      }
      row.source_priority = resolveSourcePriority(row);
      row.source_filter = sourceFilterValue(row);
      canonicalSiteIds[siteId] = true;
      shuttleBySite[siteId].push(row);
    });

    icosSites.forEach(function (row) {
      var siteId = String(row && row.site_id || "").trim();
      if (!siteId) {
        return;
      }
      if (shuttleBySite[siteId] && shuttleBySite[siteId].length) {
        icosSuppressedByShuttle += 1;
        return;
      }
      if (canonicalSiteIds[siteId]) {
        return;
      }
      if (!row.data_hub) {
        row.data_hub = ICOS_DIRECT_SOURCE_ONLY;
      }
      if (!row.download_mode) {
        row.download_mode = "direct";
      }
      if (!row.download_link && row.direct_download_url) {
        row.download_link = row.direct_download_url;
      }
      if (!row.source_label) {
        row.source_label = ICOS_DIRECT_SOURCE_ONLY;
      }
      if (!row.source_reason) {
        row.source_reason = "Available directly from the ICOS Carbon Portal archive.";
      }
      if (!row.source_origin) {
        row.source_origin = ICOS_DIRECT_SOURCE_ORIGIN;
      }
      row.source_priority = resolveSourcePriority(row);
      row.source_filter = sourceFilterValue(row);
      canonicalSiteIds[siteId] = true;
      icosDirectOnlySites += 1;
      mergedRows.push(row);
    });

    japanFluxSites.forEach(function (row) {
      var siteId = String(row && row.site_id || "").trim();
      if (!siteId) {
        return;
      }
      if (shuttleBySite[siteId] && shuttleBySite[siteId].length) {
        japanFluxSuppressedByHigherPrecedence += 1;
        return;
      }
      if (canonicalSiteIds[siteId]) {
        japanFluxSuppressedByHigherPrecedence += 1;
        return;
      }
      if (!row.data_hub) {
        row.data_hub = SOURCE_FILTER_TAG_JAPANFLUX;
      }
      if (!row.download_mode) {
        row.download_mode = "direct";
      }
      if (!row.source_label) {
        row.source_label = SOURCE_FILTER_TAG_JAPANFLUX;
      }
      if (!row.source_reason) {
        row.source_reason = "Available from the JapanFlux2024 ADS archive.";
      }
      if (!row.source_origin) {
        row.source_origin = JAPANFLUX_DIRECT_SOURCE_ORIGIN;
      }
      row.source_priority = resolveSourcePriority(row);
      row.source_filter = sourceFilterValue(row);
      canonicalSiteIds[siteId] = true;
      japanFluxOnlySites += 1;
      mergedRows.push(row);
    });

    ameriSites.forEach(function (site) {
      var siteId = String(site && site.site_id || "").trim();
      if (!siteId) {
        return;
      }
      var shuttleMatches = shuttleBySite[siteId];
      if (shuttleMatches && shuttleMatches.length) {
        overlapSites += 1;
        shuttleMatches.forEach(function (row) {
          row.source_label = AMERIFLUX_SHUTTLE;
          row.source_reason = "Available in both Shuttle and AmeriFlux; Shuttle is preferred when both exist.";
          row.source_filter = sourceFilterValue(row);
        });
        return;
      }
      if (canonicalSiteIds[siteId]) {
        return;
      }
      ameriOnlySites += 1;
      canonicalSiteIds[siteId] = true;
      mergedRows.push(buildAmeriFluxApiRow(site, mergedRows.length, {
        dataProduct: AMERIFLUX_FLUXNET_PRODUCT,
        sourceLabel: AMERIFLUX_SOURCE_ONLY,
        sourceReason: "Only available from AmeriFlux API."
      }));
    });

    fluxnet2015.forEach(function (site) {
      var siteId = String(site && site.site_id || "").trim();
      if (!siteId || canonicalSiteIds[siteId]) {
        return;
      }
      fluxnet2015OnlySites += 1;
      canonicalSiteIds[siteId] = true;
      mergedRows.push(buildAmeriFluxApiRow(site, mergedRows.length, {
        dataProduct: FLUXNET2015_PRODUCT,
        sourceLabel: FLUXNET2015_SOURCE_ONLY,
        sourceReason: "Only available from AmeriFlux API FLUXNET2015 fallback."
      }));
    });

    mergedRows.forEach(function (row, idx) {
      var siteId = String(row.site_id || "").trim();
      row._index = idx;
      if (!row._selection_key) {
        row._selection_key = String(row.data_hub || "unknown") + "|" + siteId + "|" + String(row.download_link || "dynamic");
      }
      finalizeRowComputedState(row);
    });

    var availabilityLookups = {
      ameriFluxFluxnet: buildSiteAvailabilityLookup(ameriSites),
      fluxnet2015: buildSiteAvailabilityLookup(fluxnet2015),
      ameriFluxBase: buildSiteAvailabilityLookup(ameriFluxBase)
    };
    var finalRows = mergedRows.map(function (row) {
      return applySurfacedProductSelection(row, availabilityLookups);
    });
    var finalSiteIds = {};

    finalRows.forEach(function (row) {
      var siteId = normalizeSiteId(row && row.site_id);
      if (siteId) {
        finalSiteIds[siteId] = true;
      }
      if (row && row.surfacedProductClassification === SURFACED_CLASSIFICATION_FLUXNET_AND_OTHER) {
        additionalBaseYearsSites += 1;
      }
    });

    ameriFluxBase.forEach(function (site) {
      var siteId = normalizeSiteId(site && site.site_id);
      var baseRow;
      if (!siteId || finalSiteIds[siteId]) {
        return;
      }
      baseOnlySites += 1;
      finalSiteIds[siteId] = true;
      baseRow = buildAmeriFluxApiRow(site, finalRows.length, {
        dataProduct: AMERIFLUX_BASE_PRODUCT,
        sourceLabel: BASE_SOURCE_ONLY,
        sourceReason: "Only available from AmeriFlux API."
      });
      finalRows.push(applySurfacedProductSelection(baseRow, availabilityLookups));
    });

    efdSites.forEach(function (row) {
      var siteId = normalizeSiteId(row && row.site_id);
      var finalizedRow;
      if (!siteId) {
        return;
      }
      if (finalSiteIds[siteId]) {
        efdSuppressedByHigherPrecedence += 1;
        return;
      }
      if (!row.data_hub) {
        row.data_hub = SOURCE_FILTER_TAG_EFD;
      }
      if (!row.download_link && row.request_page_url) {
        row.download_link = row.request_page_url;
      }
      if (!row.download_mode) {
        row.download_mode = REQUEST_PAGE_DOWNLOAD_MODE;
      }
      if (!row.source_label) {
        row.source_label = SOURCE_FILTER_TAG_EFD;
      }
      if (!row.source_reason) {
        row.source_reason = "Listed in the public European Fluxes Database site catalog.";
      }
      if (!row.source_origin) {
        row.source_origin = EFD_SOURCE_ORIGIN;
      }
      row.source_priority = resolveSourcePriority(row);
      row.source_filter = sourceFilterValue(row);
      finalizedRow = applySurfacedProductSelection(finalizeRowComputedState(row), availabilityLookups);
      finalRows.push(finalizedRow);
      finalSiteIds[siteId] = true;
      efdOnlySites += 1;
    });

    finalRows.forEach(function (row, idx) {
      row._index = idx;
      if (!row._selection_key) {
        row._selection_key = String(row.data_hub || "unknown") + "|" + String(row.site_id || "") + "|" + String(row.download_link || "dynamic");
      }
      row.search_text = buildRowSearchText(row);
    });

    return {
      rows: finalRows,
      icosDirectTotalSites: icosSites.length,
      icosDirectSuppressedByShuttle: icosSuppressedByShuttle,
      icosDirectOnlySites: icosDirectOnlySites,
      japanFluxTotalSites: japanFluxSites.length,
      japanFluxSuppressedByHigherPrecedence: japanFluxSuppressedByHigherPrecedence,
      japanFluxOnlySites: japanFluxOnlySites,
      amerifluxTotalSites: ameriSites.length,
      amerifluxSitesWithYears: ameriSites.length,
      amerifluxOverlapSites: overlapSites,
      amerifluxOnlySites: ameriOnlySites,
      ameriFluxBaseTotalSites: ameriFluxBase.length,
      ameriFluxBaseSitesWithYears: ameriFluxBase.length,
      baseOnlySites: baseOnlySites,
      additionalBaseYearsSites: additionalBaseYearsSites,
      fluxnet2015TotalSites: fluxnet2015.length,
      fluxnet2015SitesWithYears: fluxnet2015.length,
      fluxnet2015OnlySites: fluxnet2015OnlySites,
      efdTotalSites: efdSites.length,
      efdSuppressedByHigherPrecedence: efdSuppressedByHigherPrecedence,
      efdOnlySites: efdOnlySites
    };
  }

  function mergeShuttleAndAmeriFluxRows(shuttleRows, ameriFluxSites) {
    return mergeCatalogRows(shuttleRows, [], ameriFluxSites, []);
  }

  function parseCsv(text) {
    var rows = [];
    var row = [];
    var field = "";
    var inQuotes = false;
    var i;
    var ch;
    var next;

    for (i = 0; i < text.length; i += 1) {
      ch = text.charAt(i);
      next = i + 1 < text.length ? text.charAt(i + 1) : "";

      if (inQuotes) {
        if (ch === "\"") {
          if (next === "\"") {
            field += "\"";
            i += 1;
          } else {
            inQuotes = false;
          }
        } else {
          field += ch;
        }
        continue;
      }

      if (ch === "\"") {
        inQuotes = true;
      } else if (ch === ",") {
        row.push(field);
        field = "";
      } else if (ch === "\n") {
        row.push(field);
        rows.push(row);
        row = [];
        field = "";
      } else if (ch === "\r") {
        if (next === "\n") {
          row.push(field);
          rows.push(row);
          row = [];
          field = "";
          i += 1;
        } else {
          row.push(field);
          rows.push(row);
          row = [];
          field = "";
        }
      } else {
        field += ch;
      }
    }

    if (inQuotes) {
      throw new Error("CSV parse error: unmatched quote");
    }

    if (field !== "" || row.length > 0) {
      row.push(field);
      rows.push(row);
    }

    return rows;
  }

  function csvTextToObjects(text) {
    var matrix = parseCsv(String(text || ""));
    if (!matrix.length) {
      return [];
    }
    var headers = (matrix.shift() || []).map(function (h) {
      return toSnakeCase(h);
    });
    var out = [];

    matrix.forEach(function (cells) {
      if (!cells || !cells.length || (cells.length === 1 && cells[0] === "")) {
        return;
      }
      var obj = {};
      var i;
      for (i = 0; i < headers.length; i += 1) {
        obj[headers[i]] = cells[i] == null ? "" : String(cells[i]).trim();
      }
      out.push(obj);
    });

    return out;
  }

  function payloadJsonToObjects(payload) {
    if (Array.isArray(payload)) {
      return payload.map(function (row) {
        var obj = {};
        Object.keys(row || {}).forEach(function (key) {
          obj[toSnakeCase(key)] = row[key];
        });
        return obj;
      });
    }

    if (payload && Array.isArray(payload.columns) && Array.isArray(payload.rows)) {
      var columns = payload.columns.map(function (col) {
        return toSnakeCase(col);
      });
      return payload.rows.map(function (cells) {
        var obj = {};
        var i;
        for (i = 0; i < columns.length; i += 1) {
          obj[columns[i]] = Array.isArray(cells) ? cells[i] : undefined;
        }
        return obj;
      });
    }

    throw new Error("Unsupported snapshot JSON format");
  }

  function buildRawSelectionKey(raw) {
    var siteId = String(raw && (raw.site_id || raw.site) || "").trim();
    var hub = String(raw && (raw.data_hub || raw.hub) || "").trim();
    var downloadLink = String(raw && (raw.download_link || raw.url) || "").trim();
    if (!siteId || !hub || !downloadLink) {
      return "";
    }
    return hub + "|" + siteId + "|" + downloadLink;
  }

  function parseCoordinate(value, min, max) {
    var number = typeof value === "number" ? value : parseFloat(String(value == null ? "" : value).trim());
    if (!isFinite(number)) {
      return null;
    }
    if (typeof min === "number" && number < min) {
      return null;
    }
    if (typeof max === "number" && number > max) {
      return null;
    }
    return number;
  }

  function extractRawLatitude(raw) {
    return parseCoordinate(
      raw && (raw.location_lat || raw.latitude || raw.lat),
      -90,
      90
    );
  }

  function extractRawLongitude(raw) {
    return parseCoordinate(
      raw && (raw.location_long || raw.location_lon || raw.longitude || raw.lon || raw.lng),
      -180,
      180
    );
  }

  function buildCoordinateLookup(rawRows) {
    var lookup = {};
    (Array.isArray(rawRows) ? rawRows : []).forEach(function (raw) {
      var key = buildRawSelectionKey(raw);
      var latitude = extractRawLatitude(raw);
      var longitude = extractRawLongitude(raw);
      if (!key || latitude == null || longitude == null) {
        return;
      }
      lookup[key] = {
        location_lat: latitude,
        location_long: longitude
      };
    });
    return lookup;
  }

  function enrichRowsWithCoordinateLookup(rawRows, coordinateLookup) {
    if (!Array.isArray(rawRows) || !coordinateLookup) {
      return Array.isArray(rawRows) ? rawRows.slice() : [];
    }
    return rawRows.map(function (raw) {
      var key = buildRawSelectionKey(raw);
      var coords = key ? coordinateLookup[key] : null;
      var latitude = extractRawLatitude(raw);
      var longitude = extractRawLongitude(raw);
      var enriched;
      if (!coords || (latitude != null && longitude != null)) {
        return raw;
      }
      enriched = {};
      Object.keys(raw || {}).forEach(function (prop) {
        enriched[prop] = raw[prop];
      });
      enriched.location_lat = coords.location_lat;
      enriched.location_long = coords.location_long;
      return enriched;
    });
  }

  function buildAmeriFluxSiteInfoLookup(rawRows) {
    var lookup = {};
    (Array.isArray(rawRows) ? rawRows : []).forEach(function (raw) {
      var siteId = ameriFluxSiteInfoSiteId(raw);
      if (!siteId) {
        return;
      }
      lookup[siteId] = buildSiteInfoEntry(siteId, raw);
    });
    return lookup;
  }

  function buildSiteNameMetadataLookup(rawRows) {
    var lookup = {};
    (Array.isArray(rawRows) ? rawRows : []).forEach(function (raw) {
      var siteId = siteNameMetadataSiteId(raw);
      var siteName = siteInfoSiteName(raw);
      if (!siteId || !siteName || lookup[siteId]) {
        return;
      }
      lookup[siteId] = siteName;
    });
    return lookup;
  }

  function buildVegetationMetadataLookup(rawRows) {
    var lookup = {};
    (Array.isArray(rawRows) ? rawRows : []).forEach(function (raw) {
      var siteId = vegetationMetadataSiteId(raw);
      var vegetationType = extractVegetationMetadataValue(raw);
      if (!siteId || !vegetationType) {
        return;
      }
      lookup[siteId] = vegetationType;
    });
    return lookup;
  }

  function loadAmeriFluxSiteInfo(url) {
    return fetchText(url).then(function (result) {
      return {
        lookup: buildAmeriFluxSiteInfoLookup(csvTextToObjects(result.text)),
        source: "csv",
        sourceUrl: url,
        lastModified: result.lastModified || "",
        warning: ""
      };
    }).catch(function (error) {
      return {
        lookup: {},
        source: "csv",
        sourceUrl: url,
        lastModified: "",
        warning: "AmeriFlux site metadata unavailable; API-only site map coverage may be incomplete.",
        error: error
      };
    });
  }

  function loadSiteNameMetadata(url) {
    return fetchText(url).then(function (result) {
      var lookup = buildSiteNameMetadataLookup(csvTextToObjects(result.text));
      var canonical = JSON.stringify(Object.keys(lookup).sort().map(function (siteId) {
        return [siteId, lookup[siteId]];
      }));
      return {
        lookup: lookup,
        source: "csv",
        sourceUrl: url,
        lastModified: result.lastModified || "",
        meta: {
          version: stableHashString(canonical)
        },
        warning: ""
      };
    }).catch(function (error) {
      return {
        lookup: {},
        source: "csv",
        sourceUrl: url,
        lastModified: "",
        meta: {},
        warning: "Site-name metadata unavailable; rows with missing names may still show site IDs.",
        error: error
      };
    });
  }

  function loadVegetationMetadata(url) {
    return fetchText(url).then(function (result) {
      var lookup = buildVegetationMetadataLookup(csvTextToObjects(result.text));
      var canonical = JSON.stringify(Object.keys(lookup).sort().map(function (siteId) {
        return [siteId, lookup[siteId]];
      }));
      return {
        lookup: lookup,
        source: "csv",
        sourceUrl: url,
        lastModified: result.lastModified || "",
        meta: {
          version: stableHashString(canonical)
        },
        warning: ""
      };
    }).catch(function (error) {
      return {
        lookup: {},
        source: "csv",
        sourceUrl: url,
        lastModified: "",
        meta: {},
        warning: "Vegetation metadata unavailable; API-only vegetation coverage may be incomplete.",
        error: error
      };
    });
  }

  function siteNameNeedsFallback(siteId, siteName) {
    var normalizedSiteId = normalizeSiteId(siteId);
    var trimmedSiteName = String(siteName == null ? "" : siteName).trim();
    if (!trimmedSiteName) {
      return true;
    }
    if (normalizedSiteId && normalizeSiteId(trimmedSiteName) === normalizedSiteId) {
      return true;
    }
    return false;
  }

  function enrichSurfacedProductSiteName(product, siteName) {
    var enriched;
    if (!product || !siteName || !siteNameNeedsFallback(product.siteId || product.site_id, product.siteName || product.site_name)) {
      return product;
    }
    enriched = Object.assign({}, product);
    enriched.siteName = siteName;
    enriched.site_name = siteName;
    return enriched;
  }

  function enrichRowsWithSiteNameLookup(rows, siteNameLookup) {
    return (Array.isArray(rows) ? rows : []).map(function (row) {
      var siteId = normalizeSiteId(row && row.site_id);
      var siteName = siteId && siteNameLookup ? siteNameLookup[siteId] : "";
      var enriched;
      if (!siteName || !siteNameNeedsFallback(siteId, row && row.site_name)) {
        return row;
      }
      enriched = Object.assign({}, row);
      enriched.site_name = siteName;
      if (enriched.primaryProcessedProduct) {
        enriched.primaryProcessedProduct = enrichSurfacedProductSiteName(enriched.primaryProcessedProduct, siteName);
      }
      if (enriched.ameriFluxBaseProduct) {
        enriched.ameriFluxBaseProduct = enrichSurfacedProductSiteName(enriched.ameriFluxBaseProduct, siteName);
      }
      if (Array.isArray(enriched.surfacedProducts)) {
        enriched.surfacedProducts = enriched.surfacedProducts.map(function (product) {
          return enrichSurfacedProductSiteName(product, siteName);
        });
      }
      enriched.search_text = buildRowSearchText(enriched);
      return enriched;
    });
  }

  function enrichSitesWithMetadata(sites, siteInfoLookup, vegetationLookup) {
    return (Array.isArray(sites) ? sites : []).map(function (site) {
      var siteId = normalizeSiteId(site && site.site_id);
      var siteInfo = siteId && siteInfoLookup ? siteInfoLookup[siteId] : null;
      var enriched = Object.assign({}, site);
      var derivedCountry = deriveCountry(siteId, "");
      var vegetationType = extractVegetationMetadataValue(enriched);
      enriched.country = deriveCountry(siteId, enriched.country);
      if (vegetationType) {
        enriched.vegetation_type = vegetationType;
      } else if (siteId && vegetationLookup && vegetationLookup[siteId]) {
        enriched.vegetation_type = vegetationLookup[siteId];
      }
      if (siteInfo) {
        if (!String(enriched.site_name || "").trim() && siteInfo.site_name) {
          enriched.site_name = siteInfo.site_name;
        }
        if ((!String(enriched.country || "").trim() || String(enriched.country || "").trim() === derivedCountry) && siteInfo.country) {
          enriched.country = siteInfo.country;
        }
        if (parseCoordinate(enriched.latitude, -90, 90) == null && siteInfo.latitude != null) {
          enriched.latitude = siteInfo.latitude;
        }
        if (parseCoordinate(enriched.longitude, -180, 180) == null && siteInfo.longitude != null) {
          enriched.longitude = siteInfo.longitude;
        }
      }
      return enriched;
    });
  }

  function enrichAmeriFluxSitesWithMetadata(sites, siteInfoLookup, vegetationLookup) {
    return enrichSitesWithMetadata(sites, siteInfoLookup, vegetationLookup);
  }

  function buildFluxnet2015SiteLookup(rawRows) {
    var lookup = {};
    (Array.isArray(rawRows) ? rawRows : []).forEach(function (raw) {
      var siteId = fluxnet2015SiteInfoSiteId(raw);
      if (!siteId) {
        return;
      }
      lookup[siteId] = buildSiteInfoEntry(siteId, raw);
    });
    return lookup;
  }

  function loadFluxnet2015SiteInfo(url) {
    return fetchText(url).then(function (result) {
      return {
        lookup: buildFluxnet2015SiteLookup(csvTextToObjects(result.text)),
        source: "csv",
        sourceUrl: url,
        lastModified: result.lastModified || "",
        warning: ""
      };
    }).catch(function (error) {
      return {
        lookup: {},
        source: "csv",
        sourceUrl: url,
        lastModified: "",
        warning: "FLUXNET2015 site metadata unavailable; FLUXNET2015 API-only map coverage may be incomplete.",
        error: error
      };
    });
  }

  function enrichFluxnet2015SitesWithMetadata(sites, siteInfoLookup, vegetationLookup) {
    return enrichSitesWithMetadata(sites, siteInfoLookup, vegetationLookup);
  }

  function summarizeApiOnlyRowCoordinateCoverage(rows) {
    var summary = {
      amerifluxWithCoordinates: 0,
      amerifluxWithoutCoordinates: 0,
      fluxnet2015WithCoordinates: 0,
      fluxnet2015WithoutCoordinates: 0,
      fluxnet2015MissingSiteIds: []
    };

    (Array.isArray(rows) ? rows : []).forEach(function (row) {
      var sourceLabel = String(row && row.source_label || "").trim();
      var hasCoordinates = parseCoordinate(row && row.latitude, -90, 90) != null &&
        parseCoordinate(row && row.longitude, -180, 180) != null;

      if (sourceLabel === AMERIFLUX_SOURCE_ONLY) {
        if (hasCoordinates) {
          summary.amerifluxWithCoordinates += 1;
        } else {
          summary.amerifluxWithoutCoordinates += 1;
        }
        return;
      }

      if (sourceLabel === FLUXNET2015_SOURCE_ONLY) {
        if (hasCoordinates) {
          summary.fluxnet2015WithCoordinates += 1;
        } else {
          summary.fluxnet2015WithoutCoordinates += 1;
          if (row && row.site_id) {
            summary.fluxnet2015MissingSiteIds.push(String(row.site_id));
          }
        }
      }
    });

    summary.fluxnet2015MissingSiteIds.sort();
    return summary;
  }

  function normalizeRow(raw, index) {
    var siteId = String(raw.site_id || raw.site || "").trim();
    var siteName = String(raw.site_name || "").trim();
    var hub = String(raw.data_hub || raw.hub || "").trim();
    var network = String(raw.network || "").trim();
    var sourceNetwork = String(raw.source_network || raw.product_source_network || "").trim();
    var networkDisplay = network || sourceNetwork;
    var vegetationType = String(raw.vegetation_type || raw.igbp || raw.veg_type || "").trim();
    var country = deriveCountry(siteId, raw.country || raw.country_code || "");
    var firstYear = parseIntOrNull(raw.first_year || raw.year_start || "");
    var lastYear = parseIntOrNull(raw.last_year || raw.year_end || "");
    var directDownloadUrl = String(raw.direct_download_url || "").trim();
    var downloadLink = String(raw.download_link || raw.url || directDownloadUrl).trim();
    var latitude = extractRawLatitude(raw);
    var longitude = extractRawLongitude(raw);
    var downloadMode = String(raw.download_mode || "").trim() || "direct";
    var sourceLabel = String(raw.source_label || raw.source || "").trim();
    var sourceReason = String(raw.source_reason || "").trim();
    var apiDataProduct = String(raw.api_data_product || "").trim();
    var sourceOrigin = String(raw.source_origin || raw.catalog_source || "").trim() || SHUTTLE_SOURCE_ORIGIN;
    var sourcePriority = parseIntOrNull(raw.source_priority);
    var objectId = String(raw.object_id || "").trim();
    var fileName = String(raw.file_name || raw.filename || "").trim();
    var metadataUrl = String(raw.metadata_url || "").trim();
    var latestVersionUrl = String(raw.latest_version_url || "").trim();
    var accessUrl = String(raw.access_url || "").trim();
    var objectSpec = String(raw.object_spec || "").trim();
    var project = String(raw.project || "").trim();
    var coverageStart = String(raw.coverage_start || "").trim();
    var coverageEnd = String(raw.coverage_end || "").trim();
    var productionEnd = String(raw.production_end || "").trim();
    var citation = String(raw.citation || "").trim();
    var landingPageUrl = String(raw.landing_page_url || "").trim();
    var metadataId = String(raw.metadata_id || "").trim();
    var version = String(raw.version || "").trim();
    var productFamily = String(raw.product_family || "").trim();
    var fluxList = String(raw.flux_list || "").trim();
    var accessLabel = String(raw.access_label || raw.access_policy_label || "").trim();
    var dataUseLabel = String(raw.data_use_label || raw.data_use_policy_label || "").trim();
    var requestPageUrl = String(raw.request_page_url || "").trim();
    var lastUpdated = String(raw.last_updated || "").trim();

    if (!siteId || !hub || !downloadLink) {
      return null;
    }

    var row = {
      _index: index,
      _selection_key: hub + "|" + siteId + "|" + downloadLink,
      site_id: siteId,
      site_name: siteName,
      country: country,
      data_hub: hub,
      network: network,
      source_network: sourceNetwork,
      network_display: networkDisplay,
      network_tokens: splitNetworks(networkDisplay),
      vegetation_type: vegetationType,
      first_year: firstYear,
      last_year: lastYear,
      years: yearRangeLabel(firstYear, lastYear),
      length_years: calculateCoverageLength(firstYear, lastYear),
      latitude: latitude,
      longitude: longitude,
      download_link: downloadLink,
      download_mode: downloadMode,
      source_label: sourceLabel,
      source_reason: sourceReason,
      source_origin: sourceOrigin,
      source_priority: sourcePriority,
      product_family: productFamily,
      api_data_product: apiDataProduct,
      object_id: objectId,
      file_name: fileName,
      direct_download_url: directDownloadUrl,
      metadata_url: metadataUrl,
      latest_version_url: latestVersionUrl,
      access_url: accessUrl,
      object_spec: objectSpec,
      project: project,
      coverage_start: coverageStart,
      coverage_end: coverageEnd,
      production_end: productionEnd,
      citation: citation,
      landing_page_url: landingPageUrl,
      metadata_id: metadataId,
      version: version,
      flux_list: fluxList,
      access_label: accessLabel,
      data_use_label: dataUseLabel,
      request_page_url: requestPageUrl,
      last_updated: lastUpdated
    };
    return finalizeRowComputedState(row);
  }

  function normalizeRows(rawRows) {
    var rows = [];
    var dropped = 0;
    rawRows.forEach(function (raw, index) {
      var row = normalizeRow(raw || {}, index);
      if (row) {
        rows.push(row);
      } else {
        dropped += 1;
      }
    });
    return { rows: rows, dropped: dropped };
  }

  function extractSnapshotMeta(payload) {
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
      return {};
    }
    var meta = payload.meta;
    if (!meta || typeof meta !== "object" || Array.isArray(meta)) {
      return {};
    }
    return meta;
  }

  function buildSnapshotFreshnessKey(result) {
    if (result && result.meta && result.meta.version) {
      return [
        "meta",
        String(result.meta.version),
        String(result.meta.snapshot_updated_at || ""),
        String(result.meta.snapshot_updated_date || "")
      ].join(":");
    }
    if (result && result.lastModified) {
      return "last-modified:" + String(result.lastModified);
    }
    return "source:" + String(result && result.source ? result.source : "") + ":" + String(result && result.sourceUrl ? result.sourceUrl : "");
  }

  function fetchText(url) {
    return fetch(url).then(function (res) {
      if (!res.ok) {
        throw new Error("HTTP " + res.status + " for " + url);
      }
      return res.text().then(function (text) {
        return {
          text: text,
          lastModified: res.headers && res.headers.get ? (res.headers.get("last-modified") || "") : ""
        };
      });
    });
  }

  function fetchJson(url) {
    return fetch(url).then(function (res) {
      if (!res.ok) {
        throw new Error("HTTP " + res.status + " for " + url);
      }
      return res.json().then(function (payload) {
        return {
          payload: payload,
          lastModified: res.headers && res.headers.get ? (res.headers.get("last-modified") || "") : ""
        };
      });
    });
  }

  function loadSnapshot(jsonUrl, csvUrl) {
    function settle(promise) {
      return promise.then(function (value) {
        return { ok: true, value: value };
      }).catch(function (error) {
        return { ok: false, error: error };
      });
    }

    return Promise.all([
      settle(fetchJson(jsonUrl)),
      settle(fetchText(csvUrl))
    ]).then(function (results) {
      var jsonResult = results[0] && results[0].ok ? results[0].value : null;
      var csvResult = results[1] && results[1].ok ? results[1].value : null;
      var jsonError = results[0] && !results[0].ok ? results[0].error : null;
      var csvError = results[1] && !results[1].ok ? results[1].error : null;
      var csvRows;
      var coordinateLookup;

      if (jsonResult) {
        csvRows = csvResult ? csvTextToObjects(csvResult.text) : [];
        coordinateLookup = csvRows.length ? buildCoordinateLookup(csvRows) : null;
        return {
          rawRows: coordinateLookup
            ? enrichRowsWithCoordinateLookup(payloadJsonToObjects(jsonResult.payload), coordinateLookup)
            : payloadJsonToObjects(jsonResult.payload),
          source: "json",
          sourceUrl: jsonUrl,
          lastModified: jsonResult.lastModified || "",
          meta: extractSnapshotMeta(jsonResult.payload),
          warning: ""
        };
      }

      if (csvResult) {
        return {
          rawRows: csvTextToObjects(csvResult.text),
          source: "csv",
          sourceUrl: csvUrl,
          lastModified: csvResult.lastModified || "",
          meta: {},
          warning: "JSON snapshot unavailable; loaded CSV fallback.",
          jsonError: jsonError
        };
      }

      var error = new Error(
        "Failed to load snapshot JSON and CSV fallback. JSON error: " +
        (jsonError && jsonError.message ? jsonError.message : String(jsonError)) +
        "; CSV error: " +
        (csvError && csvError.message ? csvError.message : String(csvError))
      );
      error.jsonError = jsonError;
      error.csvError = csvError;
      throw error;
    });
  }

  function ShuttleSource(jsonUrl, csvUrl) {
    this.jsonUrl = jsonUrl;
    this.csvUrl = csvUrl;
  }

  ShuttleSource.prototype.list_sites = function () {
    return loadSnapshot(this.jsonUrl, this.csvUrl).then(function (result) {
      var normalized = normalizeRows(result.rawRows || []);
      return {
        rows: normalized.rows,
        droppedRows: normalized.dropped,
        source: result.source || "",
        sourceUrl: result.sourceUrl || "",
        warning: result.warning || "",
        lastModified: result.lastModified || "",
        meta: result.meta || {}
      };
    });
  };

  function resolveAmeriFluxIdentityFromRoot(root) {
    var globalCfg = (typeof window !== "undefined" && window && window.FLUXNET_EXPLORER_CONFIG && typeof window.FLUXNET_EXPLORER_CONFIG === "object")
      ? window.FLUXNET_EXPLORER_CONFIG
      : {};
    var cfgUserId = String(globalCfg.amerifluxUserId || "").trim();
    var cfgEmail = String(globalCfg.amerifluxUserEmail || "").trim();
    var trustedRuntime = !!(globalCfg[AMERIFLUX_TRUSTED_RUNTIME_FLAG] || globalCfg.allowBrowserAmeriFluxDownload);
    return {
      userId: cfgUserId,
      userEmail: cfgEmail,
      trustedRuntime: trustedRuntime
    };
  }

  function AmeriFluxSource(options) {
    var opts = options || {};
    this.availabilityUrl = String(opts.availabilityUrl || AMERIFLUX_FLUXNET_AVAILABILITY_URL);
    this.userId = String(opts.userId || "").trim();
    this.userEmail = String(opts.userEmail || "").trim();
    this.trustedRuntime = !!opts.trustedRuntime;
    this.dataProduct = normalizeDownloadProduct(opts.dataProduct || AMERIFLUX_FLUXNET_PRODUCT);
    this.downloadUrl = String(opts.downloadUrl || getDownloadEndpointForProduct(this.dataProduct));
    this.sourceLabel = String(opts.sourceLabel || "").trim() || (this.dataProduct === FLUXNET2015_PRODUCT
      ? FLUXNET2015_SOURCE_ONLY
      : (this.dataProduct === AMERIFLUX_BASE_PRODUCT ? BASE_SOURCE_ONLY : AMERIFLUX_SOURCE_ONLY));
    this.availabilityCacheKey = String(opts.availabilityCacheKey || (this.dataProduct === FLUXNET2015_PRODUCT
      ? FLUXNET2015_AVAILABILITY_CACHE_KEY
      : (this.dataProduct === AMERIFLUX_BASE_PRODUCT ? AMERIFLUX_BASE_AVAILABILITY_CACHE_KEY : AMERIFLUX_FLUXNET_AVAILABILITY_CACHE_KEY)));
    this.freshnessNamespace = String(opts.freshnessNamespace || this.dataProduct.toLowerCase());
    this.retryCount = parseInt(opts.retryCount, 10);
    if (!isFinite(this.retryCount)) {
      this.retryCount = MAX_HTTP_RETRIES;
    }
    this.retryBaseDelayMs = parseInt(opts.retryBaseDelayMs, 10);
    if (!isFinite(this.retryBaseDelayMs)) {
      this.retryBaseDelayMs = RETRY_BASE_DELAY_MS;
    }
  }

  AmeriFluxSource.prototype.getDownloadIdentity = function () {
    var userId = String(this.userId || "").trim();
    var userEmail = String(this.userEmail || "").trim();
    var hasCredentials = !!(userId && userEmail);
    var trustedRuntime = !!this.trustedRuntime;
    var enabled = !!(trustedRuntime && hasCredentials);
    var reason = "";
    if (!trustedRuntime) {
      reason = "manual_download_required";
    } else if (!hasCredentials) {
      reason = "missing_credentials";
    }
    return {
      user_id: userId,
      user_email: userEmail,
      enabled: enabled,
      trusted_runtime: trustedRuntime,
      has_credentials: hasCredentials,
      reason: reason
    };
  };

  AmeriFluxSource.prototype.canDownload = function () {
    return !!this.getDownloadIdentity().enabled;
  };

  AmeriFluxSource.prototype.list_sites = function () {
    var cached = readAvailabilityCache(this.availabilityCacheKey);
    var self = this;
    if (cached && cached.isFresh) {
      return Promise.resolve({
        totalSites: cached.totalSites || 0,
        sitesWithYears: cached.sitesWithYears || 0,
        sites: Array.isArray(cached.sites) ? cached.sites : [],
        warning: "",
        freshnessKey: cached.freshnessKey || "ameriflux-cache:" + String(cached.cachedAt || "")
      });
    }

    return fetchJsonWithRetry(this.availabilityUrl, {}, this.retryCount, this.retryBaseDelayMs)
      .then(function (result) {
        var parsed = parseAmeriFluxAvailabilityPayload(result.payload || {}, self.freshnessNamespace);
        writeAvailabilityCache(self.availabilityCacheKey, parsed);
        return {
          totalSites: parsed.totalSites,
          sitesWithYears: parsed.sitesWithYears,
          sites: parsed.sites,
          warning: "",
          freshnessKey: parsed.freshnessKey
        };
      })
      .catch(function (error) {
        if (cached && Array.isArray(cached.sites) && cached.sites.length) {
          return {
            totalSites: cached.totalSites || 0,
            sitesWithYears: cached.sitesWithYears || 0,
            sites: cached.sites,
            warning: "AmeriFlux availability refresh failed; using cached AmeriFlux availability.",
            freshnessKey: cached.freshnessKey || "ameriflux-cache:" + String(cached.cachedAt || "")
          };
        }
        throw error;
      });
  };

  AmeriFluxSource.prototype.buildDownloadPayload = function (siteIds, variant, policy, identityOverride) {
    var identity = identityOverride || this.getDownloadIdentity();
    return buildDownloadPayloadForProduct(siteIds, variant, policy, identity, this.dataProduct);
  };

  AmeriFluxSource.prototype.getManualDownloadResult = function (siteId, variant, policy, reason, identityOverride) {
    var site = String(siteId || "").trim();
    var manualReason = String(reason || "").trim();
    var resolvedIdentity = resolveAmeriFluxIdentityOverride(identityOverride);
    var payloadTemplate = this.buildDownloadPayload([site || "SITE_ID_HERE"], variant, policy, {
      user_id: resolvedIdentity ? resolvedIdentity.user_id : AMERIFLUX_TEMPLATE_USER_ID,
      user_email: resolvedIdentity ? resolvedIdentity.user_email : AMERIFLUX_TEMPLATE_USER_EMAIL
    });
    return {
      mode: "manual",
      manual_download_required: true,
      site_id: site,
      reason: manualReason || "AmeriFlux downloads require configured credentials in a trusted runtime context.",
      message: this.sourceLabel + " downloads require your own AmeriFlux identity. Copy and run the generated curl command locally.",
      payload_template: payloadTemplate,
      curl_command: buildAmeriFluxCurlCommand(site, variant, policy, this.downloadUrl, this.dataProduct, resolvedIdentity || undefined),
      manifest: {},
      data_urls: []
    };
  };

  AmeriFluxSource.prototype.get_download_urls = function (siteId, variant, policy, identityOverride) {
    var runtimeIdentity = this.getDownloadIdentity();
    var site = String(siteId || "").trim();
    if (!site) {
      return Promise.reject(new Error("AmeriFlux download requires a site_id."));
    }
    if (!runtimeIdentity.enabled) {
      var reason = !runtimeIdentity.trusted_runtime
        ? "AmeriFlux API downloads are disabled in this browser runtime."
        : "AMERIFLUX_USER_ID or AMERIFLUX_USER_EMAIL is missing.";
      return Promise.resolve(this.getManualDownloadResult(site, variant, policy, reason, identityOverride));
    }

    var requestIdentity = resolveAmeriFluxIdentityOverride(identityOverride) || runtimeIdentity;
    var identity = {
      user_id: requestIdentity.user_id,
      user_email: requestIdentity.user_email
    };
    var payload = this.buildDownloadPayload([site], variant, policy, identity);
    return fetchJsonWithRetry(this.downloadUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json"
      },
      body: JSON.stringify(payload)
    }, this.retryCount, this.retryBaseDelayMs).then(function (result) {
      var responsePayload = result.payload || {};
      var dataUrls = Array.isArray(responsePayload.data_urls) ? responsePayload.data_urls : [];
      if (!dataUrls.length) {
        throw new Error("AmeriFlux download request succeeded but returned empty data_urls for " + site + ".");
      }
      return {
        mode: "api",
        manual_download_required: false,
        manifest: responsePayload.manifest || {},
        data_urls: dataUrls
      };
    }).catch(function (error) {
      var detail = error && error.message ? error.message : String(error);
      throw new Error("AmeriFlux download request failed for " + site + ": " + detail);
    });
  };

  AmeriFluxSource.prototype.download_site = function (siteId, variant, policy, identityOverride) {
    return this.get_download_urls(siteId, variant, policy, identityOverride);
  };

  function compareRows(a, b, sortKey, sortDir) {
    var dir = sortDir === "desc" ? -1 : 1;
    var av;
    var bv;

    if (sortKey === "years") {
      av = a.first_year == null ? Number.POSITIVE_INFINITY : a.first_year;
      bv = b.first_year == null ? Number.POSITIVE_INFINITY : b.first_year;
      if (av !== bv) {
        return (av < bv ? -1 : 1) * dir;
      }
      av = a.last_year == null ? Number.POSITIVE_INFINITY : a.last_year;
      bv = b.last_year == null ? Number.POSITIVE_INFINITY : b.last_year;
      if (av !== bv) {
        return (av < bv ? -1 : 1) * dir;
      }
    } else {
      av = a[sortKey];
      bv = b[sortKey];
      if (typeof av === "number" || typeof bv === "number") {
        av = av == null ? Number.POSITIVE_INFINITY : av;
        bv = bv == null ? Number.POSITIVE_INFINITY : bv;
        if (av !== bv) {
          return (av < bv ? -1 : 1) * dir;
        }
      } else {
        av = String(av || "").toLowerCase();
        bv = String(bv || "").toLowerCase();
        if (av !== bv) {
          return (av < bv ? -1 : 1) * dir;
        }
      }
    }

    var aHub = String(a.data_hub || "").toLowerCase();
    var bHub = String(b.data_hub || "").toLowerCase();
    if (aHub !== bHub) {
      return aHub < bHub ? -1 : 1;
    }
    var aSite = String(a.site_id || "").toLowerCase();
    var bSite = String(b.site_id || "").toLowerCase();
    if (aSite !== bSite) {
      return aSite < bSite ? -1 : 1;
    }
    return a._index - b._index;
  }

  function buildAttributionText(snapshotUpdatedDate) {
    return "We appreciate acknowledgement of the QED FLUXNET Data Explorer when convenient. Available data is updated as of: " + snapshotUpdatedDateDisplayText(snapshotUpdatedDate) + ". Contact TF Keenan (trevorkeenan@berkeley.edu) with any questions or suggestions. Funding for the FLUXNET Data Explorer was generously provided by the NSF AccelNet program.";
  }

  function buildAttributionHtml(snapshotUpdatedDate) {
    return "We appreciate acknowledgement of the QED FLUXNET Data Explorer when convenient. Available data is updated as of: " + escapeHtml(snapshotUpdatedDateDisplayText(snapshotUpdatedDate)) + ". Contact TF Keenan (<a href=\"mailto:trevorkeenan@berkeley.edu\">trevorkeenan@berkeley.edu</a>) with any questions or suggestions. Funding for the FLUXNET Data Explorer was generously provided by the NSF AccelNet program.";
  }

  function csvEscape(value) {
    var s = String(value == null ? "" : value);
    if (/[",\r\n]/.test(s)) {
      return "\"" + s.replace(/"/g, "\"\"") + "\"";
    }
    return s;
  }

  function combineWarnings() {
    var parts = [];
    var i;
    for (i = 0; i < arguments.length; i += 1) {
      var value = String(arguments[i] || "").trim();
      if (value) {
        parts.push(value);
      }
    }
    return parts.join(" ");
  }

  function shouldEnableBulkToolsActions(selectedCount) {
    return (parseIntOrNull(selectedCount) || 0) > 1;
  }

  function formatSelectedSiteCount(selectedCount) {
    var count = parseIntOrNull(selectedCount) || 0;
    return count + " selected " + (count === 1 ? "site" : "sites");
  }

  function normalizeClipboardCellText(value) {
    return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
  }

  function tableClipboardCellValue(row, columnKey) {
    var value;
    if (!row || !columnKey) {
      return "\u2014";
    }
    if (columnKey === "length_years") {
      return row.length_years == null ? "\u2014" : String(row.length_years);
    }
    value = normalizeClipboardCellText(row[columnKey]);
    return value || "\u2014";
  }

  function buildTableClipboardText(rows) {
    var visibleRows = Array.isArray(rows) ? rows : [];
    var lines = [
      SORT_COLUMNS.map(function (column) {
        return normalizeClipboardCellText(column.label);
      }).join("\t")
    ];
    visibleRows.forEach(function (row) {
      lines.push(SORT_COLUMNS.map(function (column) {
        return tableClipboardCellValue(row, column.key);
      }).join("\t"));
    });
    return lines.join("\n") + "\n";
  }

  function sourceBadgeClass(sourceLabel) {
    if (sourceLabel === ICOS_DIRECT_SOURCE_ONLY) {
      return "shuttle-explorer__source-badge--icos";
    }
    if (sourceLabel === SOURCE_FILTER_TAG_EFD) {
      return "shuttle-explorer__source-badge--efd";
    }
    if (sourceLabel === SOURCE_FILTER_TAG_JAPANFLUX) {
      return "shuttle-explorer__source-badge--japanflux";
    }
    if (sourceLabel === BASE_SOURCE_ONLY) {
      return "shuttle-explorer__source-badge--base";
    }
    if (sourceLabel === AMERIFLUX_SOURCE_ONLY) {
      return "shuttle-explorer__source-badge--ameriflux";
    }
    if (sourceLabel === FLUXNET2015_SOURCE_ONLY) {
      return "shuttle-explorer__source-badge--fluxnet2015";
    }
    if (sourceLabel === AMERIFLUX_SHUTTLE) {
      return "shuttle-explorer__source-badge--ameriflux-shuttle";
    }
    return "";
  }

  function renderSourceBadgeHtml(sourceLabel, sourceReason) {
    var label = String(sourceLabel || "").trim();
    if (!label) {
      return "<span class=\"shuttle-explorer__muted\">\u2014</span>";
    }
    var klass = sourceBadgeClass(label);
    var title = sourceReason ? (" title=\"" + escapeHtml(sourceReason) + "\"") : "";
    return "<span class=\"shuttle-explorer__source-badge " + escapeHtml(klass) + "\"" + title + ">" + escapeHtml(label) + "</span>";
  }

  function renderSurfacedCoverageHtml(row) {
    var products = getSurfacedProductsForRow(row);
    var content;
    var badgeHtml = "";

    if (!products.length) {
      return escapeHtml(String(row && row.years || "\u2014"));
    }

    content = products.map(function (product) {
      return "<span class=\"shuttle-explorer__coverage-item\"><strong>" +
        escapeHtml(surfacedProductDisplayName(product, false)) +
        ":</strong> " +
        escapeHtml(String(product && product.coverageLabel || "\u2014")) +
        "</span>";
    }).join("<span class=\"shuttle-explorer__coverage-sep\" aria-hidden=\"true\">\u00b7</span>");

    if (row && row.surfacedProductClassification === SURFACED_CLASSIFICATION_FLUXNET_AND_OTHER) {
      badgeHtml = "<div class=\"shuttle-explorer__coverage-badge\"><span class=\"shuttle-explorer__source-badge shuttle-explorer__source-badge--base-addition\">" + escapeHtml(TABLE_LABEL_FLUXNET_AND_OTHER) + "</span></div>";
    }

    return "<div class=\"shuttle-explorer__coverage-list\">" + content + "</div>" + badgeHtml;
  }

  function buildRowDownloadOptions(row, canAmeriFluxDownload) {
    return getRowActionProducts(row).map(function (product) {
      var dataProduct = normalizeDownloadProduct(product && product.apiDataProduct);
      var option = {
        product: product,
        displayLabel: String(product && (product.displayLabel || product.display_label) || "").trim() || surfacedProductDisplayName(product, true),
        mode: String(product && product.downloadMode || ""),
        siteId: String(product && product.siteId || ""),
        sourceLabel: String(product && product.sourceLabel || ""),
        sourceOrigin: String(product && (product.sourceOrigin || product.source_origin) || ""),
        dataProduct: dataProduct,
        downloadLink: String(product && product.downloadLink || ""),
        isIcos: !!(product && product.isIcos),
        title: ""
      };

      if (option.mode === "ameriflux_api") {
        option.actionLabel = canAmeriFluxDownload ? getApiActionRequestLabel(dataProduct) : getApiActionCopyLabel(dataProduct);
        if (!canAmeriFluxDownload) {
          option.title = apiProductDisplayName(dataProduct) + " downloads require your own AmeriFlux identity. Click to copy a curl command template.";
        }
      } else if (option.mode === REQUEST_PAGE_DOWNLOAD_MODE) {
        option.actionLabel = option.sourceOrigin === EFD_SOURCE_ORIGIN || option.sourceLabel === SOURCE_FILTER_TAG_EFD
          ? "Request at EFD"
          : "Open request page";
        option.title = "Login is required. Some EFD data may require PI approval, and download links are emailed after request submission.";
      } else if (option.mode === LANDING_PAGE_DOWNLOAD_MODE) {
        option.actionLabel = "Open landing page";
        option.title = "Direct ZIP URL could not be validated automatically; open the ADS landing page to download manually.";
      } else {
        option.actionLabel = option.isIcos ? "Accept ICOS license and download" : "Download";
      }

      return option;
    });
  }

  function ensureStyles() {
    if (document.querySelector("link[href*='shuttle-explorer.css']")) {
      return;
    }
    if (document.getElementById(STYLE_ID)) {
      return;
    }
    var style = document.createElement("style");
    style.id = STYLE_ID;
    style.type = "text/css";
    style.textContent = [
      ".shuttle-explorer{max-width:1100px;margin:24px auto 0;padding:10px;border:1px solid #d5dbe3;border-radius:8px;background:#fff;}",
      ".shuttle-explorer *{box-sizing:border-box;}",
      ".shuttle-explorer h2{margin:0 0 6px 0;}",
      ".shuttle-explorer p{margin:0 0 8px 0;line-height:1.4;}",
      ".shuttle-explorer__status{margin:0 0 10px 0;padding:8px 10px;border-radius:6px;background:#f8fafc;color:#33475b;font-size:.9em;}",
      ".shuttle-explorer__status.is-error{background:#fff3f3;color:#8b1e1e;border:1px solid #f2c7c7;}",
      ".shuttle-explorer__status.is-loading{background:#f2f7ff;color:#234d7b;border:1px solid #d6e5fb;}",
      ".shuttle-explorer__status.is-ok{background:#f3fbf4;color:#225e2b;border:1px solid #cfe8d4;}",
      ".shuttle-explorer__controls{display:grid;grid-template-columns:2fr 1fr 1fr 1fr 1fr 1fr;gap:10px;margin:0 0 10px;}",
      ".shuttle-explorer__field{display:flex;flex-direction:column;gap:4px;}",
      ".shuttle-explorer__field label{font-size:.82em;color:#4d5b6a;}",
      ".shuttle-explorer__label-row{display:inline-flex;align-items:center;gap:6px;}",
      ".shuttle-explorer__tooltip-wrap{position:relative;display:inline-flex;align-items:center;}",
      ".shuttle-explorer__tooltip-toggle{display:inline-flex;align-items:center;justify-content:center;width:18px;height:18px;padding:0;border:1px solid #b7c1ce;border-radius:999px;background:#fff;color:#2f5374;font-size:.74em;font-weight:700;line-height:1;cursor:help;}",
      ".shuttle-explorer__tooltip-toggle:hover,.shuttle-explorer__tooltip-toggle:focus{background:#eef3f9;}",
      ".shuttle-explorer__tooltip-toggle:focus{outline:2px solid #2f5374;outline-offset:1px;}",
      ".shuttle-explorer__tooltip{position:absolute;top:calc(100% + 6px);right:0;z-index:5;display:block;width:min(260px,calc(100vw - 40px));padding:8px 10px;border:1px solid #d5dbe3;border-radius:8px;background:#fff;box-shadow:0 8px 18px rgba(35,54,74,.12);color:#33475b;font-size:.82em;line-height:1.4;opacity:0;visibility:hidden;pointer-events:none;transform:translateY(-2px);transition:opacity .12s ease,transform .12s ease,visibility .12s ease;}",
      ".shuttle-explorer__tooltip-wrap.is-open .shuttle-explorer__tooltip,.shuttle-explorer__tooltip-wrap:hover .shuttle-explorer__tooltip,.shuttle-explorer__tooltip-wrap:focus-within .shuttle-explorer__tooltip{opacity:1;visibility:visible;pointer-events:auto;transform:translateY(0);}",
      ".shuttle-explorer__tooltip a{color:#2f5374;}",
      ".shuttle-explorer__tooltip a:hover,.shuttle-explorer__tooltip a:focus{text-decoration:underline;}",
      ".shuttle-explorer__field input,.shuttle-explorer__field select{width:100%;padding:7px 8px;border:1px solid #b7c1ce;border-radius:6px;background:#fff;font:inherit;}",
      ".shuttle-explorer__hub-filters{display:flex;flex-wrap:wrap;gap:8px;margin:0 0 10px;}",
      ".shuttle-explorer__hub-chip{display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border:1px solid #d5dbe3;border-radius:999px;background:#f8fafc;font-size:.88em;}",
      ".shuttle-explorer__row{display:flex;justify-content:space-between;align-items:center;gap:10px;margin:0 0 10px;}",
      ".shuttle-explorer__selection-actions{display:flex;flex-wrap:wrap;gap:8px;margin:0 0 10px;}",
      ".shuttle-explorer__bulk{margin:0 0 10px;border:1px solid #d5dbe3;border-radius:8px;background:#fbfdff;}",
      ".shuttle-explorer__bulk-summary{margin:0;padding:10px;cursor:pointer;font-weight:600;font-size:.88em;color:#23364a;}",
      ".shuttle-explorer__bulk-summary-label{font-size:inherit;}",
      ".shuttle-explorer__bulk-summary-count{display:inline-block;margin-left:8px;color:#556779;font-size:.92em;font-weight:400;}",
      ".shuttle-explorer__bulk[open] .shuttle-explorer__bulk-summary{margin:0 0 8px 0;border-bottom:1px solid #e7edf4;}",
      ".shuttle-explorer__bulk-body{display:block;padding:0 10px 10px;}",
      ".shuttle-explorer__bulk-header{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;margin:0 0 8px;}",
      ".shuttle-explorer__bulk-actions{display:flex;flex-wrap:wrap;gap:8px;margin:8px 0 0;}",
      ".shuttle-explorer__bulk-source{margin:10px 0 0;padding:10px;border:1px solid #dce3eb;border-radius:8px;background:#ffffff;}",
      ".shuttle-explorer__bulk-identity-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:8px 0 0;}",
      ".shuttle-explorer__bulk-source h4{margin:0;font-size:.95em;}",
      ".shuttle-explorer__bulk-guide{margin:10px 0 0;}",
      ".shuttle-explorer__cli-panel{margin:10px 0 0;padding:10px;border:1px solid #dce3eb;border-radius:8px;background:#ffffff;}",
      ".shuttle-explorer__cli-pre{margin:8px 0 0;padding:10px;border-radius:6px;background:#f3f6fa;overflow:auto;font-size:.82em;line-height:1.35;}",
      ".shuttle-explorer__summary{font-size:.9em;color:#33475b;}",
      ".shuttle-explorer__btn{display:inline-block;padding:7px 10px;border:1px solid #b7c1ce;border-radius:6px;background:#fff;color:#23364a;text-decoration:none;font:inherit;cursor:pointer;}",
      ".shuttle-explorer__btn:hover,.shuttle-explorer__btn:focus{background:#eef3f9;text-decoration:none;}",
      ".shuttle-explorer__btn:focus{outline:2px solid #2f5374;outline-offset:1px;}",
      ".shuttle-explorer__btn--small{padding:5px 8px;font-size:.86em;}",
      ".shuttle-explorer__btn[disabled]{opacity:.45;cursor:default;}",
      ".shuttle-explorer__table-wrap{overflow:auto;border:1px solid #d5dbe3;border-radius:8px;background:#fff;}",
      ".shuttle-explorer__table{width:100%;border-collapse:collapse;min-width:880px;font-size:.9em;}",
      ".shuttle-explorer__table th,.shuttle-explorer__table td{padding:8px 10px;border-bottom:1px solid #edf1f5;vertical-align:top;text-align:left;}",
      ".shuttle-explorer__table thead th{position:sticky;top:0;background:#f8fafc;z-index:1;}",
      ".shuttle-explorer__source-badge{display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;font-size:.86em;font-weight:600;line-height:1.25;}",
      ".shuttle-explorer__source-badge--icos{background:#eef7f8;border:1px solid #b7d9dc;color:#245761;}",
      ".shuttle-explorer__source-badge--efd{background:#f9efe5;border:1px solid #dfc09a;color:#7b4a13;}",
      ".shuttle-explorer__source-badge--japanflux{background:#f4efe7;border:1px solid #d7b99a;color:#704624;}",
      ".shuttle-explorer__source-badge--base{background:#f8f3e8;border:1px solid #d9c288;color:#755319;}",
      ".shuttle-explorer__source-badge--ameriflux{background:#edf7f0;border:1px solid #b6dcc2;color:#1f6c3f;}",
      ".shuttle-explorer__source-badge--fluxnet2015{background:#fff4e5;border:1px solid #ebc47d;color:#8a5600;}",
      ".shuttle-explorer__source-badge--ameriflux-shuttle{background:#eef3fa;border:1px solid #b8c9e4;color:#2a4f7a;}",
      ".shuttle-explorer__source-badge--base-addition{background:#f4efe2;border:1px solid #d4bf92;color:#6f5220;}",
      ".shuttle-explorer__site-badge{margin-top:4px;}",
      ".shuttle-explorer__coverage-list{display:flex;flex-wrap:wrap;align-items:center;gap:6px;}",
      ".shuttle-explorer__coverage-item{display:inline-flex;align-items:center;gap:4px;}",
      ".shuttle-explorer__coverage-sep{color:#607184;}",
      ".shuttle-explorer__coverage-badge{margin-top:6px;}",
      ".shuttle-explorer__download-cell{min-width:220px;}",
      ".shuttle-explorer__download-option + .shuttle-explorer__download-option{margin-top:8px;}",
      ".shuttle-explorer__download-option-label{margin:0 0 4px;color:#556779;font-size:.8em;font-weight:600;line-height:1.25;}",
      ".shuttle-explorer__sort{display:inline-flex;align-items:center;gap:4px;border:0;background:transparent;padding:0;margin:0;color:inherit;font:inherit;cursor:pointer;}",
      ".shuttle-explorer__sort-indicator{color:#6b7a89;font-size:.9em;}",
      ".shuttle-explorer__table-copy-btn{display:inline-flex;max-width:11rem;padding:0;margin:0;border:0;background:transparent;color:#2f5374;font:inherit;font-weight:600;line-height:1.25;text-align:left;cursor:pointer;white-space:normal;}",
      ".shuttle-explorer__table-copy-btn:hover,.shuttle-explorer__table-copy-btn:focus{text-decoration:underline;}",
      ".shuttle-explorer__table-copy-btn:focus{outline:2px solid #2f5374;outline-offset:2px;border-radius:4px;}",
      ".shuttle-explorer__table-copy-btn.is-success{color:#25673a;}",
      ".shuttle-explorer__table-copy-btn.is-error{color:#a22f2f;}",
      ".shuttle-explorer__muted{color:#607184;}",
      ".shuttle-explorer__header p + p{margin-top:12px;}",
      ".shuttle-explorer__pagination{display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin:10px 0 0;}",
      ".shuttle-explorer__pages{display:flex;flex-wrap:wrap;gap:6px;}",
      ".shuttle-explorer__page.is-active{background:#e8f0fb;border-color:#a8bddb;}",
      ".shuttle-explorer__empty{margin:10px 0 0;padding:12px;border:1px dashed #c6d1dd;border-radius:8px;background:#fbfdff;color:#3a4d60;}",
      ".shuttle-explorer__hidden{display:none !important;}",
      ".shuttle-explorer__attribution{margin:12px 0 0;padding:10px;border:1px solid #d5dbe3;border-radius:8px;background:#ffffff;}",
      ".shuttle-explorer__attribution h3{margin:0 0 6px;font-size:.95em;}",
      ".shuttle-explorer__attribution ul{margin:8px 0 0 18px;padding:0;}",
      ".shuttle-explorer__attribution li + li{margin-top:6px;}",
      ".shuttle-explorer__attribution-row{display:flex;justify-content:space-between;align-items:center;gap:10px;margin:8px 0 0;}",
      ".shuttle-explorer__tiny{font-size:.82em;color:#556779;}",
      "@media (max-width: 860px){.shuttle-explorer__controls{grid-template-columns:1fr;}.shuttle-explorer__row{flex-direction:column;align-items:flex-start;}.shuttle-explorer__bulk-identity-grid{grid-template-columns:1fr;}}"
    ].join("");
    document.head.appendChild(style);
  }

  function createLayout(root) {
    root.classList.add("shuttle-explorer");
    root.innerHTML = [
      "<div class=\"shuttle-explorer__header\">",
      "  <h2>FLUXNET Data Explorer</h2>",
      "  <p class=\"shuttle-explorer__muted\">Explore and download a regularly refreshed snapshot of FLUXNET Shuttle coverage, selected direct FLUXNET archive supplements, JapanFlux2024 archive records, and AmeriFlux BASE (standardized observations) coverage (last updated: <span data-role=\"widget-last-updated-inline\">unavailable</span>). Search by site ID or site name, then open the download or landing-page links shown in the table. This explorer keeps source provenance explicit instead of folding every record into the FLUXNET Shuttle bucket.</p>",
      "  <p class=\"shuttle-explorer__muted\">Data are provided by site teams around the world. Shuttle-backed FLUXNET rows are served via the FLUXNET Shuttle (<a href=\"https://data.fluxnet.org/\" target=\"_blank\" rel=\"noopener\">https://data.fluxnet.org/</a>), additional FLUXNET rows may be surfaced through ICOS and AmeriFlux APIs, and JapanFlux2024 rows are surfaced separately from the ADS archive because they use FLUXNET-style conventions with dataset-specific adaptations.</p>",
      "</div>",
      "<p class=\"shuttle-explorer__status is-loading\" data-role=\"status\" role=\"status\" aria-live=\"polite\">Loading snapshot…</p>",
      "<div class=\"shuttle-explorer__controls shuttle-explorer__hidden\" data-role=\"controls\">",
      "  <div class=\"shuttle-explorer__field\">",
      "    <label for=\"shuttle-search\">Search (site ID or site name)</label>",
      "    <input id=\"shuttle-search\" type=\"search\" placeholder=\"e.g., US-Ton or Tonzi\" data-role=\"search\" />",
      "  </div>",
      "  <div class=\"shuttle-explorer__field\">",
      "    <label for=\"shuttle-availability\">Availability</label>",
      "    <select id=\"shuttle-availability\" data-role=\"availability-filter\"><option value=\"\">All sites</option></select>",
      "  </div>",
      "  <div class=\"shuttle-explorer__field\">",
      "    <label for=\"shuttle-source\">Source</label>",
      "    <select id=\"shuttle-source\" data-role=\"source-filter\"><option value=\"\">All sources</option></select>",
      "  </div>",
      "  <div class=\"shuttle-explorer__field\">",
      "    <label for=\"shuttle-network\">Network</label>",
      "    <select id=\"shuttle-network\" data-role=\"network-filter\"><option value=\"\">All networks</option></select>",
      "  </div>",
      "  <div class=\"shuttle-explorer__field\">",
      "    <label for=\"shuttle-country\">Country</label>",
      "    <select id=\"shuttle-country\" data-role=\"country-filter\"><option value=\"\">All countries</option></select>",
      "  </div>",
      "  <div class=\"shuttle-explorer__field\">",
      "    <div class=\"shuttle-explorer__label-row\">",
      "      <label for=\"shuttle-vegetation\">Veg. type</label>",
      "      <span class=\"shuttle-explorer__tooltip-wrap\" data-role=\"vegetation-info-wrap\">",
      "        <button type=\"button\" class=\"shuttle-explorer__tooltip-toggle\" data-role=\"vegetation-info-toggle\" aria-label=\"About IGBP vegetation codes\" aria-describedby=\"shuttle-vegetation-tooltip\" aria-expanded=\"false\">i</button>",
      "        <span class=\"shuttle-explorer__tooltip\" id=\"shuttle-vegetation-tooltip\" role=\"tooltip\">Vegetation codes follow IGBP classifications as outlined <a href=\"https://fluxnet.org/data/badm-data-templates/igbp-classification/\" target=\"_blank\" rel=\"noopener noreferrer\">here</a></span>",
      "      </span>",
      "    </div>",
      "    <select id=\"shuttle-vegetation\" data-role=\"vegetation-filter\"><option value=\"\">All vegetation types</option></select>",
      "  </div>",
      "</div>",
      "<div class=\"shuttle-explorer__hub-filters shuttle-explorer__hidden\" data-role=\"hub-filters\" aria-label=\"Hub filters\"></div>",
      "<div class=\"shuttle-explorer__row shuttle-explorer__hidden\" data-role=\"summary-row\">",
      "  <div class=\"shuttle-explorer__summary\" data-role=\"summary\"></div>",
      "  <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"reset\">Reset filters</button>",
      "</div>",
      "<div class=\"shuttle-explorer__selection-actions shuttle-explorer__hidden\" data-role=\"selection-actions\">",
      "  <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"select-filtered\">Select all (filtered results)</button>",
      "  <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"select-all-sites\">Select all (all sites)</button>",
      "  <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"clear-selection\">Clear selection</button>",
      "</div>",
      "<details class=\"shuttle-explorer__bulk shuttle-explorer__hidden\" data-role=\"bulk-panel\">",
      "  <summary class=\"shuttle-explorer__bulk-summary\">",
      "    <span class=\"shuttle-explorer__bulk-summary-label\">Bulk download tools</span>",
      "    <span class=\"shuttle-explorer__bulk-summary-count\" data-role=\"selection-count\">0 selected sites</span>",
      "  </summary>",
      "  <div class=\"shuttle-explorer__bulk-body\">",
      "  <div class=\"shuttle-explorer__bulk-actions shuttle-explorer__hidden\" data-role=\"all-selected-actions\">",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"download-all-selected-script\">Download all selected scripts</button>",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"copy-all-selected-script\">Copy download all selected script</button>",
      "  </div>",
      "  <section class=\"shuttle-explorer__bulk-source shuttle-explorer__hidden\" data-role=\"shuttle-bulk-section\" aria-labelledby=\"shuttle-bulk-source-heading\">",
      "    <div class=\"shuttle-explorer__bulk-header\">",
      "      <h4 id=\"shuttle-bulk-source-heading\">Bulk download for direct-link and Shuttle catalog rows</h4>",
      "      <p class=\"shuttle-explorer__tiny\" data-role=\"shuttle-selection-count\">0 direct-source sites selected</p>",
      "    </div>",
      "    <p class=\"shuttle-explorer__tiny\">Applies to validated direct-download rows from the FLUXNET Shuttle, ICOS direct links, and JapanFlux direct links. Landing-page-only rows remain in the manifest but are excluded from the links and shell-script outputs.</p>",
      "    <div class=\"shuttle-explorer__bulk-actions\">",
      "      <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"download-script\">Download download_shuttle_selected.sh</button>",
      "    </div>",
      "    <div class=\"shuttle-explorer__bulk-actions\">",
      "      <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"download-manifest\">Download shuttle_selected_manifest.csv</button>",
      "      <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"download-links\">Download shuttle_links.txt</button>",
      "      <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"download-sites-file\">Download shuttle_selected_sites.txt</button>",
      "    </div>",
      "    <div class=\"shuttle-explorer__bulk-actions\">",
      "      <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"show-cli-command\">Show Shuttle CLI command</button>",
      "      <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"copy-command\">Copy Shuttle CLI command</button>",
      "      <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"copy-links\">Copy Shuttle links</button>",
      "    </div>",
      "  </section>",
      "  <section class=\"shuttle-explorer__bulk-source shuttle-explorer__hidden\" data-role=\"ameriflux-bulk-section\" aria-labelledby=\"ameriflux-bulk-source-heading\">",
      "    <div class=\"shuttle-explorer__bulk-header\">",
      "      <h4 id=\"ameriflux-bulk-source-heading\">Bulk download for surfaced AmeriFlux API products</h4>",
      "      <p class=\"shuttle-explorer__tiny\" data-role=\"ameriflux-selection-count\">0 sites available elsewhere selected</p>",
      "    </div>",
      "    <p class=\"shuttle-explorer__tiny\">Applies to surfaced AmeriFlux API products selected in the table. This includes AmeriFlux FLUXNET rows, BASE rows, FLUXNET2015 fallback rows, and any additional non-ONEFlux processed years surfaced alongside a FLUXNET-processed product.</p>",
      "    <p class=\"shuttle-explorer__tiny\">Optional: enter your own AmeriFlux username and email. If left blank, the generated script will use default values.</p>",
      "    <div class=\"shuttle-explorer__bulk-identity-grid\">",
      "      <div class=\"shuttle-explorer__field\">",
      "        <label>AmeriFlux username (optional)</label>",
      "        <input type=\"text\" autocomplete=\"username\" data-role=\"ameriflux-bulk-user-id\" />",
      "      </div>",
      "      <div class=\"shuttle-explorer__field\">",
      "        <label>AmeriFlux email (optional)</label>",
      "        <input type=\"email\" autocomplete=\"email\" data-role=\"ameriflux-bulk-user-email\" />",
      "      </div>",
      "    </div>",
      "    <div class=\"shuttle-explorer__bulk-actions\">",
      "      <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"download-ameriflux-script\">Download download_ameriflux_selected.sh</button>",
      "    </div>",
      "    <div class=\"shuttle-explorer__bulk-actions\">",
      "      <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"copy-ameriflux-script\">Copy AmeriFlux API shell script</button>",
      "      <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"download-ameriflux-sites-file\">Download ameriflux_selected_sites.txt</button>",
      "    </div>",
      "  </section>",
      "  <details class=\"shuttle-explorer__bulk-guide\">",
      "    <summary>How to use these bulk tools</summary>",
      "    <ul>",
      "      <li><strong>download_all_selected.sh</strong>: wrapper script that runs the direct-link and AmeriFlux API bulk scripts in sequence when the three generated scripts are kept in the same directory.</li>",
      "      <li><strong>Shuttle script</strong>: uses validated direct URLs plus retries/resume support for non-AmeriFlux rows that expose direct downloads.</li>",
      "      <li><strong>AmeriFlux API script</strong>: requests URLs dynamically for the surfaced AmeriFlux API products selected in the table, using <code>/api/v2/data_download</code> for FLUXNET and BASE, and <code>/api/v1/data_download</code> for FLUXNET2015, then downloads each returned file.</li>",
      "      <li><strong>Show Shuttle CLI command</strong>: reveals a command template that uses <code>shuttle_selected_sites.txt</code> and your local snapshot file.</li>",
      "      <li><strong>Copy Shuttle CLI command</strong>: copies the Shuttle CLI helper command shown in the panel.</li>",
      "      <li><strong>shuttle_selected_sites.txt</strong>: one <code>site_id</code> per line for Shuttle CLI workflows.</li>",
      "      <li><strong>ameriflux_selected_sites.txt</strong>: tab-delimited <code>site_id</code>, <code>data_product</code>, and <code>source_label</code> for AmeriFlux API-backed workflows.</li>",
      "      <li><strong>shuttle_selected_manifest.csv</strong>: selected non-AmeriFlux rows with source metadata, download modes, and links.</li>",
      "      <li><strong>shuttle_links.txt / Copy Shuttle links</strong>: validated direct URLs only (AmeriFlux rows, landing-page-only rows, and request-only rows are excluded).</li>",
      "    </ul>",
      "  </details>",
      "  <div class=\"shuttle-explorer__cli-panel shuttle-explorer__hidden\" data-role=\"cli-panel\">",
      "    <p class=\"shuttle-explorer__tiny\">The FLUXNET Shuttle CLI supports <code>download -f SNAPSHOT.csv -s SITE1 SITE2 ...</code> but does not provide a sites-file option. Use the helper command below with <code>shuttle_selected_sites.txt</code>.</p>",
      "    <pre class=\"shuttle-explorer__cli-pre\" data-role=\"cli-command\"></pre>",
      "  </div>",
      "  <p class=\"shuttle-explorer__tiny shuttle-explorer__bulk-status\" data-role=\"bulk-status\" aria-live=\"polite\"></p>",
      "  </div>",
      "</details>",
      "<div class=\"shuttle-explorer__table-wrap shuttle-explorer__hidden\" data-role=\"table-wrap\">",
      "  <table class=\"shuttle-explorer__table\" data-role=\"table\">",
      "    <thead><tr data-role=\"thead-row\"></tr></thead>",
      "    <tbody data-role=\"tbody\"></tbody>",
      "  </table>",
      "</div>",
      "<div class=\"shuttle-explorer__empty shuttle-explorer__hidden\" data-role=\"empty\"></div>",
      "<div class=\"shuttle-explorer__pagination shuttle-explorer__hidden\" data-role=\"pagination\">",
      "  <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"prev-page\">Previous</button>",
      "  <div class=\"shuttle-explorer__pages\" data-role=\"page-buttons\"></div>",
      "  <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"next-page\">Next</button>",
      "  <span class=\"shuttle-explorer__tiny shuttle-explorer__page-summary\" data-role=\"page-summary\"></span>",
      "</div>",
      "<section class=\"shuttle-explorer__map-panel shuttle-explorer__hidden\" data-role=\"map-panel\" aria-labelledby=\"shuttle-map-heading\">",
      "  <div class=\"shuttle-explorer__map-header\">",
      "    <div>",
      "      <h3 id=\"shuttle-map-heading\">Selected sites map</h3>",
      "      <p class=\"shuttle-explorer__tiny shuttle-explorer__map-summary\" data-role=\"map-summary\">Select one or more sites to show them on the map.</p>",
      "    </div>",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small shuttle-explorer__hidden\" data-role=\"reset-map-view\">Reset map view</button>",
      "  </div>",
      "  <div class=\"shuttle-explorer__map-shell\">",
      "    <div class=\"shuttle-explorer__map-canvas\" data-role=\"map-canvas\" aria-label=\"Map of selected FLUXNET sites\"></div>",
      "    <div class=\"shuttle-explorer__map-empty\" data-role=\"map-empty\" aria-live=\"polite\">Select one or more sites to show them on the map.</div>",
      "  </div>",
      "</section>",
      "<aside class=\"shuttle-explorer__attribution\" data-role=\"data-notes\">",
      "  <h3>Data Notes</h3>",
      "  <p class=\"shuttle-explorer__tiny\">These notes highlight how the explorer labels datasets and how the bulk tools behave.</p>",
      "  <ul class=\"shuttle-explorer__tiny\">",
      "    <li>The explorer includes both gap-filled and partitioned data [FLUXNET] and non-gap-filled, non-partitioned observations [e.g., AmeriFlux-BASE].</li>",
      "    <li>If a site has different versions of its dataset published at different times, the explorer surfaces the most recently processed dataset.</li>",
      "    <li>Data labeled as FLUXNET in the year column have been gap-filled and partitioned with the ONEFlux processing pipeline. Use the Availability filter options [FLUXNET processed], [Other processed], and [Sites with both FLUXNET and additional processed years] to distinguish ONEFlux-derived coverage from non-ONEFlux processed coverage.</li>",
      "    <li>FLUXNET data are contributed by site teams around the world and distributed through one of three processing hubs: AmeriFlux (<a href=\"https://ameriflux.lbl.gov/\" target=\"_blank\" rel=\"noopener noreferrer\">https://ameriflux.lbl.gov/</a>), ICOS (<a href=\"https://www.icos-etc.eu/icos/\" target=\"_blank\" rel=\"noopener noreferrer\">https://www.icos-etc.eu/icos/</a>), or TERN (<a href=\"https://www.tern.org.au/\" target=\"_blank\" rel=\"noopener noreferrer\">https://www.tern.org.au/</a>).</li>",
      "    <li>The FLUXNET Shuttle serves data processed with the most recent ONEFlux version and should generally be treated as the highest-quality processed product available here. Choose the Source filter option [FLUXNET-Shuttle] to view the subset of FLUXNET-format datasets generated with this most up-to-date processing.</li>",
      "    <li>Rows labeled [JapanFlux] come from the public JapanFlux2024 ADS archive. Although JapanFlux2024 is provided in the FLUXNET format and gap-filled/partitioned, it is not processed with ONEflux.</li>",
      "    <li>EFD records link to the European Fluxes Database request workflow. Login is required, some data may require PI approval, and download links are provided by EFD after request submission.</li>",
      "    <li>For some sites, the AmeriFlux-BASE product currently extends to years that are not yet available in the corresponding FLUXNET product. Records from both products are presented in such cases</li>",
      "    <li>The bulk-download scripts may require users to install a jq package if neither jq nor python3 are already installed. jq is a lightweight command-line JSON parser used by the script workflow.</li>",
      "  </ul>",
      "</aside>",
      "<aside class=\"shuttle-explorer__attribution\" data-role=\"attribution\">",
      "  <h3>Data Use and Attribution</h3>",
      "  <p class=\"shuttle-explorer__tiny\">Data users must follow dataset- and network-specific access, attribution, and citation guidance included with each downloaded archive or request workflow. All directly downloadable data are provided under <a href=\"https://creativecommons.org/licenses/by/4.0/\" target=\"_blank\" rel=\"noopener noreferrer\">CC-BY 4.0</a></p>",
      "</aside>"
    ].join("");
  }

  function Explorer(root) {
    var savedAmeriFluxBulkIdentity = readAmeriFluxBulkIdentityPreferences();
    this.root = root;
    this.jsonUrl = root.getAttribute("data-json-src") || DEFAULT_JSON_URL;
    this.csvUrl = root.getAttribute("data-csv-src") || DEFAULT_CSV_URL;
    this.icosDirectJsonUrl = root.getAttribute("data-icos-direct-json-src") || DEFAULT_ICOS_DIRECT_JSON_URL;
    this.icosDirectCsvUrl = root.getAttribute("data-icos-direct-csv-src") || DEFAULT_ICOS_DIRECT_CSV_URL;
    this.japanFluxJsonUrl = root.getAttribute("data-japanflux-direct-json-src") || DEFAULT_JAPANFLUX_DIRECT_JSON_URL;
    this.japanFluxCsvUrl = root.getAttribute("data-japanflux-direct-csv-src") || DEFAULT_JAPANFLUX_DIRECT_CSV_URL;
    this.efdJsonUrl = root.getAttribute("data-efd-json-src") || DEFAULT_EFD_JSON_URL;
    this.efdCsvUrl = root.getAttribute("data-efd-csv-src") || DEFAULT_EFD_CSV_URL;
    this.ameriFluxSiteInfoUrl = root.getAttribute("data-ameriflux-site-info-src") || AMERIFLUX_SITE_INFO_URL;
    this.fluxnet2015SiteInfoUrl = root.getAttribute("data-fluxnet2015-site-info-src") || FLUXNET2015_SITE_INFO_URL;
    this.siteNameMetadataUrl = root.getAttribute("data-site-name-metadata-src") || SITE_NAME_METADATA_URL;
    this.vegetationMetadataUrl = root.getAttribute("data-vegetation-metadata-src") || SITE_VEGETATION_METADATA_URL;
    this.pageSize = Math.max(1, parseInt(root.getAttribute("data-page-size") || String(DEFAULT_PAGE_SIZE), 10) || DEFAULT_PAGE_SIZE);
    var ameriIdentity = resolveAmeriFluxIdentityFromRoot(root);
    this.shuttleSource = new ShuttleSource(this.jsonUrl, this.csvUrl);
    this.icosDirectSource = new ShuttleSource(this.icosDirectJsonUrl, this.icosDirectCsvUrl);
    this.japanFluxSource = new ShuttleSource(this.japanFluxJsonUrl, this.japanFluxCsvUrl);
    this.efdSource = new ShuttleSource(this.efdJsonUrl, this.efdCsvUrl);
    this.ameriFluxSource = new AmeriFluxSource({
      availabilityUrl: AMERIFLUX_FLUXNET_AVAILABILITY_URL,
      downloadUrl: AMERIFLUX_V2_DOWNLOAD_URL,
      userId: ameriIdentity.userId,
      userEmail: ameriIdentity.userEmail,
      trustedRuntime: ameriIdentity.trustedRuntime,
      dataProduct: AMERIFLUX_FLUXNET_PRODUCT,
      sourceLabel: AMERIFLUX_SOURCE_ONLY,
      availabilityCacheKey: AMERIFLUX_FLUXNET_AVAILABILITY_CACHE_KEY,
      freshnessNamespace: "ameriflux-fluxnet"
    });
    this.ameriFluxBaseSource = new AmeriFluxSource({
      availabilityUrl: AMERIFLUX_BASE_AVAILABILITY_URL,
      downloadUrl: AMERIFLUX_V2_DOWNLOAD_URL,
      userId: ameriIdentity.userId,
      userEmail: ameriIdentity.userEmail,
      trustedRuntime: ameriIdentity.trustedRuntime,
      dataProduct: AMERIFLUX_BASE_PRODUCT,
      sourceLabel: BASE_SOURCE_ONLY,
      availabilityCacheKey: AMERIFLUX_BASE_AVAILABILITY_CACHE_KEY,
      freshnessNamespace: "ameriflux-base"
    });
    this.fluxnet2015Source = new AmeriFluxSource({
      availabilityUrl: FLUXNET2015_AVAILABILITY_URL,
      downloadUrl: AMERIFLUX_V1_DOWNLOAD_URL,
      userId: ameriIdentity.userId,
      userEmail: ameriIdentity.userEmail,
      trustedRuntime: ameriIdentity.trustedRuntime,
      dataProduct: FLUXNET2015_PRODUCT,
      sourceLabel: FLUXNET2015_SOURCE_ONLY,
      availabilityCacheKey: FLUXNET2015_AVAILABILITY_CACHE_KEY,
      freshnessNamespace: "ameriflux-fluxnet2015"
    });
    this.ameriFluxApiSources = {};
    this.ameriFluxApiSources[AMERIFLUX_FLUXNET_PRODUCT] = this.ameriFluxSource;
    this.ameriFluxApiSources[AMERIFLUX_BASE_PRODUCT] = this.ameriFluxBaseSource;
    this.ameriFluxApiSources[FLUXNET2015_PRODUCT] = this.fluxnet2015Source;

    this.state = {
      mode: "loading",
      rows: [],
      filteredRows: [],
      source: "",
      sourceUrl: "",
      warning: "",
      snapshotUpdatedDate: "",
      droppedRows: 0,
      errorMessage: "",
      search: "",
      selectedNetwork: "",
      selectedSource: "",
      selectedAvailability: "",
      selectedCountry: "",
      selectedVegetation: "",
      selectedHubs: {},
      selectedKeys: {},
      cliPanelVisible: false,
      sortKey: "data_hub",
      sortDir: "asc",
      page: 1,
      ameriFluxBulkUserIdInput: savedAmeriFluxBulkIdentity.userId,
      ameriFluxBulkUserEmailInput: savedAmeriFluxBulkIdentity.userEmail,
      amerifluxTotalSites: 0,
      amerifluxSitesWithYears: 0,
      amerifluxOverlapSites: 0,
      amerifluxOnlySites: 0,
      fluxnet2015TotalSites: 0,
      fluxnet2015SitesWithYears: 0,
      fluxnet2015OnlySites: 0
    };

    createLayout(root);
    this.bindings = this.getBindings();
    this.syncAmeriFluxBulkIdentityInputs();
    this.bindEvents();
    this.renderTableHeader();
    this.setAttributionText(buildAttributionText(""), buildAttributionHtml(""));
    this.render();
  }

  Explorer.prototype.getBindings = function () {
    return {
      status: bySelector(this.root, "[data-role='status']"),
      controls: bySelector(this.root, "[data-role='controls']"),
      search: bySelector(this.root, "[data-role='search']"),
      networkFilter: bySelector(this.root, "[data-role='network-filter']"),
      sourceFilter: bySelector(this.root, "[data-role='source-filter']"),
      availabilityFilter: bySelector(this.root, "[data-role='availability-filter']"),
      countryFilter: bySelector(this.root, "[data-role='country-filter']"),
      vegetationFilter: bySelector(this.root, "[data-role='vegetation-filter']"),
      vegetationInfoWrap: bySelector(this.root, "[data-role='vegetation-info-wrap']"),
      vegetationInfoToggle: bySelector(this.root, "[data-role='vegetation-info-toggle']"),
      hubFilters: bySelector(this.root, "[data-role='hub-filters']"),
      widgetLastUpdatedInline: bySelector(this.root, "[data-role='widget-last-updated-inline']"),
      summaryRow: bySelector(this.root, "[data-role='summary-row']"),
      summary: bySelector(this.root, "[data-role='summary']"),
      reset: bySelector(this.root, "[data-role='reset']"),
      selectionActions: bySelector(this.root, "[data-role='selection-actions']"),
      bulkPanel: bySelector(this.root, "[data-role='bulk-panel']"),
      selectionCount: bySelector(this.root, "[data-role='selection-count']"),
      allSelectedActions: bySelector(this.root, "[data-role='all-selected-actions']"),
      downloadAllSelectedScript: bySelector(this.root, "[data-role='download-all-selected-script']"),
      copyAllSelectedScript: bySelector(this.root, "[data-role='copy-all-selected-script']"),
      shuttleBulkSection: bySelector(this.root, "[data-role='shuttle-bulk-section']"),
      shuttleSelectionCount: bySelector(this.root, "[data-role='shuttle-selection-count']"),
      ameriFluxBulkSection: bySelector(this.root, "[data-role='ameriflux-bulk-section']"),
      ameriFluxSelectionCount: bySelector(this.root, "[data-role='ameriflux-selection-count']"),
      ameriFluxBulkUserId: bySelector(this.root, "[data-role='ameriflux-bulk-user-id']"),
      ameriFluxBulkUserEmail: bySelector(this.root, "[data-role='ameriflux-bulk-user-email']"),
      selectFiltered: bySelector(this.root, "[data-role='select-filtered']"),
      selectAllSites: bySelector(this.root, "[data-role='select-all-sites']"),
      clearSelection: bySelector(this.root, "[data-role='clear-selection']"),
      downloadManifest: bySelector(this.root, "[data-role='download-manifest']"),
      downloadLinks: bySelector(this.root, "[data-role='download-links']"),
      downloadScript: bySelector(this.root, "[data-role='download-script']"),
      downloadSitesFile: bySelector(this.root, "[data-role='download-sites-file']"),
      downloadAmeriFluxSitesFile: bySelector(this.root, "[data-role='download-ameriflux-sites-file']"),
      downloadAmeriFluxScript: bySelector(this.root, "[data-role='download-ameriflux-script']"),
      copyAmeriFluxScript: bySelector(this.root, "[data-role='copy-ameriflux-script']"),
      copyLinks: bySelector(this.root, "[data-role='copy-links']"),
      showCliCommand: bySelector(this.root, "[data-role='show-cli-command']"),
      copyCommand: bySelector(this.root, "[data-role='copy-command']"),
      cliPanel: bySelector(this.root, "[data-role='cli-panel']"),
      cliCommand: bySelector(this.root, "[data-role='cli-command']"),
      bulkStatus: bySelector(this.root, "[data-role='bulk-status']"),
      mapPanel: bySelector(this.root, "[data-role='map-panel']"),
      mapSummary: bySelector(this.root, "[data-role='map-summary']"),
      mapCanvas: bySelector(this.root, "[data-role='map-canvas']"),
      mapEmpty: bySelector(this.root, "[data-role='map-empty']"),
      resetMapView: bySelector(this.root, "[data-role='reset-map-view']"),
      tableWrap: bySelector(this.root, "[data-role='table-wrap']"),
      table: bySelector(this.root, "[data-role='table']"),
      theadRow: bySelector(this.root, "[data-role='thead-row']"),
      tbody: bySelector(this.root, "[data-role='tbody']"),
      empty: bySelector(this.root, "[data-role='empty']"),
      pagination: bySelector(this.root, "[data-role='pagination']"),
      prevPage: bySelector(this.root, "[data-role='prev-page']"),
      nextPage: bySelector(this.root, "[data-role='next-page']"),
      pageButtons: bySelector(this.root, "[data-role='page-buttons']"),
      pageSummary: bySelector(this.root, "[data-role='page-summary']"),
      attributionText: bySelector(this.root, "[data-role='attribution-text']"),
      copyAttribution: bySelector(this.root, "[data-role='copy-attribution']"),
      copyStatus: bySelector(this.root, "[data-role='copy-status']")
    };
  };

  Explorer.prototype.setVegetationTooltipOpen = function (isOpen) {
    if (!this.bindings.vegetationInfoWrap || !this.bindings.vegetationInfoToggle) {
      return;
    }
    this.bindings.vegetationInfoWrap.classList.toggle("is-open", !!isOpen);
    this.bindings.vegetationInfoToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
  };

  Explorer.prototype.bindEvents = function () {
    var self = this;
    var b = this.bindings;
    var applySearch = function () {
      self._searchDebounceTimer = null;
      self.state.search = String(b.search.value || "");
      self.state.page = 1;
      self.updateDerivedState();
      self.render();
      gaEvent("fx_search", {
        q_len: self.state.search.length,
        results: self.state.filteredRows.length
      });
    };

    if (b.search) {
      b.search.addEventListener("input", function () {
        if (self._searchDebounceTimer) {
          window.clearTimeout(self._searchDebounceTimer);
        }
        self._searchDebounceTimer = window.setTimeout(applySearch, SEARCH_DEBOUNCE_MS);
      });
    }

    if (b.networkFilter) {
      b.networkFilter.addEventListener("change", function () {
        self.state.selectedNetwork = String(b.networkFilter.value || "");
        self.state.page = 1;
        self.updateDerivedState();
        self.render();
        self.trackFilterChange("network", self.state.selectedNetwork);
      });
    }

    if (b.sourceFilter) {
      b.sourceFilter.addEventListener("change", function () {
        self.state.selectedSource = String(b.sourceFilter.value || "");
        self.state.page = 1;
        self.updateDerivedState();
        self.render();
        self.trackFilterChange("source", self.state.selectedSource);
      });
    }

    if (b.availabilityFilter) {
      b.availabilityFilter.addEventListener("change", function () {
        self.state.selectedAvailability = String(b.availabilityFilter.value || "");
        self.state.page = 1;
        self.updateDerivedState();
        self.render();
        self.trackFilterChange("availability", self.state.selectedAvailability);
      });
    }

    if (b.countryFilter) {
      b.countryFilter.addEventListener("change", function () {
        self.state.selectedCountry = String(b.countryFilter.value || "");
        self.state.page = 1;
        self.updateDerivedState();
        self.render();
        self.trackFilterChange("country", self.state.selectedCountry);
      });
    }

    if (b.vegetationFilter) {
      b.vegetationFilter.addEventListener("change", function () {
        self.state.selectedVegetation = String(b.vegetationFilter.value || "");
        self.state.page = 1;
        self.updateDerivedState();
        self.render();
        self.trackFilterChange("vegetation_type", self.state.selectedVegetation);
      });
    }

    if (b.vegetationInfoWrap && b.vegetationInfoToggle) {
      this.setVegetationTooltipOpen(false);
      b.vegetationInfoToggle.addEventListener("click", function (event) {
        event.preventDefault();
        event.stopPropagation();
        self.setVegetationTooltipOpen(!b.vegetationInfoWrap.classList.contains("is-open"));
      });
      b.vegetationInfoToggle.addEventListener("keydown", function (event) {
        if (event.key === "Escape" || event.key === "Esc") {
          self.setVegetationTooltipOpen(false);
          b.vegetationInfoToggle.blur();
        }
      });
      b.vegetationInfoWrap.addEventListener("focusout", function (event) {
        if (!b.vegetationInfoWrap.contains(event.relatedTarget)) {
          self.setVegetationTooltipOpen(false);
        }
      });
      document.addEventListener("click", function (event) {
        if (!b.vegetationInfoWrap.contains(event.target)) {
          self.setVegetationTooltipOpen(false);
        }
      });
    }

    if (b.hubFilters) {
      b.hubFilters.addEventListener("change", function (event) {
        var target = event.target;
        if (!target || target.tagName !== "INPUT" || target.type !== "checkbox") {
          return;
        }
        var hub = String(target.getAttribute("data-hub") || "");
        if (!hub) {
          return;
        }
        self.state.selectedHubs[hub] = !!target.checked;
        self.state.page = 1;
        self.updateDerivedState();
        self.render();
        self.trackFilterChange("hub", hub + ":" + (target.checked ? "on" : "off"));
      });
    }

    if (b.reset) {
      b.reset.addEventListener("click", function () {
        self.resetFilters();
      });
    }

    if (b.selectFiltered) {
      b.selectFiltered.addEventListener("click", function () {
        self.selectRows(self.state.filteredRows, true);
        gaEvent("fx_select_all_filtered", {
          count: self.state.filteredRows.length
        });
      });
    }

    if (b.selectAllSites) {
      b.selectAllSites.addEventListener("click", function () {
        self.selectRows(self.state.rows, true);
      });
    }

    if (b.clearSelection) {
      b.clearSelection.addEventListener("click", function () {
        self.clearAllSelection();
      });
    }

    if (b.downloadAllSelectedScript) {
      b.downloadAllSelectedScript.addEventListener("click", function () {
        self.handleDownloadAllSelectedScript();
      });
    }

    if (b.copyAllSelectedScript) {
      b.copyAllSelectedScript.addEventListener("click", function () {
        self.handleCopyAllSelectedScript();
      });
    }

    if (b.downloadManifest) {
      b.downloadManifest.addEventListener("click", function () {
        self.handleDownloadManifest();
      });
    }

    if (b.downloadLinks) {
      b.downloadLinks.addEventListener("click", function () {
        self.handleDownloadLinks();
      });
    }

    if (b.downloadScript) {
      b.downloadScript.addEventListener("click", function () {
        self.handleDownloadScript();
      });
    }

    if (b.downloadSitesFile) {
      b.downloadSitesFile.addEventListener("click", function () {
        self.handleDownloadSitesFile();
      });
    }

    if (b.downloadAmeriFluxSitesFile) {
      b.downloadAmeriFluxSitesFile.addEventListener("click", function () {
        self.handleDownloadAmeriFluxSitesFile();
      });
    }

    function handleAmeriFluxBulkIdentityInput() {
      self.state.ameriFluxBulkUserIdInput = String(b.ameriFluxBulkUserId && b.ameriFluxBulkUserId.value || "");
      self.state.ameriFluxBulkUserEmailInput = String(b.ameriFluxBulkUserEmail && b.ameriFluxBulkUserEmail.value || "");
      writeAmeriFluxBulkIdentityPreferences(self.state.ameriFluxBulkUserIdInput, self.state.ameriFluxBulkUserEmailInput);
      self.renderBulkPanel();
    }

    if (b.ameriFluxBulkUserId) {
      b.ameriFluxBulkUserId.addEventListener("input", handleAmeriFluxBulkIdentityInput);
      b.ameriFluxBulkUserId.addEventListener("change", handleAmeriFluxBulkIdentityInput);
    }

    if (b.ameriFluxBulkUserEmail) {
      b.ameriFluxBulkUserEmail.addEventListener("input", handleAmeriFluxBulkIdentityInput);
      b.ameriFluxBulkUserEmail.addEventListener("change", handleAmeriFluxBulkIdentityInput);
    }

    if (b.downloadAmeriFluxScript) {
      b.downloadAmeriFluxScript.addEventListener("click", function () {
        self.handleDownloadAmeriFluxScript();
      });
    }

    if (b.copyAmeriFluxScript) {
      b.copyAmeriFluxScript.addEventListener("click", function () {
        self.handleCopyAmeriFluxScript();
      });
    }

    if (b.copyLinks) {
      b.copyLinks.addEventListener("click", function () {
        self.handleCopyLinks();
      });
    }

    if (b.showCliCommand) {
      b.showCliCommand.addEventListener("click", function () {
        self.toggleCliPanel();
      });
    }

    if (b.copyCommand) {
      b.copyCommand.addEventListener("click", function () {
        self.handleCopyCommand();
      });
    }

    if (b.resetMapView) {
      b.resetMapView.addEventListener("click", function () {
        self.resetMapView();
      });
    }

    if (b.prevPage) {
      b.prevPage.addEventListener("click", function () {
        if (self.state.page > 1) {
          self.state.page -= 1;
          self.render();
        }
      });
    }

    if (b.nextPage) {
      b.nextPage.addEventListener("click", function () {
        var totalPages = self.getTotalPages();
        if (self.state.page < totalPages) {
          self.state.page += 1;
          self.render();
        }
      });
    }

    if (b.pageButtons) {
      b.pageButtons.addEventListener("click", function (event) {
        var target = event.target;
        if (!target || target.tagName !== "BUTTON") {
          return;
        }
        var page = parseInt(target.getAttribute("data-page") || "", 10);
        if (!page || page < 1) {
          return;
        }
        self.state.page = page;
        self.render();
      });
    }

    if (b.theadRow) {
      b.theadRow.addEventListener("click", function (event) {
        var target = event.target;
        while (target && target !== b.theadRow && !(target.tagName === "BUTTON" && (target.hasAttribute("data-sort-key") || target.getAttribute("data-role") === "copy-table-button"))) {
          target = target.parentNode;
        }
        if (!target || target === b.theadRow) {
          return;
        }
        if (target.getAttribute("data-role") === "copy-table-button") {
          self.handleCopyTable();
          return;
        }
        var key = target.getAttribute("data-sort-key");
        if (!key) {
          return;
        }
        if (self.state.sortKey === key) {
          self.state.sortDir = self.state.sortDir === "asc" ? "desc" : "asc";
        } else {
          self.state.sortKey = key;
          self.state.sortDir = "asc";
        }
        self.state.page = 1;
        self.updateDerivedState();
        self.render();
      });
    }

    if (b.tbody) {
      b.tbody.addEventListener("change", function (event) {
        var target = event.target;
        if (!target || target.tagName !== "INPUT" || target.type !== "checkbox" || target.getAttribute("data-role") !== "row-select") {
          return;
        }
        var key = String(target.getAttribute("data-key") || "");
        if (!key) {
          return;
        }
        self.state.selectedKeys[key] = !!target.checked;
        self.render();
      });

      b.tbody.addEventListener("click", function (event) {
        var target = event.target;
        while (
          target &&
          target !== b.tbody &&
          !(
            (target.tagName === "BUTTON" && target.getAttribute("data-role") === "ameriflux-download") ||
            (target.tagName === "A" && target.getAttribute("data-role") === "row-link-action")
          )
        ) {
          target = target.parentNode;
        }
        if (!target || target === b.tbody) {
          return;
        }
        if (target.tagName === "A" && target.getAttribute("data-role") === "row-link-action") {
          gaEvent(String(target.getAttribute("data-outbound-event") || "fx_row_link_click"), {
            site_id: String(target.getAttribute("data-site-id") || ""),
            mode: String(target.getAttribute("data-link-mode") || ""),
            source_label: String(target.getAttribute("data-source-label") || "")
          });
          return;
        }
        var siteId = String(target.getAttribute("data-site-id") || "");
        var dataProduct = String(target.getAttribute("data-product") || "");
        var sourceLabel = String(target.getAttribute("data-source-label") || "");
        if (!siteId) {
          return;
        }
        self.handleAmeriFluxRowDownload(siteId, dataProduct, sourceLabel, target);
      });
    }

    if (b.copyAttribution) {
      b.copyAttribution.addEventListener("click", function () {
        self.copyAttribution();
      });
    }

  };

  Explorer.prototype.setAttributionText = function (text, html) {
    var pageAttributionNote = document.getElementById("shuttle-attribution-note");
    if (pageAttributionNote) {
      pageAttributionNote.setAttribute("data-plain-text", String(text || ""));
      pageAttributionNote.innerHTML = html || escapeHtml(String(text || ""));
    }
  };

  Explorer.prototype.syncAmeriFluxBulkIdentityInputs = function () {
    if (this.bindings.ameriFluxBulkUserId) {
      this.bindings.ameriFluxBulkUserId.value = String(this.state.ameriFluxBulkUserIdInput || "");
    }
    if (this.bindings.ameriFluxBulkUserEmail) {
      this.bindings.ameriFluxBulkUserEmail.value = String(this.state.ameriFluxBulkUserEmailInput || "");
    }
  };

  Explorer.prototype.trackFilterChange = function (filterName, value) {
    gaEvent("fx_filter_change", {
      filter: String(filterName || ""),
      value: String(value || ""),
      results: this.state.filteredRows.length
    });
  };

  Explorer.prototype.trackExplorerLoadedOnce = function () {
    if (this._gaExplorerLoadedTracked) {
      return;
    }
    this._gaExplorerLoadedTracked = true;
    gaEvent("fx_explorer_loaded", {
      rows: this.state.rows.length,
      snapshot_last_updated: this.state.snapshotUpdatedDate || ""
    });
  };

  Explorer.prototype.renderSnapshotUpdatedText = function () {
    var label = snapshotUpdatedDateDisplayText(this.state.snapshotUpdatedDate);
    if (this.bindings.widgetLastUpdatedInline) {
      this.bindings.widgetLastUpdatedInline.textContent = label;
    }
    var pageIntroSpan = document.getElementById("shuttle-snapshot-last-updated");
    if (pageIntroSpan) {
      pageIntroSpan.textContent = label;
    }
    this.setAttributionText(
      buildAttributionText(this.state.snapshotUpdatedDate),
      buildAttributionHtml(this.state.snapshotUpdatedDate)
    );
  };

  Explorer.prototype.copyAttribution = function () {
    var self = this;
    var attributionText = this.bindings.attributionText;
    var status = this.bindings.copyStatus;
    var plainText = attributionText
      ? String(attributionText.getAttribute("data-plain-text") || attributionText.textContent || "")
      : "";
    if (!attributionText) {
      return;
    }

    function setStatus(message) {
      if (status) {
        status.textContent = message;
      }
    }

    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(plainText).then(function () {
        setStatus("Copied.");
      }).catch(function () {
        try {
          var helper = document.createElement("textarea");
          helper.value = plainText;
          helper.setAttribute("readonly", "readonly");
          helper.style.position = "absolute";
          helper.style.left = "-9999px";
          document.body.appendChild(helper);
          helper.focus();
          helper.select();
          document.execCommand("copy");
          document.body.removeChild(helper);
          setStatus("Copied.");
        } catch (err) {
          setStatus("Copy failed. Select and copy manually.");
        }
      });
      return;
    }

    try {
      var helperFallback = document.createElement("textarea");
      helperFallback.value = plainText;
      helperFallback.setAttribute("readonly", "readonly");
      helperFallback.style.position = "absolute";
      helperFallback.style.left = "-9999px";
      document.body.appendChild(helperFallback);
      helperFallback.focus();
      helperFallback.select();
      document.execCommand("copy");
      document.body.removeChild(helperFallback);
      setStatus("Copied.");
    } catch (err2) {
      setStatus("Copy failed. Select and copy manually.");
    }

    window.setTimeout(function () {
      if (status && status.textContent === "Copied.") {
        status.textContent = "";
      }
    }, 1500);
  };

  Explorer.prototype.setMode = function (mode, message, extraClass) {
    this.state.mode = mode;
    var status = this.bindings.status;
    if (!status) {
      return;
    }
    status.className = "shuttle-explorer__status" + (extraClass ? " " + extraClass : "");
    status.textContent = message || "";
  };

  Explorer.prototype.getTotalPages = function () {
    var total = this.state.filteredRows.length;
    return Math.max(1, Math.ceil(total / this.pageSize));
  };

  Explorer.prototype.resetFilters = function () {
    var self = this;
    if (this._searchDebounceTimer) {
      window.clearTimeout(this._searchDebounceTimer);
      this._searchDebounceTimer = null;
    }
    this.state.search = "";
    this.state.selectedNetwork = "";
    this.state.selectedSource = "";
    this.state.selectedAvailability = "";
    this.state.selectedCountry = "";
    this.state.selectedVegetation = "";
    Object.keys(this.state.selectedHubs).forEach(function (hub) {
      self.state.selectedHubs[hub] = true;
    });
    this.state.sortKey = "data_hub";
    this.state.sortDir = "asc";
    this.state.page = 1;

    if (this.bindings.search) {
      this.bindings.search.value = "";
    }
    if (this.bindings.networkFilter) {
      this.bindings.networkFilter.value = "";
    }
    if (this.bindings.sourceFilter) {
      this.bindings.sourceFilter.value = "";
    }
    if (this.bindings.availabilityFilter) {
      this.bindings.availabilityFilter.value = "";
    }
    if (this.bindings.countryFilter) {
      this.bindings.countryFilter.value = "";
    }
    if (this.bindings.vegetationFilter) {
      this.bindings.vegetationFilter.value = "";
    }
    qsa(this.bindings.hubFilters, "input[type='checkbox']").forEach(function (input) {
      input.checked = true;
    });

    this.updateDerivedState();
    this.render();
  };

  Explorer.prototype.getSelectedRows = function () {
    var selectedKeys = this.state.selectedKeys || {};
    return this.state.rows.filter(function (row) {
      return !!selectedKeys[row._selection_key];
    });
  };

  Explorer.prototype.getSelectedCount = function () {
    var count = 0;
    var selectedKeys = this.state.selectedKeys || {};
    Object.keys(selectedKeys).forEach(function (key) {
      if (selectedKeys[key]) {
        count += 1;
      }
    });
    return count;
  };

  Explorer.prototype.selectRows = function (rows, checked) {
    var self = this;
    (rows || []).forEach(function (row) {
      if (!row || !row._selection_key) {
        return;
      }
      self.state.selectedKeys[row._selection_key] = !!checked;
    });
    this.render();
  };

  Explorer.prototype.clearAllSelection = function () {
    this.state.selectedKeys = {};
    this.render();
  };

  Explorer.prototype.getMapSelectionState = function () {
    var selectedRows = this.getSelectedRows();
    var mappableRows = [];
    var missingCoordinates = 0;
    var signatureParts = [];

    selectedRows.forEach(function (row) {
      var latitude = parseCoordinate(row && row.latitude, -90, 90);
      var longitude = parseCoordinate(row && row.longitude, -180, 180);
      if (latitude == null || longitude == null) {
        missingCoordinates += 1;
        return;
      }
      row.latitude = latitude;
      row.longitude = longitude;
      row.has_coordinates = true;
      mappableRows.push(row);
      signatureParts.push(String(row._selection_key || "") + ":" + latitude + ":" + longitude);
    });

    signatureParts.sort();

    return {
      selectedRows: selectedRows,
      mappableRows: mappableRows,
      missingCoordinates: missingCoordinates,
      signature: [
        String(selectedRows.length),
        String(missingCoordinates),
        signatureParts.join("|")
      ].join("::")
    };
  };

  Explorer.prototype.ensureMap = function () {
    var L;
    if (this.map || !this.bindings.mapCanvas) {
      return !!this.map;
    }
    if (typeof window === "undefined" || !window.L) {
      return false;
    }

    L = window.L;
    this.map = L.map(this.bindings.mapCanvas, {
      scrollWheelZoom: true,
      worldCopyJump: true
    });
    this.map.setView([20, 0], 2);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap contributors",
      maxZoom: 18
    }).addTo(this.map);
    this.mapMarkerLayer = L.featureGroup().addTo(this.map);
    if (this.map.attributionControl && this.map.attributionControl.setPrefix) {
      this.map.attributionControl.setPrefix("");
    }
    if (!this._mapResizeHandler) {
      var self = this;
      this._mapResizeHandler = function () {
        self.invalidateMapSize(false);
      };
      window.addEventListener("resize", this._mapResizeHandler);
    }
    return true;
  };

  Explorer.prototype.invalidateMapSize = function (refit) {
    var self = this;
    if (!this.map) {
      return;
    }
    var callback = function () {
      if (!self.map) {
        return;
      }
      self.map.invalidateSize(false);
      if (refit) {
        self.fitMapToMarkers();
      }
    };
    if (typeof window !== "undefined" && window.requestAnimationFrame) {
      window.requestAnimationFrame(callback);
      return;
    }
    window.setTimeout(callback, 0);
  };

  Explorer.prototype.fitMapToMarkers = function () {
    var layers;
    if (!this.map || !this.mapMarkerLayer) {
      return;
    }
    layers = this.mapMarkerLayer.getLayers();
    if (!layers.length) {
      this.map.setView([20, 0], 2);
      return;
    }
    if (layers.length === 1) {
      this.map.setView(layers[0].getLatLng(), 6);
      return;
    }
    this.map.fitBounds(this.mapMarkerLayer.getBounds(), {
      padding: [24, 24],
      maxZoom: 6
    });
  };

  Explorer.prototype.resetMapView = function () {
    if (!this.ensureMap()) {
      return;
    }
    this.fitMapToMarkers();
  };

  Explorer.prototype.buildMapPopupHtml = function (row) {
    var lines = [
      "<div class=\"shuttle-explorer__map-popup\">",
      "  <strong>" + escapeHtml(row.site_id || "") + "</strong>"
    ];

    if (row.site_name) {
      lines.push("  <div>" + escapeHtml(row.site_name) + "</div>");
    }
    if (row.country) {
      lines.push("  <div class=\"shuttle-explorer__map-popup-meta\">" + escapeHtml(row.country) + "</div>");
    }
    lines.push("</div>");
    return lines.join("");
  };

  Explorer.prototype.renderMapMarkers = function (rows) {
    var self = this;
    var L;
    if (!this.ensureMap() || !this.mapMarkerLayer) {
      return;
    }
    L = window.L;
    this.mapMarkerLayer.clearLayers();
    (rows || []).forEach(function (row) {
      var marker = L.circleMarker([row.latitude, row.longitude], {
        radius: 6,
        weight: 1.5,
        color: "#2f5374",
        fillColor: "#5f8bb3",
        fillOpacity: 0.9
      });
      marker.bindPopup(self.buildMapPopupHtml(row), {
        autoPan: true
      });
      marker.on("click", function () {
        if (self.map) {
          self.map.panTo(marker.getLatLng(), {
            animate: true
          });
        }
      });
      self.mapMarkerLayer.addLayer(marker);
    });
  };

  Explorer.prototype.setMapEmptyState = function (message) {
    var empty = this.bindings.mapEmpty;
    if (!empty) {
      return;
    }
    empty.textContent = message || "";
    empty.classList.toggle("shuttle-explorer__hidden", !message);
  };

  Explorer.prototype.renderMap = function () {
    var b = this.bindings;
    var selectionState;
    var selectionChanged;
    var summary;
    var message = "";
    if (!b.mapPanel) {
      return;
    }

    selectionState = this.getMapSelectionState();

    if (b.mapSummary) {
      if (!selectionState.selectedRows.length) {
        summary = "Select one or more sites to show them on the map.";
      } else if (!selectionState.mappableRows.length) {
        summary = selectionState.selectedRows.length + " selected " + (selectionState.selectedRows.length === 1 ? "site" : "sites") + ", but coordinates are unavailable in the current snapshot.";
      } else {
        summary = "Showing " + selectionState.mappableRows.length + " selected " + (selectionState.mappableRows.length === 1 ? "site" : "sites") + " on the map.";
        if (selectionState.missingCoordinates) {
          summary += " " + selectionState.missingCoordinates + " selected " + (selectionState.missingCoordinates === 1 ? "site was" : "sites were") + " omitted because coordinates are unavailable.";
        }
      }
      b.mapSummary.textContent = summary;
    }

    if (!this.ensureMap()) {
      if (b.resetMapView) {
        b.resetMapView.classList.add("shuttle-explorer__hidden");
      }
      this.setMapEmptyState("Map preview unavailable because the map library did not load.");
      return;
    }

    selectionChanged = selectionState.signature !== this._mapSelectionSignature;
    if (selectionChanged) {
      this._mapSelectionSignature = selectionState.signature;
      this.renderMapMarkers(selectionState.mappableRows);
    }

    if (b.resetMapView) {
      b.resetMapView.classList.toggle("shuttle-explorer__hidden", !selectionState.mappableRows.length);
    }

    if (!selectionState.selectedRows.length) {
      message = "Select one or more sites to show them on the map.";
    } else if (!selectionState.mappableRows.length) {
      message = "The selected sites do not include map coordinates in the current metadata snapshot.";
    }
    this.setMapEmptyState(message);
    this.invalidateMapSize(selectionChanged);
  };

  Explorer.prototype.pruneSelection = function () {
    var valid = {};
    this.state.rows.forEach(function (row) {
      if (row && row._selection_key) {
        valid[row._selection_key] = true;
      }
    });
    Object.keys(this.state.selectedKeys || {}).forEach(function (key) {
      if (!valid[key]) {
        delete this.state.selectedKeys[key];
      }
    }, this);
  };

  Explorer.prototype.setBulkStatus = function (message) {
    if (this.bindings.bulkStatus) {
      this.bindings.bulkStatus.textContent = message || "";
    }
  };

  Explorer.prototype.downloadTextFile = function (filename, text, mimeType) {
    var blob = new Blob([String(text || "")], { type: mimeType || "text/plain;charset=utf-8" });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.setTimeout(function () {
      URL.revokeObjectURL(url);
    }, 2000);
  };

  Explorer.prototype.copyText = function (text, successMessage) {
    var self = this;
    var value = String(text || "");
    if (!value) {
      this.setBulkStatus("Nothing to copy.");
      return;
    }
    this.copyPlainText(
      value,
      function () {
        self.setBulkStatus(successMessage || "Copied.");
      },
      function () {
        self.setBulkStatus("Copy failed. Try downloading the file instead.");
      }
    );
  };

  Explorer.prototype.copyPlainText = function (text, onSuccess, onFailure) {
    var value = String(text || "");
    var self = this;
    if (!value) {
      if (typeof onFailure === "function") {
        onFailure();
      }
      return;
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(value).then(function () {
        if (typeof onSuccess === "function") {
          onSuccess();
        }
      }).catch(function () {
        self.fallbackCopyText(value, onSuccess, onFailure);
      });
      return;
    }
    this.fallbackCopyText(value, onSuccess, onFailure);
  };

  Explorer.prototype.fallbackCopyText = function (text, onSuccess, onFailure) {
    var ta = document.createElement("textarea");
    ta.value = String(text || "");
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    try {
      document.execCommand("copy");
      if (typeof onSuccess === "function") {
        onSuccess();
      }
    } catch (err) {
      if (typeof onFailure === "function") {
        onFailure(err);
      }
    }
    document.body.removeChild(ta);
  };

  Explorer.prototype.getTableCopyButton = function () {
    return bySelector(this.bindings.theadRow, "[data-role='copy-table-button']");
  };

  Explorer.prototype.resetTableCopyButtonLabel = function () {
    var button = this.getTableCopyButton();
    if (!button) {
      return;
    }
    button.textContent = COPY_TABLE_BUTTON_LABEL;
    button.setAttribute("aria-label", COPY_TABLE_BUTTON_LABEL);
    button.classList.remove("is-success");
    button.classList.remove("is-error");
    if (this._tableCopyFeedbackTimer) {
      window.clearTimeout(this._tableCopyFeedbackTimer);
      this._tableCopyFeedbackTimer = null;
    }
  };

  Explorer.prototype.setTableCopyFeedback = function (label, stateClass) {
    var self = this;
    var button = this.getTableCopyButton();
    if (!button) {
      return;
    }
    if (this._tableCopyFeedbackTimer) {
      window.clearTimeout(this._tableCopyFeedbackTimer);
      this._tableCopyFeedbackTimer = null;
    }
    button.textContent = label;
    button.setAttribute("aria-label", label);
    button.classList.toggle("is-success", stateClass === "success");
    button.classList.toggle("is-error", stateClass === "error");
    this._tableCopyFeedbackTimer = window.setTimeout(function () {
      self.resetTableCopyButtonLabel();
    }, COPY_TABLE_FEEDBACK_MS);
  };

  Explorer.prototype.handleCopyTable = function () {
    var self = this;
    var text = buildTableClipboardText(this.getDisplayedRows());
    this.copyPlainText(
      text,
      function () {
        self.setTableCopyFeedback(COPY_TABLE_SUCCESS_LABEL, "success");
      },
      function () {
        self.setTableCopyFeedback(COPY_TABLE_FAILURE_LABEL, "error");
      }
    );
  };

  Explorer.prototype.getSelectedRowsOrWarn = function () {
    var selectedRows = this.getSelectedRows();
    if (!selectedRows.length) {
      this.setBulkStatus("Select one or more sites first.");
      return null;
    }
    return selectedRows;
  };

  Explorer.prototype.getShuttleRows = function (rows) {
    return partitionRowsByBulkSource(rows).shuttleRows;
  };

  Explorer.prototype.getShuttleDownloadRows = function (rows) {
    return partitionRowsByBulkSource(rows).shuttleDownloadRows;
  };

  Explorer.prototype.getManualLandingPageRows = function (rows) {
    return partitionRowsByBulkSource(rows).manualLandingPageRows;
  };

  Explorer.prototype.getRequestOnlyRows = function (rows) {
    return partitionRowsByBulkSource(rows).requestOnlyRows;
  };

  Explorer.prototype.getShuttleCliRows = function (rows) {
    return (rows || []).filter(isShuttleCatalogRow);
  };

  Explorer.prototype.getAmeriFluxRows = function (rows) {
    return partitionRowsByBulkSource(rows).ameriFluxRows;
  };

  Explorer.prototype.getBulkSelectionSummary = function (rows) {
    return summarizeBulkSelection(rows);
  };

  Explorer.prototype.getAmeriFluxApiSource = function (dataProduct) {
    var product = String(dataProduct || "").trim().toUpperCase();
    return this.ameriFluxApiSources[product] || this.ameriFluxSource;
  };

  Explorer.prototype.getAmeriFluxBulkEntries = function (rows) {
    return normalizeAmeriFluxBulkEntries((rows || []).map(function (row) {
      return {
        site_id: String(row && row.site_id || "").trim(),
        data_product: getApiRowDataProduct(row),
        source_label: String(row && row.source_label || "").trim()
      };
    }));
  };

  Explorer.prototype.buildSelectionManifestCsv = function (rows) {
    var lines = [
      ["site_id", "data_hub", "source_label", "download_mode", "network", "country", "download_link"].join(",")
    ];
    rows.forEach(function (row) {
      lines.push([
        csvEscape(row.site_id),
        csvEscape(row.data_hub),
        csvEscape(row.source_label || ""),
        csvEscape(row.download_mode || ""),
        csvEscape(row.network_display || row.network || ""),
        csvEscape(row.country || ""),
        csvEscape(row.download_link)
      ].join(","));
    });
    return lines.join("\n") + "\n";
  };

  Explorer.prototype.buildLinksText = function (rows) {
    return rows.map(function (row) {
      return row.download_link;
    }).join("\n") + "\n";
  };

  Explorer.prototype.buildSelectedSitesText = function (rows) {
    return selectedSiteIdsText(uniqueSiteIdsFromRows(rows));
  };

  Explorer.prototype.buildAmeriFluxSelectedSitesText = function (rows) {
    return buildAmeriFluxSelectedSitesText(this.getAmeriFluxBulkEntries(rows));
  };

  Explorer.prototype.getEffectiveAmeriFluxIdentity = function () {
    return resolveAmeriFluxBulkIdentity(
      this.state.ameriFluxBulkUserIdInput,
      this.state.ameriFluxBulkUserEmailInput
    );
  };

  Explorer.prototype.getAmeriFluxBulkIdentity = function () {
    return this.getEffectiveAmeriFluxIdentity();
  };

  Explorer.prototype.buildAmeriFluxBulkScript = function (rows) {
    var entries = this.getAmeriFluxBulkEntries(rows);
    var identity = this.getEffectiveAmeriFluxIdentity();
    return buildAmeriFluxBulkScriptText(entries, {
      defaultUserId: identity.user_id,
      defaultUserEmail: identity.user_email,
      v2DownloadUrl: AMERIFLUX_V2_DOWNLOAD_URL,
      v1DownloadUrl: AMERIFLUX_V1_DOWNLOAD_URL,
      variant: AMERIFLUX_DEFAULT_VARIANT,
      policy: AMERIFLUX_DEFAULT_POLICY,
      v2IntendedUse: AMERIFLUX_V2_INTENDED_USE,
      v1IntendedUse: AMERIFLUX_V1_INTENDED_USE
    });
  };

  Explorer.prototype.buildDownloadAllSelectedScript = function (rows) {
    var selectionSummary = this.getBulkSelectionSummary(rows);
    return buildDownloadAllSelectedScriptText({
      includeShuttle: selectionSummary.shuttleDownloadCount > 0,
      includeAmeriFlux: selectionSummary.ameriFluxCount > 0,
      shuttleScript: "./download_shuttle_selected.sh",
      ameriFluxScript: "./download_ameriflux_selected.sh"
    });
  };

  Explorer.prototype.buildDownloadAllSelectedFiles = function (rows) {
    var selectedRows = Array.isArray(rows) ? rows : [];
    return buildDownloadAllSelectedFileBundle({
      wrapperText: this.buildDownloadAllSelectedScript(selectedRows),
      ameriFluxText: this.buildAmeriFluxBulkScript(this.getAmeriFluxRows(selectedRows)),
      shuttleText: this.buildCurlScript(this.getShuttleDownloadRows(selectedRows))
    });
  };

  Explorer.prototype.buildManualLandingPageWarning = function (rows) {
    var manualCount = uniqueSiteIdsFromRows(this.getManualLandingPageRows(rows)).length;
    var requestOnlyCount = uniqueSiteIdsFromRows(this.getRequestOnlyRows(rows)).length;
    var parts = [];
    if (!manualCount && !requestOnlyCount) {
      return "";
    }
    if (manualCount) {
      parts.push(manualCount + " landing-page-only");
    }
    if (requestOnlyCount) {
      parts.push(requestOnlyCount + " request-only");
    }
    return parts.join(" and ") + " selection(s) were excluded from direct bulk output.";
  };

  Explorer.prototype.getDuplicateSelectedSiteIds = function (rows) {
    var counts = {};
    rows.forEach(function (row) {
      var siteId = String(row.site_id || "").trim();
      if (!siteId) {
        return;
      }
      counts[siteId] = (counts[siteId] || 0) + 1;
    });
    return Object.keys(counts).filter(function (siteId) {
      return counts[siteId] > 1;
    }).sort();
  };

  Explorer.prototype.buildCurlScript = function (rows) {
    var total = rows.length;
    var sanitizeScriptField = function (value) {
      return String(value == null ? "" : value).replace(/[\r\n\t]+/g, " ").trim();
    };
    var scriptRows = rows.map(function (row) {
      return [
        sanitizeScriptField(row.site_id),
        sanitizeScriptField(row.data_hub),
        sanitizeScriptField(row.first_year),
        sanitizeScriptField(row.last_year),
        sanitizeScriptField(row.download_link)
      ].join("\t");
    });
    return [
      "#!/usr/bin/env bash",
      "set -euo pipefail",
      "",
      "# NOTE: ICOS links may require interactive license acceptance in a browser before download.",
      "# If an ICOS URL does not download directly, open it in a browser and follow ICOS prompts.",
      "# NOTE: JapanFlux rows without validated ZIP URLs are excluded from this direct-download script.",
      "",
      "OUTDIR=\"${1:-fluxnet_downloads}\"",
      "LOGFILE=\"${2:-bulk_download.log}\"",
      "CONCURRENCY=\"${CONCURRENCY:-5}\"",
      "MAX_ATTEMPTS=\"${MAX_ATTEMPTS:-5}\"",
      "URL_FILE=\"${URL_FILE:-}\"",
      "PARALLEL_MODE=\"sequential\"",
      "SCRIPT_VERSION=\"2026-03-04-8\"",
      "",
      "mkdir -p \"$OUTDIR\"",
      "SUCCESS_FILE=\"$OUTDIR/download_success.txt\"",
      "FAILED_FILE=\"$OUTDIR/download_failed.txt\"",
      "QUEUE_DIR=\"$OUTDIR/.queue\"",
      "mkdir -p \"$QUEUE_DIR\"",
      ": > \"$SUCCESS_FILE\"",
      ": > \"$FAILED_FILE\"",
      ": > \"$LOGFILE\"",
      "",
      "if ! command -v curl >/dev/null 2>&1; then",
      "  echo \"curl is required but was not found in PATH.\" >&2",
      "  exit 1",
      "fi",
      "",
      "if command -v xargs >/dev/null 2>&1; then",
      "  if printf 'ok\\0' | xargs -0 -P 1 -I {} echo {} >/dev/null 2>&1; then",
      "    PARALLEL_MODE=\"xargs\"",
      "  fi",
      "fi",
      "",
      "if [ \"$PARALLEL_MODE\" = \"sequential\" ]; then",
      "  echo \"Parallel xargs mode unavailable; using sequential downloads.\" | tee -a \"$LOGFILE\"",
      "fi",
      "",
      "cat > \"$QUEUE_DIR/urls_all.txt\" <<'FLUXNET_URLS'",
      scriptRows.join("\n"),
      "FLUXNET_URLS",
      "",
      "if [ -n \"$URL_FILE\" ] && [ -f \"$URL_FILE\" ]; then",
      "  cp \"$URL_FILE\" \"$QUEUE_DIR/urls_all.txt\"",
      "fi",
      "",
      "sanitize_list() {",
      "  awk 'NF {",
      "    gsub(/^[[:space:]]+|[[:space:]]+$/, \"\", $0);",
      "    if ($0 != \"\") print $0;",
      "  }' \"$1\"",
      "}",
      "",
      "sanitize_list \"$QUEUE_DIR/urls_all.txt\" > \"$QUEUE_DIR/urls_0.txt\"",
      "TOTAL=$(wc -l < \"$QUEUE_DIR/urls_0.txt\" | tr -d ' ')",
      "if [ \"$TOTAL\" -eq 0 ]; then",
      "  echo \"No URLs to download.\" | tee -a \"$LOGFILE\"",
      "  exit 1",
      "fi",
      "",
      "echo \"FLUXNET bulk downloader version: $SCRIPT_VERSION\" | tee -a \"$LOGFILE\"",
      "echo \"Starting bulk download of $TOTAL files (MODE=$PARALLEL_MODE, CONCURRENCY=$CONCURRENCY, MAX_ATTEMPTS=$MAX_ATTEMPTS)\" | tee -a \"$LOGFILE\"",
      "",
      "url_hash() {",
      "  printf '%s' \"$1\" | cksum | awk '{print $1}'",
      "}",
      "",
      "safe_token() {",
      "  printf '%s' \"$1\" | tr -c '[:alnum:]_.-' '_' | sed 's/^_*//; s/_*$//'",
      "}",
      "",
      "filename_from_url() {",
      "  local url=\"$1\"",
      "  local site_id=\"${2:-}\"",
      "  local data_hub=\"${3:-}\"",
      "  local first_year=\"${4:-}\"",
      "  local last_year=\"${5:-}\"",
      "  local clean=\"${url%%\\?*}\"",
      "  local base=\"$(basename \"$clean\")\"",
      "  local hash=\"$(url_hash \"$url\")\"",
      "",
      "  if [ -z \"$base\" ] || [ \"$base\" = \".\" ] || [ \"$base\" = \"/\" ]; then",
      "    printf 'download_%s.bin\\n' \"$hash\"",
      "    return 0",
      "  fi",
      "",
      "  case \"$base\" in",
      "    licence_accept|license_accept)",
      "      local sid=\"$(safe_token \"$site_id\")\"",
      "      local hub=\"$(safe_token \"$data_hub\")\"",
      "      local years=\"unknown-years\"",
      "      if [ -z \"$hub\" ]; then",
      "        hub=\"ICOS\"",
      "      fi",
      "      if [ -n \"$first_year\" ] && [ -n \"$last_year\" ]; then",
      "        years=\"${first_year}-${last_year}\"",
      "      fi",
      "      if [ -n \"$sid\" ]; then",
      "        printf '%s_%s_FLUXNET_%s_%s.zip\\n' \"$hub\" \"$sid\" \"$years\" \"$hash\"",
      "      else",
      "        printf '%s_%s.zip\\n' \"$base\" \"$hash\"",
      "      fi",
      "      return 0",
      "      ;;",
      "    download|index|object|objects)",
      "      printf '%s_%s.bin\\n' \"$base\" \"$hash\"",
      "      return 0",
      "      ;;",
      "  esac",
      "",
      "  printf '%s\\n' \"$base\"",
      "}",
      "",
      "download_one() {",
      "  local entry=\"$1\"",
      "  local outdir=\"$2\"",
      "  local logfile=\"$3\"",
      "  local attempt=\"$4\"",
      "  local success_file=\"$5\"",
      "  local failed_file=\"$6\"",
      "",
      "  local site_id=\"\"",
      "  local data_hub=\"\"",
      "  local first_year=\"\"",
      "  local last_year=\"\"",
      "  local url=\"\"",
      "  IFS=$'\\t' read -r site_id data_hub first_year last_year url <<< \"$entry\"",
      "  if [ -z \"${url:-}\" ]; then",
      "    url=\"$entry\"",
      "    site_id=\"\"",
      "    data_hub=\"\"",
      "    first_year=\"\"",
      "    last_year=\"\"",
      "  fi",
      "",
      "  [ -n \"$url\" ] || return 0",
      "",
      "  local filename=\"$(filename_from_url \"$url\" \"$site_id\" \"$data_hub\" \"$first_year\" \"$last_year\")\"",
      "  if [ -z \"$filename\" ]; then",
      "    filename=\"download_$(url_hash \"$url\").bin\"",
      "  fi",
      "",
      "  local final_path=\"$outdir/$filename\"",
      "  local part_path=\"$final_path.part\"",
      "  local tmp_headers=\"$outdir/.headers.$$.tmp\"",
      "  local cookie_jar=\"$outdir/.cookies.$$.txt\"",
      "",
      "  local ts",
      "  ts=\"$(date +'%Y-%m-%dT%H:%M:%S%z')\"",
      "  echo \"$ts ATTEMPT=$attempt URL=$url FILE=$filename STATUS=START\" >> \"$logfile\"",
      "  echo \"Attempt $attempt START $filename\"",
      "",
      "  if [ -f \"$final_path\" ]; then",
      "    ts=\"$(date +'%Y-%m-%dT%H:%M:%S%z')\"",
      "    echo \"$ts ATTEMPT=$attempt URL=$url FILE=$filename STATUS=SKIP_EXISTS\" >> \"$logfile\"",
      "    echo \"Attempt $attempt SKIP $filename (already exists)\"",
      "    echo \"$url\" >> \"$success_file\"",
      "    rm -f \"$tmp_headers\"",
      "    rm -f \"$cookie_jar\"",
      "    return 0",
      "  fi",
      "",
      "  if curl --location --fail -C - \\",
      "      --connect-timeout 20 \\",
      "      --max-time 0 \\",
      "      --speed-time 60 --speed-limit 1024 \\",
      "      --retry 0 \\",
      "      --cookie \"$cookie_jar\" --cookie-jar \"$cookie_jar\" \\",
      "      --silent --show-error \\",
      "      --dump-header \"$tmp_headers\" \\",
      "      --output \"$part_path\" \\",
      "      \"$url\"; then",
      "    local content_type=\"\"",
      "    content_type=\"$(awk -F': ' 'BEGIN{IGNORECASE=1} /^Content-Type:/ {ct=$2} END {gsub(/\\r/, \"\", ct); print tolower(ct)}' \"$tmp_headers\" 2>/dev/null || true)\"",
      "    if echo \"$content_type\" | grep -q 'text/html'; then",
      "      ts=\"$(date +'%Y-%m-%dT%H:%M:%S%z')\"",
      "      echo \"$ts ATTEMPT=$attempt URL=$url FILE=$filename STATUS=FAIL_HTML CONTENT_TYPE=${content_type:-NA}\" >> \"$logfile\"",
      "      echo \"Attempt $attempt FAIL $filename (received HTML; likely license/landing page)\"",
      "      echo \"$url\" >> \"$failed_file\"",
      "      rm -f \"$part_path\" \"$tmp_headers\" \"$cookie_jar\"",
      "      return 1",
      "    fi",
      "",
      "    mv -f \"$part_path\" \"$final_path\"",
      "    ts=\"$(date +'%Y-%m-%dT%H:%M:%S%z')\"",
      "    echo \"$ts ATTEMPT=$attempt URL=$url FILE=$filename STATUS=SUCCESS\" >> \"$logfile\"",
      "    echo \"Attempt $attempt OK $filename\"",
      "    echo \"$url\" >> \"$success_file\"",
      "    rm -f \"$tmp_headers\" \"$cookie_jar\"",
      "    return 0",
      "  fi",
      "",
      "  local http_code=\"\"",
      "  http_code=\"$(awk 'toupper($1) ~ /^HTTP\\// {code=$2} END {print code}' \"$tmp_headers\" 2>/dev/null || true)\"",
      "  rm -f \"$tmp_headers\" \"$cookie_jar\"",
      "",
      "  ts=\"$(date +'%Y-%m-%dT%H:%M:%S%z')\"",
      "  echo \"$ts ATTEMPT=$attempt URL=$url FILE=$filename STATUS=FAIL HTTP=${http_code:-NA}\" >> \"$logfile\"",
      "  echo \"Attempt $attempt FAIL $filename HTTP=${http_code:-NA}\"",
      "  echo \"$url\" >> \"$failed_file\"",
      "  return 1",
      "}",
      "",
      "run_pass() {",
      "  local in_file=\"$1\"",
      "  local attempt=\"$2\"",
      "  local failed_out=\"$3\"",
      "  local pass_total=0",
      "  local start_success=0",
      "  local worker_pid=0",
      "  local current_success=0",
      "  local current_failed=0",
      "  local done_now=0",
      "  local last_reported=-1",
      "",
      "  : > \"$failed_out\"",
      "",
      "  if [ ! -s \"$in_file\" ]; then",
      "    return 0",
      "  fi",
      "",
      "  pass_total=$(wc -l < \"$in_file\" | tr -d ' ')",
      "  start_success=$(wc -l < \"$SUCCESS_FILE\" | tr -d ' ')",
      "  echo \"Pass $attempt: processing $pass_total URLs (mode=$PARALLEL_MODE, concurrency=$CONCURRENCY)\" | tee -a \"$LOGFILE\"",
      "",
      "  if [ \"$PARALLEL_MODE\" = \"xargs\" ]; then",
      "    export -f url_hash safe_token filename_from_url download_one",
      "    while IFS= read -r entry; do",
      "      [ -n \"$entry\" ] || continue",
      "      printf '%s\\0' \"$entry\"",
      "    done < \"$in_file\" | xargs -0 -P \"$CONCURRENCY\" -I {} bash -lc 'set -euo pipefail; download_one \"$@\"' _ \\",
      "      {} \"$OUTDIR\" \"$LOGFILE\" \"$attempt\" \"$SUCCESS_FILE\" \"$failed_out\" &",
      "    worker_pid=$!",
      "    while kill -0 \"$worker_pid\" 2>/dev/null; do",
      "      current_success=$(wc -l < \"$SUCCESS_FILE\" | tr -d ' ')",
      "      current_failed=0",
      "      if [ -s \"$failed_out\" ]; then",
      "        current_failed=$(wc -l < \"$failed_out\" | tr -d ' ')",
      "      fi",
      "      done_now=$((current_success - start_success + current_failed))",
      "      if [ \"$done_now\" -gt \"$pass_total\" ]; then",
      "        done_now=\"$pass_total\"",
      "      fi",
      "      if [ \"$done_now\" -ne \"$last_reported\" ]; then",
      "        echo \"Pass $attempt progress: $done_now/$pass_total completed\" | tee -a \"$LOGFILE\"",
      "        last_reported=\"$done_now\"",
      "      fi",
      "      sleep 15",
      "    done",
      "    wait \"$worker_pid\" || true",
      "  else",
      "    local seq_done=0",
      "    while IFS= read -r entry; do",
      "      [ -n \"$entry\" ] || continue",
      "      seq_done=$((seq_done + 1))",
      "      echo \"Pass $attempt progress: $seq_done/$pass_total\" | tee -a \"$LOGFILE\"",
      "      download_one \"$entry\" \"$OUTDIR\" \"$LOGFILE\" \"$attempt\" \"$SUCCESS_FILE\" \"$failed_out\" || true",
      "    done < \"$in_file\"",
      "  fi",
      "",
      "  sanitize_list \"$failed_out\" > \"$failed_out.tmp\" || true",
      "  mv -f \"$failed_out.tmp\" \"$failed_out\"",
      "}",
      "",
      "attempt=1",
      "queue_file=\"$QUEUE_DIR/urls_0.txt\"",
      "",
      "while [ \"$attempt\" -le \"$MAX_ATTEMPTS\" ]; do",
      "  next_failed=\"$QUEUE_DIR/failed_attempt_${attempt}.txt\"",
      "  run_pass \"$queue_file\" \"$attempt\" \"$next_failed\"",
      "",
      "  remaining=0",
      "  if [ -s \"$next_failed\" ]; then",
      "    remaining=$(wc -l < \"$next_failed\" | tr -d ' ')",
      "  fi",
      "",
      "  ts=\"$(date +'%Y-%m-%dT%H:%M:%S%z')\"",
      "  echo \"$ts PASS=$attempt REMAINING=$remaining\" >> \"$LOGFILE\"",
      "  echo \"Pass $attempt complete: remaining=$remaining\" | tee -a \"$LOGFILE\"",
      "",
      "  queue_file=\"$next_failed\"",
      "",
      "  if [ \"$remaining\" -eq 0 ]; then",
      "    break",
      "  fi",
      "",
      "  if [ \"$attempt\" -lt \"$MAX_ATTEMPTS\" ]; then",
      "    base_sleep=$((2 ** (attempt - 1)))",
      "    jitter=$((RANDOM % 3))",
      "    sleep_for=$((base_sleep + jitter))",
      "    echo \"Retry pass $((attempt + 1)) in ${sleep_for}s (remaining: $remaining)\" | tee -a \"$LOGFILE\"",
      "    sleep \"$sleep_for\"",
      "  fi",
      "",
      "  attempt=$((attempt + 1))",
      "done",
      "",
      "if [ -s \"$queue_file\" ]; then",
      "  cp \"$queue_file\" \"$FAILED_FILE\"",
      "else",
      "  : > \"$FAILED_FILE\"",
      "fi",
      "",
      "SUCCEEDED=0",
      "FAILED=0",
      "if [ -s \"$SUCCESS_FILE\" ]; then",
      "  SUCCEEDED=$(sort -u \"$SUCCESS_FILE\" | tee \"$SUCCESS_FILE.tmp\" | wc -l | tr -d ' ')",
      "  mv -f \"$SUCCESS_FILE.tmp\" \"$SUCCESS_FILE\"",
      "fi",
      "if [ -s \"$FAILED_FILE\" ]; then",
      "  FAILED=$(sort -u \"$FAILED_FILE\" | tee \"$FAILED_FILE.tmp\" | wc -l | tr -d ' ')",
      "  mv -f \"$FAILED_FILE.tmp\" \"$FAILED_FILE\"",
      "fi",
      "",
      "echo \"Done. total=$TOTAL succeeded=$SUCCEEDED failed=$FAILED\" | tee -a \"$LOGFILE\"",
      "echo \"Logs: $LOGFILE\"",
      "echo \"Success list: $SUCCESS_FILE\"",
      "echo \"Failed list: $FAILED_FILE\"",
      "",
      "if [ \"$FAILED\" -gt 0 ]; then",
      "  exit 1",
      "fi",
      ""
    ].join("\n");
  };

  Explorer.prototype.buildShuttleCommandText = function (rows) {
    var shuttleRows = this.getShuttleCliRows(rows);
    var duplicateSiteIds = this.getDuplicateSelectedSiteIds(shuttleRows);
    var skipped = (rows || []).length - shuttleRows.length;
    if (!shuttleRows.length) {
      return [
        "# No Shuttle snapshot rows are selected.",
        "# ICOS-direct and JapanFlux-direct rows are available through direct links / download_shuttle_selected.sh,",
        "# but they are not present in shuttle_snapshot.csv for Shuttle CLI site-id downloads."
      ].join("\n");
    }
    var lines = [
      "# FLUXNET Shuttle CLI syntax (confirmed from shuttle docs):",
      "# fluxnet-shuttle download -f shuttle_snapshot.csv -s SITE1 SITE2 ...",
      "#",
      "# The CLI does not support a --sites-file option, so this helper expands shuttle_selected_sites.txt:",
      "fluxnet-shuttle download -f shuttle_snapshot.csv -o fluxnet_downloads -s $(tr '\\n' ' ' < shuttle_selected_sites.txt)"
    ];
    if (skipped > 0) {
      lines.push(
        "",
        "# Note: " + skipped + " Otherwise-available selection(s) were excluded from this Shuttle CLI command."
      );
    }
    if (duplicateSiteIds.length) {
      lines.push(
        "",
        "# Warning: duplicate site_id values are selected (" + duplicateSiteIds.slice(0, 10).join(", ") +
          (duplicateSiteIds.length > 10 ? ", ..." : "") +
          "). Shuttle download uses site_id keys from the snapshot, so duplicates across hubs can be ambiguous.",
        "# For exact hub-specific downloads, prefer the links file / shell script generated by this page."
      );
    }
    return lines.join("\n");
  };

  Explorer.prototype.buildShuttleExclusionWarning = function (rows) {
    var excluded = uniqueSiteIdsFromRows(this.getAmeriFluxRows(rows)).length;
    if (!excluded) {
      return "";
    }
    return excluded + " AmeriFlux API-backed selection(s) were excluded from the direct-link bulk output.";
  };

  Explorer.prototype.handleDownloadAllSelectedScript = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    this.buildDownloadAllSelectedFiles(rows).forEach(function (file) {
      this.downloadTextFile(file.filename, file.text, file.mimeType);
    }, this);
    this.setBulkStatus("Downloaded download_all_selected.sh, download_ameriflux_selected.sh, and download_shuttle_selected.sh.");
    gaEvent("fx_download_all_script_download", { count: rows.length });
  };

  Explorer.prototype.handleCopyAllSelectedScript = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    this.copyText(this.buildDownloadAllSelectedScript(rows), "Copied download_all_selected.sh wrapper script. Keep it with the generated source-specific scripts.");
    gaEvent("fx_download_all_script_copy", { count: rows.length });
  };

  Explorer.prototype.handleDownloadManifest = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    var shuttleRows = this.getShuttleRows(rows);
    if (!shuttleRows.length) {
      this.setBulkStatus("No direct-source rows are selected. Use the Otherwise-available bulk download tools for AmeriFlux API-backed sites.");
      return;
    }
    this.downloadTextFile("shuttle_selected_manifest.csv", this.buildSelectionManifestCsv(shuttleRows), "text/csv;charset=utf-8");
    var warning = this.buildShuttleExclusionWarning(rows);
    this.setBulkStatus("Downloaded shuttle_selected_manifest.csv for " + shuttleRows.length + " non-AmeriFlux row(s)." + (warning ? (" " + warning) : ""));
    gaEvent("fx_manifest_download", { count: shuttleRows.length });
  };

  Explorer.prototype.handleDownloadLinks = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    var shuttleRows = this.getShuttleDownloadRows(rows);
    var landingPageWarning = this.buildManualLandingPageWarning(rows);
    if (!shuttleRows.length) {
      this.setBulkStatus(landingPageWarning || "No direct-download rows are selected. Otherwise-available rows do not produce static links.");
      return;
    }
    this.downloadTextFile("shuttle_links.txt", this.buildLinksText(shuttleRows), "text/plain;charset=utf-8");
    var warning = this.buildShuttleExclusionWarning(rows);
    this.setBulkStatus("Downloaded shuttle_links.txt for " + shuttleRows.length + " direct-download row(s)." + (warning || landingPageWarning ? (" " + [warning, landingPageWarning].filter(Boolean).join(" ")) : ""));
    gaEvent("fx_links_download", { count: shuttleRows.length });
  };

  Explorer.prototype.handleDownloadScript = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    var shuttleRows = this.getShuttleDownloadRows(rows);
    var landingPageWarning = this.buildManualLandingPageWarning(rows);
    if (!shuttleRows.length) {
      this.setBulkStatus(landingPageWarning || "No direct-download rows are selected. Use the Otherwise-available shell script tools for AmeriFlux API-backed sites.");
      return;
    }
    this.downloadTextFile("download_shuttle_selected.sh", this.buildCurlScript(shuttleRows), "text/x-shellscript;charset=utf-8");
    var warning = this.buildShuttleExclusionWarning(rows);
    this.setBulkStatus("Downloaded download_shuttle_selected.sh for " + shuttleRows.length + " direct-download row(s)." + (warning || landingPageWarning ? (" " + [warning, landingPageWarning].filter(Boolean).join(" ")) : ""));
    gaEvent("fx_script_download", { count: shuttleRows.length });
  };

  Explorer.prototype.handleDownloadSitesFile = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    var shuttleRows = this.getShuttleCliRows(rows);
    if (!shuttleRows.length) {
      this.setBulkStatus("No Shuttle snapshot rows are selected. ICOS-direct and JapanFlux-direct rows can still use download_shuttle_selected.sh or per-row links.");
      return;
    }
    this.downloadTextFile("shuttle_selected_sites.txt", this.buildSelectedSitesText(shuttleRows), "text/plain;charset=utf-8");
    if (shuttleRows.length < rows.length) {
      this.setBulkStatus("Downloaded shuttle_selected_sites.txt for Shuttle-backed rows only.");
      return;
    }
    this.setBulkStatus("Downloaded shuttle_selected_sites.txt.");
  };

  Explorer.prototype.handleDownloadAmeriFluxSitesFile = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    var ameriFluxRows = this.getAmeriFluxRows(rows);
    if (!ameriFluxRows.length) {
      this.setBulkStatus("No Otherwise-available rows are selected.");
      return;
    }
    this.downloadTextFile("ameriflux_selected_sites.txt", this.buildAmeriFluxSelectedSitesText(ameriFluxRows), "text/plain;charset=utf-8");
    this.setBulkStatus("Downloaded ameriflux_selected_sites.txt for " + uniqueSiteIdsFromRows(ameriFluxRows).length + " AmeriFlux API-backed site(s).");
  };

  Explorer.prototype.handleDownloadAmeriFluxScript = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    var ameriFluxRows = this.getAmeriFluxRows(rows);
    if (!ameriFluxRows.length) {
      this.setBulkStatus("No Otherwise-available rows are selected.");
      return;
    }
    this.downloadTextFile("download_ameriflux_selected.sh", this.buildAmeriFluxBulkScript(ameriFluxRows), "text/x-shellscript;charset=utf-8");
    this.setBulkStatus("Downloaded download_ameriflux_selected.sh for " + uniqueSiteIdsFromRows(ameriFluxRows).length + " AmeriFlux API-backed site(s).");
  };

  Explorer.prototype.handleCopyAmeriFluxScript = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    var ameriFluxRows = this.getAmeriFluxRows(rows);
    if (!ameriFluxRows.length) {
      this.setBulkStatus("No Otherwise-available rows are selected.");
      return;
    }
    this.copyText(this.buildAmeriFluxBulkScript(ameriFluxRows), "Copied AmeriFlux API bulk shell script.");
    gaEvent("fx_ameriflux_script_copy", { count: uniqueSiteIdsFromRows(ameriFluxRows).length });
  };

  Explorer.prototype.handleCopyLinks = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    var shuttleRows = this.getShuttleDownloadRows(rows);
    var landingPageWarning = this.buildManualLandingPageWarning(rows);
    if (!shuttleRows.length) {
      this.setBulkStatus(landingPageWarning || "No direct-download rows are selected. Otherwise-available rows require the Otherwise-available bulk shell script.");
      return;
    }
    this.copyText(this.buildLinksText(shuttleRows), "Copied Shuttle links.");
    var warning = this.buildShuttleExclusionWarning(rows);
    if (warning || landingPageWarning) {
      this.setBulkStatus("Copied Shuttle links. " + [warning, landingPageWarning].filter(Boolean).join(" "));
    }
    gaEvent("fx_copy_links", { count: shuttleRows.length });
  };

  Explorer.prototype.handleAmeriFluxRowDownload = function (siteId, dataProduct, sourceLabel, buttonEl) {
    var self = this;
    var site = String(siteId || "").trim();
    var product = normalizeDownloadProduct(dataProduct || AMERIFLUX_FLUXNET_PRODUCT);
    var label = String(sourceLabel || "").trim() || (product === FLUXNET2015_PRODUCT
      ? FLUXNET2015_SOURCE_ONLY
      : (product === AMERIFLUX_BASE_PRODUCT ? BASE_SOURCE_ONLY : AMERIFLUX_SOURCE_ONLY));
    var apiSource = this.getAmeriFluxApiSource(product);
    var identity = this.getEffectiveAmeriFluxIdentity();
    if (!site) {
      return;
    }

    var originalText = buttonEl ? buttonEl.textContent : "";
    var canDirectDownload = apiSource.canDownload();
    if (buttonEl) {
      buttonEl.disabled = true;
      buttonEl.textContent = getApiActionPreparingLabel(product, canDirectDownload);
    }

    apiSource.download_site(site, AMERIFLUX_DEFAULT_VARIANT, AMERIFLUX_DEFAULT_POLICY, identity)
      .then(function (result) {
        if (result && result.manual_download_required) {
          var curlCommand = String(result.curl_command || "").trim();
          if (curlCommand) {
            self.copyText(
              curlCommand,
              "Copied " + apiProductDisplayName(product) + " curl command for " + site + "."
            );
          } else {
            self.setBulkStatus(label + " downloads require your own AmeriFlux identity.");
          }
          gaEvent("fx_ameriflux_manual_command", { site_id: site, data_product: product });
          return;
        }
        var dataUrls = Array.isArray(result && result.data_urls) ? result.data_urls : [];
        var urls = dataUrls.map(function (entry) {
          return String(entry && entry.url || "").trim();
        }).filter(Boolean);
        if (!urls.length) {
          throw new Error("AmeriFlux returned an empty data_urls list.");
        }

        if (typeof window.open === "function") {
          window.open(urls[0], "_blank", "noopener,noreferrer");
        }
        if (urls.length > 1) {
          self.downloadTextFile(product.toLowerCase() + "_" + site + "_links.txt", urls.join("\n") + "\n", "text/plain;charset=utf-8");
        }
        self.setBulkStatus("Requested " + product + " download URL(s) for " + site + " (" + urls.length + " file" + (urls.length === 1 ? "" : "s") + ").");
        gaEvent("fx_ameriflux_download", { site_id: site, count: urls.length, data_product: product });
      })
      .catch(function (error) {
        self.setBulkStatus(product + " download request failed for " + site + ": " + (error && error.message ? error.message : String(error)));
      })
      .then(function () {
        if (buttonEl) {
          buttonEl.disabled = false;
          buttonEl.textContent = originalText || (apiSource.canDownload() ? getApiActionRequestLabel(product) : getApiActionCopyLabel(product));
        }
      });
  };

  Explorer.prototype.toggleCliPanel = function () {
    var rows = this.getSelectedRows();
    this.state.cliPanelVisible = !this.state.cliPanelVisible;
    if (this.state.cliPanelVisible && rows.length) {
      if (this.bindings.cliCommand) {
        this.bindings.cliCommand.textContent = this.buildShuttleCommandText(rows);
      }
    }
    this.render();
  };

  Explorer.prototype.handleCopyCommand = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    var shuttleRows = this.getShuttleCliRows(rows);
    if (!shuttleRows.length) {
      this.setBulkStatus("No Shuttle-backed rows are selected for a Shuttle CLI command.");
      return;
    }
    this.copyText(this.buildShuttleCommandText(rows), "Copied Shuttle CLI helper command.");
    gaEvent("fx_copy_command", { count: shuttleRows.length });
  };

  Explorer.prototype.renderBulkPanel = function () {
    var b = this.bindings;
    var hasData = this.state.mode === "ready" && this.state.rows.length > 0;
    var selectedRows = this.getSelectedRows();
    var selectedCount = selectedRows.length;
    var allowBulkActions = shouldEnableBulkToolsActions(selectedCount);
    var selectionSummary = this.getBulkSelectionSummary(selectedRows);
    var allSelectedDisabled = !selectionSummary.showAllSelectedActions;
    var shuttleDisabled = !selectionSummary.shuttleCount;
    var shuttleCliDisabled = !this.getShuttleCliRows(selectedRows).length;
    var ameriFluxDisabled = !selectionSummary.ameriFluxCount;
    var wasHidden;

    if (!b.bulkPanel) {
      return;
    }

    wasHidden = b.bulkPanel.classList.contains("shuttle-explorer__hidden");
    b.bulkPanel.classList.toggle("shuttle-explorer__hidden", !hasData);
    if (!hasData) {
      b.bulkPanel.open = false;
      this.state.cliPanelVisible = false;
      return;
    }
    if (wasHidden) {
      b.bulkPanel.open = false;
    }

    if (b.selectionCount) {
      b.selectionCount.textContent = formatSelectedSiteCount(selectedCount);
    }

    if (b.allSelectedActions) {
      b.allSelectedActions.classList.toggle("shuttle-explorer__hidden", !(allowBulkActions && selectionSummary.showAllSelectedActions));
    }

    if (b.shuttleSelectionCount) {
      var shuttleSelectionParts = [selectionSummary.shuttleDownloadCount + " direct-download"];
      if (selectionSummary.manualLandingPageCount > 0) {
        shuttleSelectionParts.push(selectionSummary.manualLandingPageCount + " landing-page-only");
      }
      if (selectionSummary.requestOnlyCount > 0) {
        shuttleSelectionParts.push(selectionSummary.requestOnlyCount + " request-only");
      }
      b.shuttleSelectionCount.textContent = shuttleSelectionParts.join(", ") + " site(s) selected";
    }
    if (b.ameriFluxSelectionCount) {
      b.ameriFluxSelectionCount.textContent = selectionSummary.ameriFluxCount + " sites available elsewhere selected";
    }

    if (b.shuttleBulkSection) {
      b.shuttleBulkSection.classList.toggle("shuttle-explorer__hidden", !(allowBulkActions && selectionSummary.showShuttleSection));
    }
    if (b.ameriFluxBulkSection) {
      b.ameriFluxBulkSection.classList.toggle("shuttle-explorer__hidden", !(allowBulkActions && selectionSummary.showAmeriFluxSection));
    }

    if (b.downloadManifest) {
      b.downloadManifest.disabled = shuttleDisabled;
    }
    [
      b.downloadLinks,
      b.downloadScript,
      b.copyLinks
    ].forEach(function (btn) {
      if (btn) {
        btn.disabled = !selectionSummary.shuttleDownloadCount;
      }
    });
    [
      b.downloadSitesFile,
      b.copyCommand
    ].forEach(function (btn) {
      if (btn) {
        btn.disabled = shuttleCliDisabled;
      }
    });

    [
      b.downloadAllSelectedScript,
      b.copyAllSelectedScript
    ].forEach(function (btn) {
      if (btn) {
        btn.disabled = allSelectedDisabled;
      }
    });

    [
      b.downloadAmeriFluxSitesFile,
      b.downloadAmeriFluxScript,
      b.copyAmeriFluxScript
    ].forEach(function (btn) {
      if (btn) {
        btn.disabled = ameriFluxDisabled;
      }
    });

    if (b.showCliCommand) {
      b.showCliCommand.disabled = !allowBulkActions || shuttleCliDisabled;
      b.showCliCommand.textContent = this.state.cliPanelVisible ? "Hide Shuttle CLI command" : "Show Shuttle CLI command";
    }

    if (!allowBulkActions) {
      this.state.cliPanelVisible = false;
      this.setBulkStatus("");
    }

    if (b.cliPanel) {
      b.cliPanel.classList.toggle("shuttle-explorer__hidden", !(allowBulkActions && this.state.cliPanelVisible && !shuttleCliDisabled));
    }

    if (b.cliCommand && !shuttleCliDisabled) {
      b.cliCommand.textContent = this.buildShuttleCommandText(selectedRows);
    } else if (b.cliCommand) {
      b.cliCommand.textContent = "";
    }
  };

  Explorer.prototype.renderTableHeader = function () {
    var self = this;
    var row = this.bindings.theadRow;
    if (!row) {
      return;
    }
    row.innerHTML = "";

    var selectTh = document.createElement("th");
    selectTh.scope = "col";
    selectTh.className = "shuttle-explorer__select-col";
    selectTh.textContent = "Select";
    row.appendChild(selectTh);

    SORT_COLUMNS.forEach(function (col) {
      var th = document.createElement("th");
      th.scope = "col";
      th.setAttribute("aria-sort", "none");
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "shuttle-explorer__sort";
      btn.setAttribute("data-sort-key", col.key);
      btn.innerHTML =
        "<span>" + escapeHtml(col.label) + "</span>" +
        "<span class=\"shuttle-explorer__sort-indicator\" aria-hidden=\"true\">↕</span>";
      th.appendChild(btn);
      row.appendChild(th);
    });

    var downloadTh = document.createElement("th");
    downloadTh.scope = "col";
    var copyButton = document.createElement("button");
    copyButton.type = "button";
    copyButton.className = "shuttle-explorer__table-copy-btn";
    copyButton.setAttribute("data-role", "copy-table-button");
    copyButton.setAttribute("aria-label", COPY_TABLE_BUTTON_LABEL);
    copyButton.textContent = COPY_TABLE_BUTTON_LABEL;
    downloadTh.appendChild(copyButton);
    row.appendChild(downloadTh);

    row.addEventListener("mouseover", function () {
      self.updateHeaderSortIndicators();
    });
  };

  Explorer.prototype.updateHeaderSortIndicators = function () {
    var self = this;
    qsa(this.bindings.theadRow, "th").forEach(function (th) {
      th.setAttribute("aria-sort", "none");
    });
    qsa(this.bindings.theadRow, "button[data-sort-key]").forEach(function (btn) {
      var key = btn.getAttribute("data-sort-key");
      var th = btn.parentNode;
      var indicator = bySelector(btn, ".shuttle-explorer__sort-indicator");
      if (key === self.state.sortKey) {
        var asc = self.state.sortDir !== "desc";
        th.setAttribute("aria-sort", asc ? "ascending" : "descending");
        if (indicator) {
          indicator.textContent = asc ? "↑" : "↓";
        }
      } else if (indicator) {
        indicator.textContent = "↕";
      }
    });
  };

  Explorer.prototype.populateFilters = function () {
    var rows = this.state.rows;
    var b = this.bindings;
    var hubs = [];
    var hubMap = {};
    var networkMap = {};
    var countryMap = {};

    rows.forEach(function (row) {
      if (!hubMap[row.data_hub]) {
        hubMap[row.data_hub] = true;
        hubs.push(row.data_hub);
      }
      row.network_tokens.forEach(function (token) {
        if (token) {
          networkMap[token] = true;
        }
      });
      if (row.country) {
        countryMap[row.country] = true;
      }
    });

    hubs.sort();
    Object.keys(hubMap).forEach(function (hub) {
      if (typeof this.state.selectedHubs[hub] === "undefined") {
        this.state.selectedHubs[hub] = true;
      }
    }, this);

    if (b.hubFilters) {
      b.hubFilters.innerHTML = "";
      var hubPrefix = document.createElement("span");
      hubPrefix.className = "shuttle-explorer__hub-prefix";
      hubPrefix.textContent = "Hubs:";
      b.hubFilters.appendChild(hubPrefix);
      hubs.forEach(function (hub) {
        var label = document.createElement("label");
        label.className = "shuttle-explorer__hub-chip";
        var input = document.createElement("input");
        input.type = "checkbox";
        input.checked = !!this.state.selectedHubs[hub];
        input.setAttribute("data-hub", hub);
        label.appendChild(input);
        label.appendChild(document.createTextNode(" " + hub));
        b.hubFilters.appendChild(label);
      }, this);
    }

    if (b.networkFilter) {
      var currentNetwork = this.state.selectedNetwork;
      b.networkFilter.innerHTML = "<option value=\"\">All networks</option>";
      Object.keys(networkMap).sort().forEach(function (network) {
        var option = document.createElement("option");
        option.value = network;
        option.textContent = network;
        b.networkFilter.appendChild(option);
      });
      b.networkFilter.value = currentNetwork;
      if (b.networkFilter.value !== currentNetwork) {
        this.state.selectedNetwork = "";
      }
    }

    if (b.sourceFilter) {
      var currentSource = this.state.selectedSource;
      b.sourceFilter.innerHTML = "<option value=\"\">All sources</option>";
      uniqueSourceFilterValues(rows).forEach(function (sourceValue) {
        var option = document.createElement("option");
        option.value = sourceValue;
        option.textContent = sourceValue;
        b.sourceFilter.appendChild(option);
      });
      b.sourceFilter.value = currentSource;
      if (b.sourceFilter.value !== currentSource) {
        this.state.selectedSource = "";
      }
    }

    if (b.availabilityFilter) {
      var currentAvailability = this.state.selectedAvailability;
      b.availabilityFilter.innerHTML = "<option value=\"\">All sites</option>";
      uniqueAvailabilityFilterValues(rows).forEach(function (availabilityValue) {
        var option = document.createElement("option");
        option.value = availabilityValue;
        option.textContent = availabilityValue;
        b.availabilityFilter.appendChild(option);
      });
      b.availabilityFilter.value = currentAvailability;
      if (b.availabilityFilter.value !== currentAvailability) {
        this.state.selectedAvailability = "";
      }
    }

    if (b.countryFilter) {
      var currentCountry = this.state.selectedCountry;
      b.countryFilter.innerHTML = "<option value=\"\">All countries</option>";
      Object.keys(countryMap).sort().forEach(function (country) {
        var option = document.createElement("option");
        option.value = country;
        option.textContent = country;
        b.countryFilter.appendChild(option);
      });
      b.countryFilter.value = currentCountry;
      if (b.countryFilter.value !== currentCountry) {
        this.state.selectedCountry = "";
      }
    }

    if (b.vegetationFilter) {
      var currentVegetation = this.state.selectedVegetation;
      b.vegetationFilter.innerHTML = "<option value=\"\">All vegetation types</option>";
      buildVegetationFilterOptions(rows).forEach(function (vegetationOption) {
        var option = document.createElement("option");
        option.value = vegetationOption.value;
        option.textContent = vegetationOption.label;
        b.vegetationFilter.appendChild(option);
      });
      b.vegetationFilter.value = currentVegetation;
      if (b.vegetationFilter.value !== currentVegetation) {
        this.state.selectedVegetation = "";
      }
    }

  };

  Explorer.prototype.updateDerivedState = function () {
    var self = this;
    var search = String(this.state.search || "").trim().toLowerCase();
    var selectedNetwork = this.state.selectedNetwork;
    var selectedSource = this.state.selectedSource;
    var selectedAvailability = this.state.selectedAvailability;
    var selectedCountry = this.state.selectedCountry;
    var selectedVegetation = this.state.selectedVegetation;
    var selectedHubs = this.state.selectedHubs;

    this.state.filteredRows = this.state.rows.filter(function (row) {
      return rowMatchesExplorerFilters(row, {
        search: search,
        selectedNetwork: selectedNetwork,
        selectedSource: selectedSource,
        selectedAvailability: selectedAvailability,
        selectedCountry: selectedCountry,
        selectedVegetation: selectedVegetation,
        selectedHubs: selectedHubs
      });
    }).slice().sort(function (a, b) {
      return compareRows(a, b, self.state.sortKey, self.state.sortDir);
    });

    var totalPages = this.getTotalPages();
    if (this.state.page > totalPages) {
      this.state.page = totalPages;
    }
    if (this.state.page < 1) {
      this.state.page = 1;
    }
  };

  Explorer.prototype.getDisplayedRows = function () {
    return Array.isArray(this.state.filteredRows) ? this.state.filteredRows : [];
  };

  Explorer.prototype.renderRows = function () {
    var tbody = this.bindings.tbody;
    if (!tbody) {
      return;
    }
    var rows = this.getDisplayedRows();
    var pageRows = rows;
    var selectedKeys = this.state.selectedKeys || {};
    var canAmeriFluxDownload = this.ameriFluxSource.canDownload();

    tbody.innerHTML = "";

    pageRows.forEach(function (row) {
      var tr = document.createElement("tr");
      var sourceBadgeHtml = row.source_label
        ? "<div class=\"shuttle-explorer__site-badge\">" + renderSourceBadgeHtml(row.source_label, row.source_reason) + "</div>"
        : "";
      var downloadOptions = buildRowDownloadOptions(row, canAmeriFluxDownload);
      tr.innerHTML = [
        "<td class=\"shuttle-explorer__select-cell\"><input type=\"checkbox\" data-role=\"row-select\" data-key=\"" + escapeHtml(row._selection_key) + "\"" +
          (selectedKeys[row._selection_key] ? " checked" : "") +
          " aria-label=\"Select " + escapeHtml(row.site_id) + "\" /></td>",
        "<td><strong>" + escapeHtml(row.site_id) + "</strong>" + sourceBadgeHtml + "</td>",
        "<td>" + (row.site_name ? escapeHtml(row.site_name) : "<span class=\"shuttle-explorer__muted\">—</span>") + "</td>",
        "<td>" + (row.country ? escapeHtml(row.country) : "<span class=\"shuttle-explorer__muted\">—</span>") + "</td>",
        "<td>" + escapeHtml(row.data_hub) + "</td>",
        "<td>" + (row.vegetation_type ? escapeHtml(row.vegetation_type) : "<span class=\"shuttle-explorer__muted\">—</span>") + "</td>",
        "<td>" + renderSurfacedCoverageHtml(row) + "</td>",
        "<td>" + (row.length_years == null ? "<span class=\"shuttle-explorer__muted\">—</span>" : escapeHtml(row.length_years)) + "</td>"
      ].join("");

      var downloadTd = document.createElement("td");
      downloadTd.className = "shuttle-explorer__download-cell";

      downloadOptions.forEach(function (option) {
        var optionWrap = document.createElement("div");
        var label = null;
        var control;
        optionWrap.className = "shuttle-explorer__download-option";

        if (downloadOptions.length > 1) {
          label = document.createElement("div");
          label.className = "shuttle-explorer__download-option-label";
          label.textContent = option.displayLabel;
          optionWrap.appendChild(label);
        }

        if (option.mode === "ameriflux_api") {
          control = document.createElement("button");
          control.type = "button";
          control.className = "shuttle-explorer__btn shuttle-explorer__btn--small";
          control.setAttribute("data-role", "ameriflux-download");
          control.setAttribute("data-site-id", option.siteId);
          control.setAttribute("data-product", option.dataProduct);
          control.setAttribute("data-source-label", option.sourceLabel);
          control.textContent = option.actionLabel;
          control.setAttribute("aria-label", option.actionLabel + " for " + option.siteId);
          if (option.title) {
            control.title = option.title;
          }
        } else {
          control = document.createElement("a");
          control.className = "shuttle-explorer__btn shuttle-explorer__btn--small";
          control.href = option.downloadLink;
          control.target = "_blank";
          control.rel = "noopener noreferrer";
          control.setAttribute("data-role", "row-link-action");
          control.setAttribute("data-site-id", option.siteId);
          control.setAttribute("data-link-mode", option.mode);
          control.setAttribute("data-source-label", option.sourceLabel);
          control.setAttribute(
            "data-outbound-event",
            option.mode === REQUEST_PAGE_DOWNLOAD_MODE
              ? "fx_request_page_click"
              : (option.mode === LANDING_PAGE_DOWNLOAD_MODE ? "fx_landing_page_click" : "fx_row_download_click")
          );
          control.textContent = option.actionLabel;
          control.setAttribute("aria-label", option.actionLabel + " for " + option.siteId);
          if (option.title) {
            control.title = option.title;
          }
        }

        optionWrap.appendChild(control);
        downloadTd.appendChild(optionWrap);
      });
      tr.appendChild(downloadTd);

      tbody.appendChild(tr);
    });
  };

  Explorer.prototype.renderPagination = function () {
    var b = this.bindings;

    if (!b.pagination) {
      return;
    }
    b.pagination.classList.add("shuttle-explorer__hidden");
    if (b.pageButtons) {
      b.pageButtons.innerHTML = "";
    }
    if (b.pageSummary) {
      b.pageSummary.textContent = "";
    }
  };

  Explorer.prototype.renderSummary = function () {
    var total = this.state.rows.length;
    var filtered = this.state.filteredRows.length;
    if (this.bindings.summary) {
      this.bindings.summary.textContent = "Showing " + filtered + " of " + total + " sites.";
    }
  };

  Explorer.prototype.renderEmptyState = function () {
    var empty = this.bindings.empty;
    if (!empty) {
      return;
    }
    if (this.state.mode === "error") {
      empty.classList.remove("shuttle-explorer__hidden");
      empty.innerHTML =
        "<strong>Could not load snapshot.</strong> " +
        escapeHtml(this.state.errorMessage || "Unknown error.") +
        " " +
        "<a href=\"" + escapeHtml(this.csvUrl) + "\" target=\"_blank\" rel=\"noopener noreferrer\">Open CSV snapshot</a>";
      return;
    }

    if (this.state.mode !== "ready") {
      empty.classList.add("shuttle-explorer__hidden");
      empty.innerHTML = "";
      return;
    }

    if (!this.state.rows.length) {
      empty.classList.remove("shuttle-explorer__hidden");
      empty.innerHTML = "Snapshot loaded, but no rows are available.";
      return;
    }

    if (!this.state.filteredRows.length) {
      empty.classList.remove("shuttle-explorer__hidden");
      empty.innerHTML = "No matching sites. <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"empty-reset\">Reset filters</button>";
      return;
    }

    empty.classList.add("shuttle-explorer__hidden");
    empty.innerHTML = "";
  };

  Explorer.prototype.renderHubSummaryInStatus = function () {
    var total = this.state.rows.length;
    var msg = "Data is available for a total of " + total + " sites.";
    if (this.state.warning) {
      msg += " " + this.state.warning;
    }
    this.setMode("ready", msg, "is-ok");
  };

  Explorer.prototype.render = function () {
    var b = this.bindings;
    var hasData = this.state.mode === "ready" && this.state.rows.length > 0;
    var hasMatches = hasData && this.state.filteredRows.length > 0;

    if (this.state.mode === "loading") {
      this.setMode("loading", "Loading snapshot…", "is-loading");
    } else if (this.state.mode === "error") {
      this.setMode("error", "Error loading snapshot.", "is-error");
    } else if (this.state.mode === "ready") {
      this.renderHubSummaryInStatus();
    }

    this.renderSnapshotUpdatedText();

    if (b.controls) {
      b.controls.classList.toggle("shuttle-explorer__hidden", !hasData);
    }
    if (b.hubFilters) {
      b.hubFilters.classList.toggle("shuttle-explorer__hidden", !hasData);
    }
    if (b.summaryRow) {
      b.summaryRow.classList.toggle("shuttle-explorer__hidden", !hasData);
    }
    if (b.selectionActions) {
      b.selectionActions.classList.toggle("shuttle-explorer__hidden", !hasData);
    }
    if (b.mapPanel) {
      b.mapPanel.classList.toggle("shuttle-explorer__hidden", !hasData);
    }
    this.renderBulkPanel();
    if (b.tableWrap) {
      b.tableWrap.classList.toggle("shuttle-explorer__hidden", !hasMatches);
    }

    this.updateHeaderSortIndicators();
    if (hasData) {
      this.renderSummary();
    }
    if (hasMatches) {
      this.renderRows();
    } else if (b.tbody) {
      b.tbody.innerHTML = "";
    }
    this.renderPagination();
    this.renderEmptyState();
    if (hasData) {
      this.renderMap();
    }

    var emptyReset = bySelector(this.bindings.empty, "[data-role='empty-reset']");
    if (emptyReset && !emptyReset._boundReset) {
      emptyReset._boundReset = true;
      emptyReset.addEventListener("click", this.resetFilters.bind(this));
    }
  };

  Explorer.prototype.applyLoadedSnapshotState = function (snapshot) {
    this.state.rows = Array.isArray(snapshot && snapshot.rows)
      ? snapshot.rows.map(function (row) {
        return finalizeRowComputedState(Object.assign({}, row));
      })
      : [];
    this.pruneSelection();
    this.state.droppedRows = snapshot && snapshot.droppedRows ? snapshot.droppedRows : 0;
    this.state.source = snapshot && snapshot.source ? snapshot.source : "";
    this.state.sourceUrl = snapshot && snapshot.sourceUrl ? snapshot.sourceUrl : "";
    this.state.warning = snapshot && snapshot.warning ? snapshot.warning : "";
    this.state.snapshotUpdatedDate = snapshot && snapshot.snapshotUpdatedDate ? snapshot.snapshotUpdatedDate : "";
    this.state.amerifluxTotalSites = snapshot && snapshot.amerifluxTotalSites ? snapshot.amerifluxTotalSites : 0;
    this.state.amerifluxSitesWithYears = snapshot && snapshot.amerifluxSitesWithYears ? snapshot.amerifluxSitesWithYears : 0;
    this.state.amerifluxOverlapSites = snapshot && snapshot.amerifluxOverlapSites ? snapshot.amerifluxOverlapSites : 0;
    this.state.amerifluxOnlySites = snapshot && snapshot.amerifluxOnlySites ? snapshot.amerifluxOnlySites : 0;
    this.state.fluxnet2015TotalSites = snapshot && snapshot.fluxnet2015TotalSites ? snapshot.fluxnet2015TotalSites : 0;
    this.state.fluxnet2015SitesWithYears = snapshot && snapshot.fluxnet2015SitesWithYears ? snapshot.fluxnet2015SitesWithYears : 0;
    this.state.fluxnet2015OnlySites = snapshot && snapshot.fluxnet2015OnlySites ? snapshot.fluxnet2015OnlySites : 0;
    this.state.errorMessage = "";

    this.populateFilters();
    this.updateDerivedState();
    this.state.mode = "ready";
    this.render();
    this.trackExplorerLoadedOnce();
  };

  Explorer.prototype.buildMergedSnapshotState = function (shuttleResult, icosDirectResult, japanFluxResult, efdResult, ameriResult, ameriBaseResult, fluxnet2015Result, ameriFluxSiteInfoResult, fluxnet2015SiteInfoResult, siteNameMetadataResult, vegetationMetadataResult) {
    var ameriFluxSiteInfoLookup = ameriFluxSiteInfoResult && ameriFluxSiteInfoResult.lookup ? ameriFluxSiteInfoResult.lookup : {};
    var fluxnet2015SiteInfoLookup = fluxnet2015SiteInfoResult && fluxnet2015SiteInfoResult.lookup ? fluxnet2015SiteInfoResult.lookup : {};
    var siteNameMetadataLookup = siteNameMetadataResult && siteNameMetadataResult.lookup ? siteNameMetadataResult.lookup : {};
    var vegetationMetadataLookup = vegetationMetadataResult && vegetationMetadataResult.lookup ? vegetationMetadataResult.lookup : {};
    var enrichedAmeriFluxSites = enrichAmeriFluxSitesWithMetadata(
      ameriResult && Array.isArray(ameriResult.sites) ? ameriResult.sites : [],
      ameriFluxSiteInfoLookup,
      vegetationMetadataLookup
    );
    var enrichedAmeriFluxBaseSites = enrichAmeriFluxSitesWithMetadata(
      ameriBaseResult && Array.isArray(ameriBaseResult.sites) ? ameriBaseResult.sites : [],
      ameriFluxSiteInfoLookup,
      vegetationMetadataLookup
    );
    var enrichedFluxnet2015Sites = enrichFluxnet2015SitesWithMetadata(
      fluxnet2015Result && Array.isArray(fluxnet2015Result.sites) ? fluxnet2015Result.sites : [],
      fluxnet2015SiteInfoLookup,
      vegetationMetadataLookup
    );
    var merge = mergeCatalogRows(
      shuttleResult && Array.isArray(shuttleResult.rows) ? shuttleResult.rows : [],
      icosDirectResult && Array.isArray(icosDirectResult.rows) ? icosDirectResult.rows : [],
      japanFluxResult && Array.isArray(japanFluxResult.rows) ? japanFluxResult.rows : [],
      enrichedAmeriFluxSites,
      enrichedFluxnet2015Sites,
      enrichedAmeriFluxBaseSites,
      efdResult && Array.isArray(efdResult.rows) ? efdResult.rows : []
    );
    var rows = enrichRowsWithSiteNameLookup(merge.rows, siteNameMetadataLookup);
    return {
      rows: rows,
      droppedRows: shuttleResult && shuttleResult.droppedRows ? shuttleResult.droppedRows : 0,
      source: "Shuttle + ICOS + JapanFlux + AmeriFlux FLUXNET + AmeriFlux BASE + FLUXNET2015 + EFD",
      sourceUrl: shuttleResult && shuttleResult.sourceUrl ? shuttleResult.sourceUrl : this.jsonUrl,
      warning: combineWarnings(
        shuttleResult && shuttleResult.warning ? shuttleResult.warning : "",
        icosDirectResult && icosDirectResult.warning ? icosDirectResult.warning : "",
        japanFluxResult && japanFluxResult.warning ? japanFluxResult.warning : "",
        efdResult && efdResult.warning ? efdResult.warning : "",
        ameriResult && ameriResult.warning ? ameriResult.warning : "",
        ameriBaseResult && ameriBaseResult.warning ? ameriBaseResult.warning : "",
        fluxnet2015Result && fluxnet2015Result.warning ? fluxnet2015Result.warning : "",
        ameriFluxSiteInfoResult && ameriFluxSiteInfoResult.warning ? ameriFluxSiteInfoResult.warning : "",
        fluxnet2015SiteInfoResult && fluxnet2015SiteInfoResult.warning ? fluxnet2015SiteInfoResult.warning : "",
        siteNameMetadataResult && siteNameMetadataResult.warning ? siteNameMetadataResult.warning : "",
        vegetationMetadataResult && vegetationMetadataResult.warning ? vegetationMetadataResult.warning : ""
      ),
      snapshotUpdatedDate: extractSnapshotUpdatedDate(shuttleResult && shuttleResult.meta ? shuttleResult.meta : {}),
      amerifluxTotalSites: ameriResult && ameriResult.totalSites ? ameriResult.totalSites : 0,
      amerifluxSitesWithYears: ameriResult && ameriResult.sitesWithYears ? ameriResult.sitesWithYears : 0,
      amerifluxOverlapSites: merge.amerifluxOverlapSites || 0,
      amerifluxOnlySites: merge.amerifluxOnlySites || 0,
      fluxnet2015TotalSites: fluxnet2015Result && fluxnet2015Result.totalSites ? fluxnet2015Result.totalSites : 0,
      fluxnet2015SitesWithYears: fluxnet2015Result && fluxnet2015Result.sitesWithYears ? fluxnet2015Result.sitesWithYears : 0,
      fluxnet2015OnlySites: merge.fluxnet2015OnlySites || 0
    };
  };

  Explorer.prototype.load = function () {
    var self = this;
    var cached = readSnapshotCache(this.jsonUrl, this.csvUrl);
    var hadCache = !!(cached && Array.isArray(cached.rows) && cached.rows.length);

    this.setMode("loading", "Loading snapshot…", "is-loading");
    if (hadCache) {
      this.applyLoadedSnapshotState({
        rows: cached.rows,
        droppedRows: cached.droppedRows || 0,
        source: cached.source || "cache",
        sourceUrl: cached.sourceUrl || this.jsonUrl,
        warning: cached.warning || "",
          snapshotUpdatedDate: cached.snapshotUpdatedDate || "",
        amerifluxTotalSites: cached.amerifluxTotalSites || 0,
        amerifluxSitesWithYears: cached.amerifluxSitesWithYears || 0,
        amerifluxOverlapSites: cached.amerifluxOverlapSites || 0,
        amerifluxOnlySites: cached.amerifluxOnlySites || 0,
        fluxnet2015TotalSites: cached.fluxnet2015TotalSites || 0,
        fluxnet2015SitesWithYears: cached.fluxnet2015SitesWithYears || 0,
        fluxnet2015OnlySites: cached.fluxnet2015OnlySites || 0
      });
    }

    Promise.all([
      this.shuttleSource.list_sites(),
      this.icosDirectSource.list_sites(),
      this.japanFluxSource.list_sites(),
      this.efdSource.list_sites(),
      this.ameriFluxSource.list_sites(),
      this.ameriFluxBaseSource.list_sites(),
      this.fluxnet2015Source.list_sites(),
      loadAmeriFluxSiteInfo(this.ameriFluxSiteInfoUrl),
      loadFluxnet2015SiteInfo(this.fluxnet2015SiteInfoUrl),
      loadSiteNameMetadata(this.siteNameMetadataUrl),
      loadVegetationMetadata(this.vegetationMetadataUrl)
    ])
      .then(function (results) {
        var shuttleResult = results[0] || {};
        var icosDirectResult = results[1] || {};
        var japanFluxResult = results[2] || {};
        var efdResult = results[3] || {};
        var ameriResult = results[4] || {};
        var ameriBaseResult = results[5] || {};
        var fluxnet2015Result = results[6] || {};
        var ameriFluxSiteInfoResult = results[7] || {};
        var fluxnet2015SiteInfoResult = results[8] || {};
        var siteNameMetadataResult = results[9] || {};
        var vegetationMetadataResult = results[10] || {};
        var snapshotState = self.buildMergedSnapshotState(
          shuttleResult,
          icosDirectResult,
          japanFluxResult,
          efdResult,
          ameriResult,
          ameriBaseResult,
          fluxnet2015Result,
          ameriFluxSiteInfoResult,
          fluxnet2015SiteInfoResult,
          siteNameMetadataResult,
          vegetationMetadataResult
        );
        var freshnessKey = [
          "shuttle:" + buildSnapshotFreshnessKey(shuttleResult),
          "icos-direct:" + buildSnapshotFreshnessKey(icosDirectResult),
          "japanflux-direct:" + buildSnapshotFreshnessKey(japanFluxResult),
          "efd:" + buildSnapshotFreshnessKey(efdResult),
          String(ameriResult.freshnessKey || "ameriflux:none"),
          String(ameriBaseResult.freshnessKey || "ameriflux-base:none"),
          String(fluxnet2015Result.freshnessKey || "fluxnet2015:none"),
          "ameriflux-site-info:" + buildSnapshotFreshnessKey(ameriFluxSiteInfoResult),
          "fluxnet2015-site-info:" + buildSnapshotFreshnessKey(fluxnet2015SiteInfoResult),
          "site-name-metadata:" + buildSnapshotFreshnessKey(siteNameMetadataResult),
          "vegetation-metadata:" + buildSnapshotFreshnessKey(vegetationMetadataResult)
        ].join("|");

        self.applyLoadedSnapshotState(snapshotState);
        writeSnapshotCache(self.jsonUrl, self.csvUrl, {
          freshnessKey: freshnessKey,
          rows: snapshotState.rows,
          droppedRows: snapshotState.droppedRows,
          source: snapshotState.source,
          sourceUrl: snapshotState.sourceUrl,
          warning: snapshotState.warning,
          snapshotUpdatedDate: snapshotState.snapshotUpdatedDate,
          amerifluxTotalSites: snapshotState.amerifluxTotalSites,
          amerifluxSitesWithYears: snapshotState.amerifluxSitesWithYears,
          amerifluxOverlapSites: snapshotState.amerifluxOverlapSites,
          amerifluxOnlySites: snapshotState.amerifluxOnlySites,
          fluxnet2015TotalSites: snapshotState.fluxnet2015TotalSites,
          fluxnet2015SitesWithYears: snapshotState.fluxnet2015SitesWithYears,
          fluxnet2015OnlySites: snapshotState.fluxnet2015OnlySites
        });
      })
      .catch(function (error) {
        if (hadCache && self.state.mode === "ready" && self.state.rows.length) {
          var msg = "Update check failed; showing cached snapshot.";
          self.state.warning = self.state.warning ? (self.state.warning + " " + msg) : msg;
          self.render();
          return;
        }
        self.state.mode = "error";
        self.state.errorMessage = error && error.message ? error.message : String(error);
        self.state.rows = [];
        self.state.filteredRows = [];
        self.render();
      });
  };

  function initOne(root) {
    try {
      var explorer = new Explorer(root);
      explorer.load();
      root._shuttleExplorer = explorer;
    } catch (error) {
      if (window.console && console.error) {
        console.error("Failed to initialize shuttle explorer", error);
      }
      root.innerHTML =
        "<div class=\"shuttle-explorer shuttle-explorer__status is-error\">Failed to initialize FLUXNET explorer.</div>";
    }
  }

  function findRoots() {
    var roots = qsa(document, "[data-shuttle-explorer]");
    var idRoot = document.getElementById("shuttle-explorer");
    if (idRoot && roots.indexOf(idRoot) === -1) {
      roots.unshift(idRoot);
    }
    return roots;
  }

  function initAll() {
    ensureStyles();
    var roots = findRoots();
    if (!roots.length) {
      if (window.console && console.warn) {
        console.warn("shuttle-explorer.js loaded but no root container found (#shuttle-explorer or [data-shuttle-explorer]).");
      }
      return;
    }
    roots.forEach(function (root) {
      if (root && !root._shuttleExplorer) {
        initOne(root);
      }
    });
  }

  var testHooks = {
    payloadJsonToObjects: payloadJsonToObjects,
    parseAmeriFluxAvailabilityPayload: parseAmeriFluxAvailabilityPayload,
    mergeCatalogRows: mergeCatalogRows,
    mergeShuttleAndAmeriFluxRows: mergeShuttleAndAmeriFluxRows,
    countryCodeToName: countryCodeToName,
    normalizeCountryName: normalizeCountryName,
    inferFluxnet2015NetworkFromCountry: inferFluxnet2015NetworkFromCountry,
    deriveCountry: deriveCountry,
    normalizeNetworkToken: normalizeNetworkToken,
    normalizeNetworkTokens: normalizeNetworkTokens,
    normalizeNetworkDisplayValue: normalizeNetworkDisplayValue,
    normalizeVegetationType: normalizeVegetationType,
    vegetationDisplayLabel: vegetationDisplayLabel,
    calculateCoverageLength: calculateCoverageLength,
    shouldEnableBulkToolsActions: shouldEnableBulkToolsActions,
    formatSelectedSiteCount: formatSelectedSiteCount,
    resolveAmeriFluxBulkIdentity: resolveAmeriFluxBulkIdentity,
    normalizeSnapshotUpdatedDate: normalizeSnapshotUpdatedDate,
    extractSnapshotUpdatedDate: extractSnapshotUpdatedDate,
    snapshotUpdatedDateDisplayText: snapshotUpdatedDateDisplayText,
    buildAttributionText: buildAttributionText,
    getDownloadEndpointForProduct: getDownloadEndpointForProduct,
    buildV2DownloadPayload: buildV2DownloadPayload,
    buildV1DownloadPayload: buildV1DownloadPayload,
    exactYearSetsMatch: exactYearSetsMatch,
    resolveProcessingLineage: resolveProcessingLineage,
    stripUrlQueryForFilename: stripUrlQueryForFilename,
    filenameFromUrl: filenameFromUrl,
    buildAmeriFluxCurlCommand: buildAmeriFluxCurlCommand,
    buildAmeriFluxSelectedSitesText: buildAmeriFluxSelectedSitesText,
    buildAmeriFluxBulkScriptText: buildAmeriFluxBulkScriptText,
    buildDownloadAllSelectedScriptText: buildDownloadAllSelectedScriptText,
    buildDownloadAllSelectedFileBundle: buildDownloadAllSelectedFileBundle,
    buildTableClipboardText: buildTableClipboardText,
    getSurfacedProductsForRow: getSurfacedProductsForRow,
    buildRowDownloadOptions: buildRowDownloadOptions,
    renderSurfacedCoverageHtml: renderSurfacedCoverageHtml,
    partitionRowsByBulkSource: partitionRowsByBulkSource,
    summarizeBulkSelection: summarizeBulkSelection,
    computeSourceFilterTags: computeSourceFilterTags,
    uniqueSourceFilterValues: uniqueSourceFilterValues,
    uniqueAvailabilityFilterValues: uniqueAvailabilityFilterValues,
    rowMatchesExplorerFilters: rowMatchesExplorerFilters,
    uniqueVegetationFilterValues: uniqueVegetationFilterValues,
    buildVegetationFilterOptions: buildVegetationFilterOptions,
    summarizeApiOnlyRowCoordinateCoverage: summarizeApiOnlyRowCoordinateCoverage,
    buildCoordinateLookup: buildCoordinateLookup,
    enrichRowsWithCoordinateLookup: enrichRowsWithCoordinateLookup,
    buildAmeriFluxSiteInfoLookup: buildAmeriFluxSiteInfoLookup,
    buildSiteNameMetadataLookup: buildSiteNameMetadataLookup,
    buildVegetationMetadataLookup: buildVegetationMetadataLookup,
    enrichRowsWithSiteNameLookup: enrichRowsWithSiteNameLookup,
    enrichAmeriFluxSitesWithMetadata: enrichAmeriFluxSitesWithMetadata,
    buildFluxnet2015SiteLookup: buildFluxnet2015SiteLookup,
    enrichFluxnet2015SitesWithMetadata: enrichFluxnet2015SitesWithMetadata,
    createAmeriFluxSource: function (options) {
      return new AmeriFluxSource(options || {});
    }
  };

  if (typeof module === "object" && module.exports) {
    module.exports = testHooks;
  }
  if (typeof window !== "undefined") {
    window.ShuttleExplorerTestHooks = testHooks;
  }

  if (typeof document !== "undefined") {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", initAll);
    } else {
      initAll();
    }
  }
})();
