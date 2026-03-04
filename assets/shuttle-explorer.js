(function () {
  "use strict";

  var DEFAULT_JSON_URL = "assets/shuttle_snapshot.json";
  var DEFAULT_CSV_URL = "assets/shuttle_snapshot.csv";
  var DEFAULT_PAGE_SIZE = 10;
  var MAX_PAGE_BUTTONS = 7;
  var SEARCH_DEBOUNCE_MS = 180;
  var STYLE_ID = "shuttle-explorer-inline-styles";
  var SNAPSHOT_CACHE_SCHEMA_VERSION = 1;
  var SNAPSHOT_CACHE_STORAGE_PREFIX = "shuttle-explorer:snapshot-cache:v1";

  var SORT_COLUMNS = [
    { key: "site_id", label: "Site ID", type: "string" },
    { key: "site_name", label: "Site Name", type: "string" },
    { key: "country", label: "Country", type: "string" },
    { key: "data_hub", label: "Hub", type: "string" },
    { key: "network", label: "Network", type: "string" },
    { key: "vegetation_type", label: "Veg Type", type: "string" },
    { key: "years", label: "Years", type: "years" }
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

  function formatLastUpdatedLabel(rawHeaderValue) {
    var raw = String(rawHeaderValue || "").trim();
    if (!raw) {
      return "";
    }
    var dt = new Date(raw);
    var formatted = formatIsoDate(dt);
    return formatted ? " (last updated: " + formatted + ")" : "";
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
      lastUpdatedLabel: entry.lastUpdatedLabel || "",
      rows: entry.rows,
      droppedRows: entry.droppedRows || 0,
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

  function deriveCountry(siteId, fallback) {
    var fb = String(fallback || "").trim();
    if (fb) {
      return fb;
    }
    var s = String(siteId || "").trim();
    if (!s) {
      return "";
    }
    var idx = s.indexOf("-");
    if (idx > 0) {
      return s.slice(0, idx).toUpperCase();
    }
    idx = s.indexOf("_");
    if (idx > 0) {
      return s.slice(0, idx).toUpperCase();
    }
    return s.slice(0, 2).toUpperCase();
  }

  function isIcosRow(row) {
    var hub = String(row.data_hub || "").toLowerCase();
    var network = String(row.network || "").toLowerCase();
    var url = String(row.download_link || "").toLowerCase();
    return hub === "icos" || network.indexOf("icos") !== -1 || /data\.icos-cp\.eu\/licence_accept/.test(url);
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
    var downloadLink = String(raw.download_link || raw.url || "").trim();

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
      download_link: downloadLink
    };

    row.is_icos = isIcosRow(row);
    row.search_text = (siteId + " " + siteName + " " + networkDisplay + " " + sourceNetwork + " " + vegetationType).toLowerCase();
    return row;
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
      return "meta:" + String(result.meta.version);
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
    return fetchJson(jsonUrl)
      .then(function (jsonResult) {
        var meta = extractSnapshotMeta(jsonResult.payload);
        return {
          rawRows: payloadJsonToObjects(jsonResult.payload),
          source: "json",
          sourceUrl: jsonUrl,
          lastModified: jsonResult.lastModified || "",
          meta: meta
        };
      })
      .catch(function (jsonError) {
        return fetchText(csvUrl).then(function (csvResult) {
          return {
            rawRows: csvTextToObjects(csvResult.text),
            source: "csv",
            sourceUrl: csvUrl,
            lastModified: csvResult.lastModified || "",
            meta: {},
            warning: "JSON snapshot unavailable; loaded CSV fallback.",
            jsonError: jsonError
          };
        }).catch(function (csvError) {
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
      });
  }

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
      av = sortKey === "network" ? (a.network_display || a.network) : a[sortKey];
      bv = sortKey === "network" ? (b.network_display || b.network) : b[sortKey];
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

  function formatHubCounts(rows) {
    var counts = {};
    rows.forEach(function (row) {
      var hub = row.data_hub || "Unknown";
      counts[hub] = (counts[hub] || 0) + 1;
    });
    return Object.keys(counts).sort().map(function (hub) {
      return hub + ": " + counts[hub];
    }).join(" | ");
  }

  function buildAttributionText() {
    var accessDate = new Date().toISOString().slice(0, 10);
    return [
      "The FLUXNET data presented here were discovered using a FLUXNET Shuttle metadata snapshot accessed via the Q.E.D. Lab FLUXNET Data Explorer.",
      "FLUXNET data are shared openly under the CC-BY 4.0 data use license, which requires attribution.",
      "Please cite the dataset/product-specific citation(s) provided with each downloaded archive, along with any other attribution requirements contained therein.",
      "Available data is updated as of: " + accessDate + "."
    ].join(" ");
  }

  function csvEscape(value) {
    var s = String(value == null ? "" : value);
    if (/[",\r\n]/.test(s)) {
      return "\"" + s.replace(/"/g, "\"\"") + "\"";
    }
    return s;
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
      ".shuttle-explorer__controls{display:grid;grid-template-columns:2fr 1fr 1fr;gap:10px;margin:0 0 10px;}",
      ".shuttle-explorer__field{display:flex;flex-direction:column;gap:4px;}",
      ".shuttle-explorer__field label{font-size:.82em;color:#4d5b6a;}",
      ".shuttle-explorer__field input,.shuttle-explorer__field select{width:100%;padding:7px 8px;border:1px solid #b7c1ce;border-radius:6px;background:#fff;font:inherit;}",
      ".shuttle-explorer__hub-filters{display:flex;flex-wrap:wrap;gap:8px;margin:0 0 10px;}",
      ".shuttle-explorer__hub-chip{display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border:1px solid #d5dbe3;border-radius:999px;background:#f8fafc;font-size:.88em;}",
      ".shuttle-explorer__row{display:flex;justify-content:space-between;align-items:center;gap:10px;margin:0 0 10px;}",
      ".shuttle-explorer__summary{font-size:.9em;color:#33475b;}",
      ".shuttle-explorer__btn{display:inline-block;padding:7px 10px;border:1px solid #b7c1ce;border-radius:6px;background:#fff;color:#23364a;text-decoration:none;font:inherit;cursor:pointer;}",
      ".shuttle-explorer__btn:hover,.shuttle-explorer__btn:focus{background:#eef3f9;text-decoration:none;}",
      ".shuttle-explorer__btn:focus{outline:2px solid #2f5374;outline-offset:1px;}",
      ".shuttle-explorer__btn--small{padding:5px 8px;font-size:.86em;}",
      ".shuttle-explorer__btn[disabled]{opacity:.45;cursor:default;}",
      ".shuttle-explorer__table-wrap{overflow:auto;border:1px solid #d5dbe3;border-radius:8px;background:#fff;}",
      ".shuttle-explorer__table{width:100%;border-collapse:collapse;min-width:760px;font-size:.9em;}",
      ".shuttle-explorer__table th,.shuttle-explorer__table td{padding:8px 10px;border-bottom:1px solid #edf1f5;vertical-align:top;text-align:left;}",
      ".shuttle-explorer__table thead th{position:sticky;top:0;background:#f8fafc;z-index:1;}",
      ".shuttle-explorer__sort{display:inline-flex;align-items:center;gap:4px;border:0;background:transparent;padding:0;margin:0;color:inherit;font:inherit;cursor:pointer;}",
      ".shuttle-explorer__sort-indicator{color:#6b7a89;font-size:.9em;}",
      ".shuttle-explorer__muted{color:#607184;}",
      ".shuttle-explorer__pagination{display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin:10px 0 0;}",
      ".shuttle-explorer__pages{display:flex;flex-wrap:wrap;gap:6px;}",
      ".shuttle-explorer__page.is-active{background:#e8f0fb;border-color:#a8bddb;}",
      ".shuttle-explorer__empty{margin:10px 0 0;padding:12px;border:1px dashed #c6d1dd;border-radius:8px;background:#fbfdff;color:#3a4d60;}",
      ".shuttle-explorer__hidden{display:none !important;}",
      ".shuttle-explorer__attribution{margin:12px 0 0;padding:10px;border:1px solid #d5dbe3;border-radius:8px;background:#ffffff;}",
      ".shuttle-explorer__attribution h3{margin:0 0 6px;font-size:.95em;}",
      ".shuttle-explorer__attribution textarea{width:100%;min-height:90px;padding:8px;border:1px solid #b7c1ce;border-radius:6px;font:inherit;line-height:1.35;resize:vertical;background:#fdfefe;}",
      ".shuttle-explorer__attribution-row{display:flex;justify-content:space-between;align-items:center;gap:10px;margin:8px 0 0;}",
      ".shuttle-explorer__tiny{font-size:.82em;color:#556779;}",
      "@media (max-width: 860px){.shuttle-explorer__controls{grid-template-columns:1fr;}.shuttle-explorer__row{flex-direction:column;align-items:flex-start;}}"
    ].join("");
    document.head.appendChild(style);
  }

  function createLayout(root) {
    root.classList.add("shuttle-explorer");
    root.innerHTML = [
      "<div class=\"shuttle-explorer__header\">",
      "  <h2>FLUXNET Data Explorer</h2>",
      "  <p class=\"shuttle-explorer__muted\">Explore and download a regularly refreshed snapshot of the FLUXNET database<span data-role=\"widget-last-updated-inline\"></span>, with observations from the various regional networks. Search by site ID or site name, then open hub-hosted download links directly from the table. Data are provided by site teams from around the world and served via the FLUXNET Shuttle.</p>",
      "</div>",
      "<p class=\"shuttle-explorer__status is-loading\" data-role=\"status\" role=\"status\" aria-live=\"polite\">Loading snapshot…</p>",
      "<div class=\"shuttle-explorer__controls shuttle-explorer__hidden\" data-role=\"controls\">",
      "  <div class=\"shuttle-explorer__field\">",
      "    <label for=\"shuttle-search\">Search (site ID or site name)</label>",
      "    <input id=\"shuttle-search\" type=\"search\" placeholder=\"e.g., US-Ton or Tonzi\" data-role=\"search\" />",
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
      "    <label for=\"shuttle-vegetation\">Vegetation type</label>",
      "    <select id=\"shuttle-vegetation\" data-role=\"vegetation-filter\"><option value=\"\">All vegetation types</option></select>",
      "  </div>",
      "</div>",
      "<div class=\"shuttle-explorer__hub-filters shuttle-explorer__hidden\" data-role=\"hub-filters\" aria-label=\"Hub filters\"></div>",
      "<div class=\"shuttle-explorer__row shuttle-explorer__hidden\" data-role=\"summary-row\">",
      "  <div class=\"shuttle-explorer__summary\" data-role=\"summary\"></div>",
      "  <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"reset\">Reset filters</button>",
      "</div>",
      "<section class=\"shuttle-explorer__bulk shuttle-explorer__hidden\" data-role=\"bulk-panel\" aria-labelledby=\"shuttle-bulk-heading\">",
      "  <div class=\"shuttle-explorer__bulk-header\">",
      "    <h3 id=\"shuttle-bulk-heading\">Bulk download tools</h3>",
      "    <p class=\"shuttle-explorer__tiny shuttle-explorer__bulk-count\" data-role=\"selection-count\">0 selected</p>",
      "  </div>",
      "  <p class=\"shuttle-explorer__tiny\">Use the selection tools below to create a manifest, links file, or shell script for batch downloads. This avoids browser popup/download limits when working with many sites.</p>",
      "  <div class=\"shuttle-explorer__bulk-actions\">",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"select-filtered\">Select all (filtered results)</button>",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"select-all-sites\">Select all (all sites)</button>",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"clear-selection\">Clear selection</button>",
      "  </div>",
      "  <div class=\"shuttle-explorer__bulk-actions\">",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"download-manifest\">Download manifest (CSV)</button>",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"download-links\">Download links file (TXT)</button>",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"download-script\">Download script (sh)</button>",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"download-sites-file\">Download selected_sites.txt</button>",
      "  </div>",
      "  <div class=\"shuttle-explorer__bulk-actions\">",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"copy-links\">Copy links</button>",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"show-cli-command\">Show Shuttle CLI command</button>",
      "    <button type=\"button\" class=\"shuttle-explorer__btn shuttle-explorer__btn--small\" data-role=\"copy-command\">Copy command</button>",
      "  </div>",
      "  <details class=\"shuttle-explorer__bulk-guide\">",
      "    <summary>How to use these bulk tools</summary>",
      "    <ul>",
      "      <li><strong>Download manifest (CSV)</strong>: a spreadsheet of the selected sites with hub/network, country, and direct download links for record-keeping or sharing.</li>",
      "      <li><strong>Download links file (TXT)</strong>: one download URL per line for use with terminal tools, download managers, or scripts.</li>",
      "      <li><strong>Download script (sh)</strong>: a ready-to-run shell script that creates an output folder and uses <code>curl -L -O</code> to download each selected archive.</li>",
      "      <li><strong>Download selected_sites.txt</strong>: one <code>site_id</code> per line for FLUXNET Shuttle CLI workflows (used with the command shown below).</li>",
      "      <li><strong>Copy links</strong>: copies the selected URLs to your clipboard for paste into another app.</li>",
      "      <li><strong>Show Shuttle CLI command</strong>: reveals a command template that uses your <code>selected_sites.txt</code> and local snapshot file.</li>",
      "      <li><strong>Copy command</strong>: copies the Shuttle CLI helper command shown in the panel.</li>",
      "    </ul>",
      "  </details>",
      "  <p class=\"shuttle-explorer__bulk-warning\">ICOS downloads may require license acceptance in a browser; use the links or follow ICOS prompts.</p>",
      "  <div class=\"shuttle-explorer__cli-panel shuttle-explorer__hidden\" data-role=\"cli-panel\">",
      "    <p class=\"shuttle-explorer__tiny\">The FLUXNET Shuttle CLI supports <code>download -f SNAPSHOT.csv -s SITE1 SITE2 ...</code> but does not provide a sites-file option. Use the helper command below with <code>selected_sites.txt</code>.</p>",
      "    <pre class=\"shuttle-explorer__cli-pre\" data-role=\"cli-command\"></pre>",
      "  </div>",
      "  <p class=\"shuttle-explorer__tiny shuttle-explorer__bulk-status\" data-role=\"bulk-status\" aria-live=\"polite\"></p>",
      "</section>",
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
      "<aside class=\"shuttle-explorer__attribution\" data-role=\"attribution\">",
      "  <h3>Data use &amp; attribution</h3>",
      "  <p class=\"shuttle-explorer__tiny\">FLUXNET data are shared under <a href=\"https://creativecommons.org/licenses/by/4.0/\" target=\"_blank\" rel=\"noopener noreferrer\">CC-BY 4.0</a>. Data users <em>must</em> follow dataset-specific attribution and citation guidance included with each downloaded archive.</p>",
      "  <textarea readonly data-role=\"attribution-text\"></textarea>",
      "</aside>"
    ].join("");
  }

  function Explorer(root) {
    this.root = root;
    this.jsonUrl = root.getAttribute("data-json-src") || DEFAULT_JSON_URL;
    this.csvUrl = root.getAttribute("data-csv-src") || DEFAULT_CSV_URL;
    this.pageSize = Math.max(1, parseInt(root.getAttribute("data-page-size") || String(DEFAULT_PAGE_SIZE), 10) || DEFAULT_PAGE_SIZE);

    this.state = {
      mode: "loading",
      rows: [],
      filteredRows: [],
      source: "",
      sourceUrl: "",
      warning: "",
      lastUpdatedLabel: "",
      droppedRows: 0,
      errorMessage: "",
      search: "",
      selectedNetwork: "",
      selectedCountry: "",
      selectedVegetation: "",
      selectedHubs: {},
      selectedKeys: {},
      cliPanelVisible: false,
      sortKey: "data_hub",
      sortDir: "asc",
      page: 1
    };

    createLayout(root);
    this.bindings = this.getBindings();
    this.bindEvents();
    this.renderTableHeader();
    this.setAttributionText(buildAttributionText());
    this.render();
  }

  Explorer.prototype.getBindings = function () {
    return {
      status: bySelector(this.root, "[data-role='status']"),
      controls: bySelector(this.root, "[data-role='controls']"),
      search: bySelector(this.root, "[data-role='search']"),
      networkFilter: bySelector(this.root, "[data-role='network-filter']"),
      countryFilter: bySelector(this.root, "[data-role='country-filter']"),
      vegetationFilter: bySelector(this.root, "[data-role='vegetation-filter']"),
      hubFilters: bySelector(this.root, "[data-role='hub-filters']"),
      widgetLastUpdatedInline: bySelector(this.root, "[data-role='widget-last-updated-inline']"),
      summaryRow: bySelector(this.root, "[data-role='summary-row']"),
      summary: bySelector(this.root, "[data-role='summary']"),
      reset: bySelector(this.root, "[data-role='reset']"),
      bulkPanel: bySelector(this.root, "[data-role='bulk-panel']"),
      selectionCount: bySelector(this.root, "[data-role='selection-count']"),
      selectFiltered: bySelector(this.root, "[data-role='select-filtered']"),
      selectAllSites: bySelector(this.root, "[data-role='select-all-sites']"),
      clearSelection: bySelector(this.root, "[data-role='clear-selection']"),
      downloadManifest: bySelector(this.root, "[data-role='download-manifest']"),
      downloadLinks: bySelector(this.root, "[data-role='download-links']"),
      downloadScript: bySelector(this.root, "[data-role='download-script']"),
      downloadSitesFile: bySelector(this.root, "[data-role='download-sites-file']"),
      copyLinks: bySelector(this.root, "[data-role='copy-links']"),
      showCliCommand: bySelector(this.root, "[data-role='show-cli-command']"),
      copyCommand: bySelector(this.root, "[data-role='copy-command']"),
      cliPanel: bySelector(this.root, "[data-role='cli-panel']"),
      cliCommand: bySelector(this.root, "[data-role='cli-command']"),
      bulkStatus: bySelector(this.root, "[data-role='bulk-status']"),
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
        while (target && target !== b.theadRow && !(target.tagName === "BUTTON" && target.hasAttribute("data-sort-key"))) {
          target = target.parentNode;
        }
        if (!target || target === b.theadRow) {
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
    }

    if (b.copyAttribution) {
      b.copyAttribution.addEventListener("click", function () {
        self.copyAttribution();
      });
    }
  };

  Explorer.prototype.setAttributionText = function (text) {
    if (this.bindings.attributionText) {
      this.bindings.attributionText.value = text;
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
      snapshot_last_updated: this.state.lastUpdatedLabel || ""
    });
  };

  Explorer.prototype.renderLastUpdatedLabel = function () {
    var label = this.state.lastUpdatedLabel || "";
    if (this.bindings.widgetLastUpdatedInline) {
      this.bindings.widgetLastUpdatedInline.textContent = label;
    }
    var pageIntroSpan = document.getElementById("shuttle-snapshot-last-updated");
    if (pageIntroSpan) {
      pageIntroSpan.textContent = label;
    }
  };

  Explorer.prototype.copyAttribution = function () {
    var self = this;
    var textarea = this.bindings.attributionText;
    var status = this.bindings.copyStatus;
    if (!textarea) {
      return;
    }

    function setStatus(message) {
      if (status) {
        status.textContent = message;
      }
    }

    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(textarea.value).then(function () {
        setStatus("Copied.");
      }).catch(function () {
        try {
          textarea.focus();
          textarea.select();
          document.execCommand("copy");
          setStatus("Copied.");
        } catch (err) {
          setStatus("Copy failed. Select and copy manually.");
        }
      });
      return;
    }

    try {
      textarea.focus();
      textarea.select();
      document.execCommand("copy");
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
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(value).then(function () {
        self.setBulkStatus(successMessage || "Copied.");
      }).catch(function () {
        self.fallbackCopyText(value, successMessage);
      });
      return;
    }
    this.fallbackCopyText(value, successMessage);
  };

  Explorer.prototype.fallbackCopyText = function (text, successMessage) {
    var ta = document.createElement("textarea");
    ta.value = String(text || "");
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    try {
      document.execCommand("copy");
      this.setBulkStatus(successMessage || "Copied.");
    } catch (err) {
      this.setBulkStatus("Copy failed. Try downloading the file instead.");
    }
    document.body.removeChild(ta);
  };

  Explorer.prototype.getSelectedRowsOrWarn = function () {
    var selectedRows = this.getSelectedRows();
    if (!selectedRows.length) {
      this.setBulkStatus("Select one or more sites first.");
      return null;
    }
    return selectedRows;
  };

  Explorer.prototype.buildSelectionManifestCsv = function (rows) {
    var lines = [
      ["site_id", "data_hub", "network", "country", "download_link"].join(",")
    ];
    rows.forEach(function (row) {
      lines.push([
        csvEscape(row.site_id),
        csvEscape(row.data_hub),
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
    var seen = {};
    var siteIds = [];
    rows.forEach(function (row) {
      var siteId = String(row.site_id || "").trim();
      if (!siteId || seen[siteId]) {
        return;
      }
      seen[siteId] = true;
      siteIds.push(siteId);
    });
    return siteIds.join("\n") + "\n";
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
    var urls = rows.map(function (row) {
      return row.download_link;
    });
    return [
      "#!/usr/bin/env bash",
      "set -euo pipefail",
      "",
      "# NOTE: ICOS links may require interactive license acceptance in a browser before download.",
      "# If an ICOS URL does not download directly, open it in a browser and follow ICOS prompts.",
      "",
      "OUTDIR=\"${1:-fluxnet_downloads}\"",
      "LOGFILE=\"${2:-bulk_download.log}\"",
      "CONCURRENCY=\"${CONCURRENCY:-5}\"",
      "MAX_ATTEMPTS=\"${MAX_ATTEMPTS:-5}\"",
      "URL_FILE=\"${URL_FILE:-}\"",
      "PARALLEL_MODE=\"sequential\"",
      "SCRIPT_VERSION=\"2026-03-04-3\"",
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
      urls.join("\n"),
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
      "download_one() {",
      "  local url=\"$1\"",
      "  local outdir=\"$2\"",
      "  local logfile=\"$3\"",
      "  local attempt=\"$4\"",
      "  local success_file=\"$5\"",
      "  local failed_file=\"$6\"",
      "",
      "  [ -n \"$url\" ] || return 0",
      "",
      "  local clean=\"${url%%\\?*}\"",
      "  local filename=\"$(basename \"$clean\")\"",
      "  if [ -z \"$filename\" ] || [ \"$filename\" = \".\" ] || [ \"$filename\" = \"/\" ]; then",
      "    filename=\"download_$(date +%s)_$RANDOM.bin\"",
      "  fi",
      "",
      "  local final_path=\"$outdir/$filename\"",
      "  local part_path=\"$final_path.part\"",
      "  local tmp_headers=\"$outdir/.headers.$$.tmp\"",
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
      "    return 0",
      "  fi",
      "",
      "  if curl --location --fail -C - \\",
      "      --connect-timeout 20 \\",
      "      --max-time 0 \\",
      "      --speed-time 60 --speed-limit 1024 \\",
      "      --retry 0 \\",
      "      --silent --show-error \\",
      "      --dump-header \"$tmp_headers\" \\",
      "      --output \"$part_path\" \\",
      "      \"$url\"; then",
      "    mv -f \"$part_path\" \"$final_path\"",
      "    ts=\"$(date +'%Y-%m-%dT%H:%M:%S%z')\"",
      "    echo \"$ts ATTEMPT=$attempt URL=$url FILE=$filename STATUS=SUCCESS\" >> \"$logfile\"",
      "    echo \"Attempt $attempt OK $filename\"",
      "    echo \"$url\" >> \"$success_file\"",
      "    rm -f \"$tmp_headers\"",
      "    return 0",
      "  fi",
      "",
      "  local http_code=\"\"",
      "  http_code=\"$(awk 'toupper($1) ~ /^HTTP\\// {code=$2} END {print code}' \"$tmp_headers\" 2>/dev/null || true)\"",
      "  rm -f \"$tmp_headers\"",
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
      "    export -f download_one",
      "    while IFS= read -r url; do",
      "      [ -n \"$url\" ] || continue",
      "      printf '%s\\0' \"$url\"",
      "    done < \"$in_file\" | xargs -0 -P \"$CONCURRENCY\" -I {} bash -lc 'download_one \"$@\"' _ \\",
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
      "      echo \"Pass $attempt progress: $done_now/$pass_total completed\" | tee -a \"$LOGFILE\"",
      "      sleep 15",
      "    done",
      "    wait \"$worker_pid\" || true",
      "  else",
      "    local seq_done=0",
      "    while IFS= read -r url; do",
      "      [ -n \"$url\" ] || continue",
      "      seq_done=$((seq_done + 1))",
      "      echo \"Pass $attempt progress: $seq_done/$pass_total\" | tee -a \"$LOGFILE\"",
      "      download_one \"$url\" \"$OUTDIR\" \"$LOGFILE\" \"$attempt\" \"$SUCCESS_FILE\" \"$failed_out\" || true",
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
    var duplicateSiteIds = this.getDuplicateSelectedSiteIds(rows);
    var lines = [
      "# FLUXNET Shuttle CLI syntax (confirmed from shuttle docs):",
      "# fluxnet-shuttle download -f shuttle_snapshot.csv -s SITE1 SITE2 ...",
      "#",
      "# The CLI does not support a --sites-file option, so this helper expands selected_sites.txt:",
      "fluxnet-shuttle download -f shuttle_snapshot.csv -o fluxnet_downloads -s $(tr '\\n' ' ' < selected_sites.txt)"
    ];
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

  Explorer.prototype.handleDownloadManifest = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    this.downloadTextFile("fluxnet_selected_manifest.csv", this.buildSelectionManifestCsv(rows), "text/csv;charset=utf-8");
    this.setBulkStatus("Downloaded manifest CSV for " + rows.length + " selected sites.");
    gaEvent("fx_manifest_download", { count: rows.length });
  };

  Explorer.prototype.handleDownloadLinks = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    this.downloadTextFile("fluxnet_selected_links.txt", this.buildLinksText(rows), "text/plain;charset=utf-8");
    this.setBulkStatus("Downloaded links TXT for " + rows.length + " selected sites.");
    gaEvent("fx_links_download", { count: rows.length });
  };

  Explorer.prototype.handleDownloadScript = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    this.downloadTextFile("fluxnet_bulk_download.sh", this.buildCurlScript(rows), "text/x-shellscript;charset=utf-8");
    this.setBulkStatus("Downloaded shell script for " + rows.length + " selected sites.");
    gaEvent("fx_script_download", { count: rows.length });
  };

  Explorer.prototype.handleDownloadSitesFile = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    this.downloadTextFile("selected_sites.txt", this.buildSelectedSitesText(rows), "text/plain;charset=utf-8");
    this.setBulkStatus("Downloaded selected_sites.txt.");
  };

  Explorer.prototype.handleCopyLinks = function () {
    var rows = this.getSelectedRowsOrWarn();
    if (!rows) {
      return;
    }
    this.copyText(this.buildLinksText(rows), "Copied selected download links.");
    gaEvent("fx_copy_links", { count: rows.length });
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
    this.copyText(this.buildShuttleCommandText(rows), "Copied Shuttle CLI helper command.");
    gaEvent("fx_copy_command", { count: rows.length });
  };

  Explorer.prototype.renderBulkPanel = function () {
    var b = this.bindings;
    var hasData = this.state.mode === "ready" && this.state.rows.length > 0;
    var selectedRows = this.getSelectedRows();
    var selectedCount = selectedRows.length;
    var disabled = !selectedCount;

    if (!b.bulkPanel) {
      return;
    }

    b.bulkPanel.classList.toggle("shuttle-explorer__hidden", !hasData);
    if (!hasData) {
      return;
    }

    if (b.selectionCount) {
      b.selectionCount.textContent = selectedCount + " selected";
    }

    [
      b.downloadManifest,
      b.downloadLinks,
      b.downloadScript,
      b.downloadSitesFile,
      b.copyLinks,
      b.copyCommand
    ].forEach(function (btn) {
      if (btn) {
        btn.disabled = disabled;
      }
    });

    if (b.showCliCommand) {
      b.showCliCommand.disabled = disabled;
      b.showCliCommand.textContent = this.state.cliPanelVisible ? "Hide Shuttle CLI command" : "Show Shuttle CLI command";
    }

    if (b.cliPanel) {
      b.cliPanel.classList.toggle("shuttle-explorer__hidden", !(this.state.cliPanelVisible && selectedCount));
    }

    if (b.cliCommand && selectedCount) {
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
    downloadTh.textContent = "Download";
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
    var vegetationMap = {};

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
      if (row.vegetation_type) {
        vegetationMap[row.vegetation_type] = true;
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
      Object.keys(vegetationMap).sort().forEach(function (vegetationType) {
        var option = document.createElement("option");
        option.value = vegetationType;
        option.textContent = vegetationType;
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
    var selectedCountry = this.state.selectedCountry;
    var selectedVegetation = this.state.selectedVegetation;
    var selectedHubs = this.state.selectedHubs;

    this.state.filteredRows = this.state.rows.filter(function (row) {
      if (search && row.search_text.indexOf(search) === -1) {
        return false;
      }
      if (selectedNetwork && row.network_tokens.indexOf(selectedNetwork) === -1) {
        return false;
      }
      if (selectedCountry && row.country !== selectedCountry) {
        return false;
      }
      if (selectedVegetation && row.vegetation_type !== selectedVegetation) {
        return false;
      }
      if (Object.keys(selectedHubs).length && !selectedHubs[row.data_hub]) {
        return false;
      }
      return true;
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

  Explorer.prototype.renderRows = function () {
    var tbody = this.bindings.tbody;
    if (!tbody) {
      return;
    }
    var rows = this.state.filteredRows;
    var pageRows = rows;
    var selectedKeys = this.state.selectedKeys || {};

    tbody.innerHTML = "";

    pageRows.forEach(function (row) {
      var tr = document.createElement("tr");
      tr.innerHTML = [
        "<td class=\"shuttle-explorer__select-cell\"><input type=\"checkbox\" data-role=\"row-select\" data-key=\"" + escapeHtml(row._selection_key) + "\"" +
          (selectedKeys[row._selection_key] ? " checked" : "") +
          " aria-label=\"Select " + escapeHtml(row.site_id) + "\" /></td>",
        "<td><strong>" + escapeHtml(row.site_id) + "</strong></td>",
        "<td>" + (row.site_name ? escapeHtml(row.site_name) : "<span class=\"shuttle-explorer__muted\">—</span>") + "</td>",
        "<td>" + (row.country ? escapeHtml(row.country) : "<span class=\"shuttle-explorer__muted\">—</span>") + "</td>",
        "<td>" + escapeHtml(row.data_hub) + "</td>",
        "<td>" + (row.network_display ? escapeHtml(row.network_display) : "<span class=\"shuttle-explorer__muted\">—</span>") + "</td>",
        "<td>" + (row.vegetation_type ? escapeHtml(row.vegetation_type) : "<span class=\"shuttle-explorer__muted\">—</span>") + "</td>",
        "<td>" + escapeHtml(row.years) + "</td>"
      ].join("");

      var downloadTd = document.createElement("td");
      var a = document.createElement("a");
      a.className = "shuttle-explorer__btn shuttle-explorer__btn--small";
      a.href = row.download_link;
      a.target = "_blank";
      a.rel = "noopener noreferrer";
      a.textContent = row.is_icos ? "Accept ICOS license and download" : "Download";
      a.setAttribute("aria-label", a.textContent + " for " + row.site_id);
      downloadTd.appendChild(a);
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
    var parts = [];
    if (filtered === total) {
      parts.push("Showing all " + total + " records.");
    } else {
      parts.push("Showing " + filtered + " of " + total + " records.");
    }
    if (this.state.warning) {
      parts.push(this.state.warning);
    }
    if (this.state.droppedRows) {
      parts.push("Skipped " + this.state.droppedRows + " malformed rows.");
    }
    if (this.bindings.summary) {
      this.bindings.summary.textContent = parts.join(" ");
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
    var hubCounts = formatHubCounts(this.state.rows);
    var source = this.state.source ? "Loaded from " + this.state.source.toUpperCase() : "";
    var msg = "Loaded " + total + " records. " + hubCounts + (source ? " " + source + "." : "");
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

    this.renderLastUpdatedLabel();

    if (b.controls) {
      b.controls.classList.toggle("shuttle-explorer__hidden", !hasData);
    }
    if (b.hubFilters) {
      b.hubFilters.classList.toggle("shuttle-explorer__hidden", !hasData);
    }
    if (b.summaryRow) {
      b.summaryRow.classList.toggle("shuttle-explorer__hidden", !hasData);
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

    var emptyReset = bySelector(this.bindings.empty, "[data-role='empty-reset']");
    if (emptyReset && !emptyReset._boundReset) {
      emptyReset._boundReset = true;
      emptyReset.addEventListener("click", this.resetFilters.bind(this));
    }
  };

  Explorer.prototype.applyLoadedSnapshotState = function (snapshot) {
    this.state.rows = Array.isArray(snapshot && snapshot.rows) ? snapshot.rows : [];
    this.pruneSelection();
    this.state.droppedRows = snapshot && snapshot.droppedRows ? snapshot.droppedRows : 0;
    this.state.source = snapshot && snapshot.source ? snapshot.source : "";
    this.state.sourceUrl = snapshot && snapshot.sourceUrl ? snapshot.sourceUrl : "";
    this.state.warning = snapshot && snapshot.warning ? snapshot.warning : "";
    this.state.lastUpdatedLabel = snapshot && snapshot.lastUpdatedLabel ? snapshot.lastUpdatedLabel : "";
    this.state.errorMessage = "";

    this.populateFilters();
    this.updateDerivedState();
    this.state.mode = "ready";
    this.render();
    this.trackExplorerLoadedOnce();
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
        lastUpdatedLabel: cached.lastUpdatedLabel || ""
      });
    }

    loadSnapshot(this.jsonUrl, this.csvUrl)
      .then(function (result) {
        var freshnessKey = buildSnapshotFreshnessKey(result);
        if (hadCache && cached.freshnessKey && cached.freshnessKey === freshnessKey) {
          if (result.lastModified) {
            self.state.lastUpdatedLabel = formatLastUpdatedLabel(result.lastModified);
            self.render();
          }
          return;
        }

        var normalized = normalizeRows(result.rawRows || []);
        var snapshotState = {
          rows: normalized.rows,
          droppedRows: normalized.dropped,
          source: result.source || "",
          sourceUrl: result.sourceUrl || "",
          warning: result.warning || "",
          lastUpdatedLabel: formatLastUpdatedLabel(result.lastModified || "")
        };
        self.applyLoadedSnapshotState(snapshotState);
        writeSnapshotCache(self.jsonUrl, self.csvUrl, {
          freshnessKey: freshnessKey,
          rows: normalized.rows,
          droppedRows: normalized.dropped,
          source: result.source || "",
          sourceUrl: result.sourceUrl || "",
          warning: result.warning || "",
          lastUpdatedLabel: formatLastUpdatedLabel(result.lastModified || "")
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

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAll);
  } else {
    initAll();
  }
})();
