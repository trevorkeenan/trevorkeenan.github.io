(function () {
	"use strict";

	var DEFAULT_MANIFEST_PATH =
		"https://www.keenangroup.info/keenangroup-data-assets/cedar-gpp/lt_cfe-hybrid_nt/v2/manifest.json";
	var DEFAULT_FPS = 2;
	var MIN_FPS = 0.25;
	var MAX_FPS = 5;
	var COVERAGE_START_MONTH = "2000-01";
	var COVERAGE_END_MONTH = "2020-12";
	var OVERLAY_OPACITY = 0.82;
	var FADE_DURATION_MS = 180;
	var WAIT_AFTER_LOAD_FRAMES = 1;
	var TILE_ERROR_THRESHOLD = 2;
	var DEBUG = false;
	var MAX_LAYER_CACHE = 3;
	var PREFETCH_MAX_TILES = 64;
	var PLAYBACK_PREFETCH_MONTHS = 3;
	var PLAYBACK_READY_MAX_WAIT_MS = 1200;
	var PLAYBACK_BUFFER_RETRY_MS = 300;
	var PREFETCH_MAX_TOTAL_IMAGES = 192;
	var PREFETCH_CACHE_MAX_URLS = 2000;
	var DATA_VIEW_BOUNDS = [[-60, -180], [85, 180]];
	var MAX_BOUNDS_VISCOSITY = 0.35;
	var COLORMAP_STOPS = {
		viridis: ["#440154", "#3b528b", "#21918c", "#5ec962", "#fde725"],
		plasma: ["#0d0887", "#7e03a8", "#cc4778", "#f89540", "#f0f921"],
		magma: ["#000004", "#51127c", "#b73779", "#fc8961", "#fcfdbf"],
		inferno: ["#000004", "#57106e", "#bc3754", "#f98e09", "#fcffa4"],
		cividis: ["#00204c", "#424f7d", "#6c7f7a", "#9ebc66", "#ffe945"]
	};

	function byId(id) {
		return document.getElementById(id);
	}

	function clamp(value, minValue, maxValue) {
		return Math.max(minValue, Math.min(maxValue, value));
	}

	function parseFps(value) {
		var parsed = parseFloat(value);
		if (!isFinite(parsed)) {
			parsed = DEFAULT_FPS;
		}
		return clamp(parsed, MIN_FPS, MAX_FPS);
	}

	function formatLegendValue(value) {
		var absValue = Math.abs(value);
		if (absValue >= 1000 || (absValue > 0 && absValue < 0.01)) {
			return value.toExponential(2);
		}
		if (absValue < 1) {
			return value.toFixed(3);
		}
		return value.toFixed(2);
	}

	function monthInCoverage(month) {
		return (
			typeof month === "string" &&
			/^\d{4}-\d{2}$/.test(month) &&
			month >= COVERAGE_START_MONTH &&
			month <= COVERAGE_END_MONTH
		);
	}

	function setStatus(statusEl, message, stateClass) {
		if (!statusEl) {
			return;
		}
		statusEl.className = stateClass ? "data-status " + stateClass : "data-status";
		statusEl.textContent = message;
	}

	function readManifestPath(root) {
		var params = new URLSearchParams(window.location.search);
		var queryPath = params.get("manifest");
		if (queryPath) {
			return queryPath;
		}
		var inlinePath = root.getAttribute("data-manifest-path");
		return inlinePath || DEFAULT_MANIFEST_PATH;
	}

	function resolveTemplateUrl(template, manifestUrl) {
		if (/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(template)) {
			return template;
		}
		if (template.indexOf("//") === 0) {
			return window.location.protocol + template;
		}
		var resolved = new URL(template, manifestUrl).toString();
		return resolved
			.replace(/%7Bmonth%7D/gi, "{month}")
			.replace(/%7Bz%7D/gi, "{z}")
			.replace(/%7Bx%7D/gi, "{x}")
			.replace(/%7By%7D/gi, "{y}");
	}

	function normalizeTemplateExtension(template, manifest) {
		var ext =
			manifest &&
			manifest.tiles &&
			typeof manifest.tiles.extension === "string"
				? manifest.tiles.extension.replace(/^\./, "")
				: "";
		if (!ext) {
			ext =
				manifest &&
				manifest.tiles &&
				typeof manifest.tiles.format === "string"
					? manifest.tiles.format.replace(/^\./, "")
					: "";
		}
		if (ext === "jpeg") {
			ext = "jpg";
		}
		if (!ext) {
			return template;
		}
		return template.replace(/\{y\}\.[a-zA-Z0-9]+(?=($|[?#]))/, "{y}." + ext);
	}

	function fetchManifestWithFallback(manifestPath) {
		var requestedUrl = new URL(manifestPath, window.location.href).toString();
		var defaultUrl = new URL(DEFAULT_MANIFEST_PATH, window.location.href).toString();

		function fetchManifest(url) {
			return fetch(url, { cache: "no-store" }).then(function (response) {
				if (!response.ok) {
					throw new Error("Manifest request failed with HTTP " + response.status + " for " + url);
				}
				return response.json();
			});
		}

		return fetchManifest(requestedUrl)
			.then(function (manifest) {
				return { manifest: manifest, manifestUrl: requestedUrl, warning: "" };
			})
			.catch(function (error) {
				if (requestedUrl !== defaultUrl) {
					console.warn(
						"Manifest load failed for '" +
							requestedUrl +
							"'. Retrying default manifest URL '" +
							defaultUrl +
							"'.",
						error
					);
				}
				return fetchManifest(defaultUrl).then(function (manifest) {
					if (requestedUrl !== defaultUrl) {
						return {
							manifest: manifest,
							manifestUrl: defaultUrl,
							warning: "Warning: custom manifest unavailable; using default dataset manifest."
						};
					}
					console.warn(
						"Manifest load failed and retried once at default URL '" + defaultUrl + "'."
					);
					return { manifest: manifest, manifestUrl: defaultUrl, warning: "" };
				});
			});
	}

	function getColorStops(colormapName) {
		var key = String(colormapName || "viridis").toLowerCase();
		return COLORMAP_STOPS[key] || COLORMAP_STOPS.viridis;
	}

	function updateLegend(manifest, legendRampEl, legendMinEl, legendMaxEl) {
		var processing = manifest && manifest.processing ? manifest.processing : {};
		var stops = getColorStops(processing.colormap);
		var vmin = Number(processing.vmin);
		var vmax = Number(processing.vmax);

		legendRampEl.style.background = "linear-gradient(to right, " + stops.join(", ") + ")";
		legendMinEl.textContent = isFinite(vmin) ? formatLegendValue(vmin) : "--";
		legendMaxEl.textContent = isFinite(vmax) ? formatLegendValue(vmax) : "--";
	}

	function fillTemplate(template, values) {
		return template
			.replace("{month}", values.month)
			.replace("{z}", String(values.z))
			.replace("{x}", String(values.x))
			.replace("{y}", String(values.y));
	}

	function debugTransition(message, details) {
		if (!DEBUG) {
			return;
		}
		if (details) {
			console.debug("[CEDAR-GPP]", message, details);
		} else {
			console.debug("[CEDAR-GPP]", message);
		}
	}

	function tileCoordKey(coords) {
		if (!coords || !isFinite(coords.z) || !isFinite(coords.x) || !isFinite(coords.y)) {
			return null;
		}
		return String(coords.z) + ":" + String(coords.x) + ":" + String(coords.y);
	}

	function tileKeyFromUrl(url) {
		if (!url) {
			return null;
		}
		var match = String(url).match(/\/(\d+)\/(\d+)\/(\d+)(?:\.[^/?#]+)(?:[?#].*)?$/);
		if (!match) {
			return null;
		}
		return match[1] + ":" + match[2] + ":" + match[3];
	}

	function probeTileVisualContent(month, overlayTemplate, onComplete) {
		var sampleCoords = [
			{ z: 2, x: 1, y: 1 },
			{ z: 3, x: 4, y: 3 },
			{ z: 4, x: 8, y: 8 }
		];
		var loadedCount = 0;
		var errorCount = 0;
		var nonTransparentCount = 0;
		var completed = 0;

		function done() {
			if (completed < sampleCoords.length) {
				return;
			}
			onComplete({
				loadedCount: loadedCount,
				errorCount: errorCount,
				nonTransparentCount: nonTransparentCount
			});
		}

		sampleCoords.forEach(function (coord) {
			var img = new Image();
			img.crossOrigin = "anonymous";
			img.onload = function () {
				loadedCount += 1;
				try {
					var canvas = document.createElement("canvas");
					canvas.width = img.width;
					canvas.height = img.height;
					var ctx = canvas.getContext("2d");
					ctx.drawImage(img, 0, 0);
					var data = ctx.getImageData(0, 0, canvas.width, canvas.height).data;
					for (var i = 3; i < data.length; i += 4) {
						if (data[i] > 0) {
							nonTransparentCount += 1;
							break;
						}
					}
				} catch (err) {
					console.warn("Tile probe canvas read failed (likely CORS):", err);
				}
				completed += 1;
				done();
			};
			img.onerror = function () {
				errorCount += 1;
				completed += 1;
				done();
			};
			img.src = fillTemplate(overlayTemplate, {
				month: month,
				z: coord.z,
				x: coord.x,
				y: coord.y
			});
		});
	}

	function runAfterFrames(frameCount, callback) {
		if (frameCount <= 0) {
			callback();
			return;
		}
		window.requestAnimationFrame(function () {
			runAfterFrames(frameCount - 1, callback);
		});
	}

	document.addEventListener("DOMContentLoaded", function () {
		var root = byId("data-viewer");
		if (!root) {
			return;
		}
		if (window.location.protocol === "file:") {
			console.warn(
				"Viewer opened from file://. If manifest or tiles fail to load, serve the site locally (e.g., python -m http.server)."
			);
		}
		if (!window.L) {
			setStatus(byId("data-status"), "Leaflet failed to load. Refresh the page.", "error");
			return;
		}

		var monthSlider = byId("month-slider");
		var monthLabel = byId("month-label");
		var playPauseBtn = byId("play-pause");
		var stepBackBtn = byId("step-back");
		var stepForwardBtn = byId("step-forward");
		var speedInput = byId("playback-fps");
		var statusEl = byId("data-status");
		var legendRampEl = byId("legend-ramp");
		var legendMinEl = byId("legend-min");
		var legendMaxEl = byId("legend-max");

			var state = {
				months: [],
				currentIndex: -1,
				requestedIndex: -1,
				fps: DEFAULT_FPS,
				playing: false,
				timerId: null,
				playbackBuffering: null,
				visibleLayer: null,
				pendingTransition: null,
				overlayTemplate: "",
				minZoom: 0,
				maxZoom: 5,
				transitionToken: 0,
				prefetchToken: 0,
				prefetchJobs: {},
				prefetchLoaded: {},
				prefetchLoadedOrder: [],
				currentViewKey: "",
				layerCache: {},
				layerCacheOrder: []
			};

		var map = L.map("gpp-map", {
			worldCopyJump: false,
			minZoom: 0,
			maxZoom: 10,
			fadeAnimation: false,
			zoomControl: true,
			maxBounds: DATA_VIEW_BOUNDS,
			maxBoundsViscosity: MAX_BOUNDS_VISCOSITY
		});

		L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
			attribution: "&copy; OpenStreetMap contributors",
			maxZoom: 19,
			noWrap: true,
			bounds: DATA_VIEW_BOUNDS
		}).addTo(map);
		L.control.scale({ imperial: false }).addTo(map);

		function applyMapExtentConstraints(forceFit, onDone) {
			var bounds = L.latLngBounds(DATA_VIEW_BOUNDS);
			var padding = L.point(0, 0);

			function applyCoverView() {
				var containZoom = map.getBoundsZoom(bounds, false, padding);
				var coverZoom = map.getBoundsZoom(bounds, true, padding);
				var chosenZoom = containZoom;
				if (isFinite(coverZoom)) {
					chosenZoom = Math.max(containZoom, coverZoom);
				}
				if (isFinite(chosenZoom)) {
					var initialZoom = Math.max(0, chosenZoom - 1);
					map.setView(bounds.getCenter(), initialZoom, { animate: false });
					return;
				}
				map.fitBounds(bounds, {
					paddingTopLeft: [padding.x, padding.y],
					paddingBottomRight: [padding.x, padding.y],
					animate: false
				});
			}

				map.setMaxBounds(bounds);
				map.options.maxBoundsViscosity = MAX_BOUNDS_VISCOSITY;
				map.setMinZoom(0);
				if (!forceFit) {
					applyCoverView();
					if (typeof onDone === "function") {
						onDone();
					}
					return;
				}

				map.invalidateSize(true);
				window.setTimeout(function () {
					map.invalidateSize(true);
					window.requestAnimationFrame(function () {
						applyCoverView();
						if (typeof onDone === "function") {
							onDone();
						}
					});
				}, 0);
			}

		setTimeout(function () {
			applyMapExtentConstraints(true);
		}, 100);

		function monthUrl(month) {
			return state.overlayTemplate.replace("{month}", month);
		}

		function monthAt(index) {
			if (index < 0 || index >= state.months.length) {
				return null;
			}
			return state.months[index];
		}

		function touchCache(month) {
			var idx = state.layerCacheOrder.indexOf(month);
			if (idx !== -1) {
				state.layerCacheOrder.splice(idx, 1);
			}
			state.layerCacheOrder.push(month);
		}

		function removeLayerFromMap(layer) {
			if (layer && map.hasLayer(layer)) {
				map.removeLayer(layer);
			}
		}

		function buildKeepSet(centerIndex) {
			var keep = {};
			var prevMonth = monthAt(centerIndex - 1);
			var currMonth = monthAt(centerIndex);
			var nextMonth = monthAt(centerIndex + 1);

			if (prevMonth) {
				keep[prevMonth] = true;
			}
			if (currMonth) {
				keep[currMonth] = true;
			}
			if (nextMonth) {
				keep[nextMonth] = true;
			}
			if (state.visibleLayer && state.visibleLayer._gppMonth) {
				keep[state.visibleLayer._gppMonth] = true;
			}
			if (state.pendingTransition && state.pendingTransition.layer && state.pendingTransition.layer._gppMonth) {
				keep[state.pendingTransition.layer._gppMonth] = true;
			}

			return keep;
		}

		function pruneLayerCache(centerIndex) {
			var keep = buildKeepSet(centerIndex);
			var keepCount = Object.keys(keep).length;
			var maxAllowed = Math.max(MAX_LAYER_CACHE, keepCount);
			var i = 0;

			while (i < state.layerCacheOrder.length && state.layerCacheOrder.length > maxAllowed) {
				var month = state.layerCacheOrder[i];
				if (keep[month]) {
					i += 1;
					continue;
				}
				var layer = state.layerCache[month];
				removeLayerFromMap(layer);
				delete state.layerCache[month];
				state.layerCacheOrder.splice(i, 1);
			}
		}

		function monthAtWrapped(index) {
			var count = state.months.length;
			if (!count) {
				return null;
			}
			var wrapped = index % count;
			if (wrapped < 0) {
				wrapped += count;
			}
			return state.months[wrapped];
		}

		function currentViewSignature() {
			var zoom = clamp(Math.round(map.getZoom()), state.minZoom, state.maxZoom);
			var pixelBounds = map.getPixelBounds(map.getCenter(), zoom);
			return [zoom, pixelBounds.min.x, pixelBounds.min.y, pixelBounds.max.x, pixelBounds.max.y].join(":");
		}

		function visibleTileCoords(maxTiles) {
			if (!map._loaded) {
				return [];
			}
			var zoom = clamp(Math.round(map.getZoom()), state.minZoom, state.maxZoom);
			var tileSize = 256;
			var pixelBounds = map.getPixelBounds(map.getCenter(), zoom);
			var minX = Math.floor(pixelBounds.min.x / tileSize);
			var maxX = Math.floor(pixelBounds.max.x / tileSize);
			var minY = Math.floor(pixelBounds.min.y / tileSize);
			var maxY = Math.floor(pixelBounds.max.y / tileSize);
			var limit = isFinite(maxTiles) ? Math.max(1, maxTiles) : null;
			var maxIndex = Math.pow(2, zoom) - 1;

			var centerPoint = map.project(map.getCenter(), zoom);
			var centerX = Math.floor(centerPoint.x / tileSize);
			var centerY = Math.floor(centerPoint.y / tileSize);
			var coords = [];

			for (var y = minY; y <= maxY; y += 1) {
				if (y < 0 || y > maxIndex) {
					continue;
				}
				for (var x = minX; x <= maxX; x += 1) {
					if (x < 0 || x > maxIndex) {
						continue;
					}
					coords.push({ z: zoom, x: x, y: y, d: Math.abs(x - centerX) + Math.abs(y - centerY) });
				}
			}

			coords.sort(function (a, b) {
				return a.d - b.d;
			});
			return limit ? coords.slice(0, limit) : coords;
		}

		function monthVisibleTileUrls(month, maxTiles) {
			var coords = visibleTileCoords(maxTiles);
			var urls = [];
			for (var i = 0; i < coords.length; i += 1) {
				urls.push(
					fillTemplate(state.overlayTemplate, {
						month: month,
						z: coords[i].z,
						x: coords[i].x,
						y: coords[i].y
					})
				);
			}
			return urls;
		}

		function buildRequiredTileSetForView() {
			var coords = visibleTileCoords();
			var requiredKeys = {};
			var requiredList = [];
			for (var i = 0; i < coords.length; i += 1) {
				var key = tileCoordKey(coords[i]);
				if (!key || requiredKeys[key]) {
					continue;
				}
				requiredKeys[key] = true;
				requiredList.push(key);
			}
			return {
				viewKey: currentViewSignature(),
				requiredKeys: requiredKeys,
				requiredList: requiredList
			};
		}

		function resolveTileEventKey(evt) {
			if (!evt) {
				return null;
			}
			var key = tileCoordKey(evt.coords);
			if (key) {
				return key;
			}
			var url = evt.tile ? evt.tile.currentSrc || evt.tile.src : "";
			return tileKeyFromUrl(url);
		}

		function getTransitionCounts(pending) {
			var resolved = 0;
			var loaded = 0;
			var errors = 0;
			var requiredList = pending.requiredList || [];
			for (var i = 0; i < requiredList.length; i += 1) {
				var key = requiredList[i];
				if (pending.loadedKeys[key]) {
					loaded += 1;
					resolved += 1;
					continue;
				}
				if (pending.errorKeys[key]) {
					errors += 1;
					resolved += 1;
				}
			}
			return {
				required: requiredList.length,
				resolved: resolved,
				loaded: loaded,
				errors: errors
			};
		}

		function markTransitionLoaded(pending, key) {
			if (!pending || !key || !pending.requiredKeys[key]) {
				return false;
			}
			pending.loadedKeys[key] = true;
			delete pending.errorKeys[key];
			return true;
		}

		function markTransitionError(pending, key, url) {
			if (!pending) {
				return false;
			}
			if ((!key || !pending.requiredKeys[key]) && url && pending.errorUrls.length < 3) {
				pending.errorUrls.push(url);
			}
			if (!key || !pending.requiredKeys[key]) {
				return false;
			}
			if (!pending.errorKeys[key]) {
				pending.errorKeys[key] = true;
				if (pending.errorUrls.length < 3) {
					pending.errorUrls.push(url || "(unknown)");
				}
			}
			return true;
		}

		function syncTransitionFromLayerTiles(pending) {
			if (!pending || !pending.layer || !pending.layer._tiles) {
				return;
			}
			var tiles = pending.layer._tiles;
			var tileIds = Object.keys(tiles);
			for (var i = 0; i < tileIds.length; i += 1) {
				var record = tiles[tileIds[i]];
				if (!record) {
					continue;
				}
				var key = tileCoordKey(record.coords);
				if (!key || !pending.requiredKeys[key]) {
					continue;
				}
				var el = record.el;
				if (record.loaded || (el && el.complete && el.naturalWidth > 0)) {
					markTransitionLoaded(pending, key);
				}
			}
		}

		function markPrefetchLoaded(url) {
			if (!url || state.prefetchLoaded[url]) {
				return;
			}
			state.prefetchLoaded[url] = true;
			state.prefetchLoadedOrder.push(url);
			while (state.prefetchLoadedOrder.length > PREFETCH_CACHE_MAX_URLS) {
				var stale = state.prefetchLoadedOrder.shift();
				delete state.prefetchLoaded[stale];
			}
		}

		function cancelPrefetch(keepMonths) {
			var keep = keepMonths || {};
			var months = Object.keys(state.prefetchJobs);
			for (var i = 0; i < months.length; i += 1) {
				var month = months[i];
				if (keep[month]) {
					continue;
				}
				var job = state.prefetchJobs[month];
				var images = job && job.images ? job.images : [];
				for (var j = 0; j < images.length; j += 1) {
					images[j].onload = null;
					images[j].onerror = null;
					try {
						images[j].src = "";
					} catch (e) {}
				}
				delete state.prefetchJobs[month];
			}
			if (!keepMonths) {
				state.prefetchToken += 1;
			}
		}

		function cancelPrefetchMonth(month) {
			if (!month || !state.prefetchJobs[month]) {
				return;
			}
			var job = state.prefetchJobs[month];
			var images = job && job.images ? job.images : [];
			for (var i = 0; i < images.length; i += 1) {
				images[i].onload = null;
				images[i].onerror = null;
				try {
					images[i].src = "";
				} catch (e) {}
			}
			delete state.prefetchJobs[month];
		}

		function startPrefetchMonth(month, token, maxTilesPerMonth) {
			if (!month || !state.overlayTemplate) {
				return;
			}
			if (state.visibleLayer && state.visibleLayer._gppMonth === month) {
				return;
			}
			if (state.pendingTransition && state.pendingTransition.month === month) {
				return;
			}

			var viewKey = state.currentViewKey;
			var existing = state.prefetchJobs[month];
			if (existing && existing.token === token && existing.viewKey === viewKey) {
				return;
			}

			var urls = monthVisibleTileUrls(month, maxTilesPerMonth);
			if (!urls.length) {
				return;
			}

			if (existing) {
				cancelPrefetchMonth(month);
			}

			var job = {
				token: token,
				month: month,
				viewKey: viewKey,
				images: [],
				remaining: urls.length
			};
			state.prefetchJobs[month] = job;

			function done() {
				if (!state.prefetchJobs[month] || state.prefetchJobs[month].token !== token) {
					return;
				}
				job.remaining -= 1;
				if (job.remaining <= 0) {
					delete state.prefetchJobs[month];
				}
			}

			for (var i = 0; i < urls.length; i += 1) {
				var url = urls[i];
				if (state.prefetchLoaded[url]) {
					done();
					continue;
				}
				var img = new Image();
				img.decoding = "async";
				img.onload = (function (fallbackUrl) {
					return function (evt) {
						var src = evt && evt.target ? evt.target.currentSrc || evt.target.src : "";
						markPrefetchLoaded(src || fallbackUrl);
						done();
					};
				})(url);
				img.onerror = done;
				img.src = url;
				job.images.push(img);
			}
		}

		function prefetchForwardBuffer(centerIndex, depth) {
			if (!state.months.length || !state.overlayTemplate) {
				return;
			}

			state.currentViewKey = currentViewSignature();
			if (!state.prefetchToken) {
				state.prefetchToken = 1;
			}
			var token = state.prefetchToken;
			var keep = {};
			var perMonthLimit = Math.max(6, Math.floor(PREFETCH_MAX_TOTAL_IMAGES / Math.max(1, depth)));

			for (var step = 1; step <= depth; step += 1) {
				var month = monthAtWrapped(centerIndex + step);
				if (!month || keep[month]) {
					continue;
				}
				keep[month] = true;
				startPrefetchMonth(month, token, perMonthLimit);
			}

			cancelPrefetch(keep);
		}

		function isMonthReadyForCurrentView(month) {
			if (!month || !state.overlayTemplate) {
				return false;
			}
			var urls = monthVisibleTileUrls(month);
			if (!urls.length) {
				return false;
			}
			for (var i = 0; i < urls.length; i += 1) {
				if (!state.prefetchLoaded[urls[i]]) {
					return false;
				}
			}
			return true;
		}

		function prefetchNeighborTiles(centerIndex) {
			var depth = state.playing ? PLAYBACK_PREFETCH_MONTHS : 1;
			prefetchForwardBuffer(centerIndex, depth);
			pruneLayerCache(centerIndex);
		}

		function getOrCreateLayer(month) {
			var layer = state.layerCache[month];
			var url = monthUrl(month);

			if (!layer) {
				layer = L.tileLayer(url, {
					opacity: OVERLAY_OPACITY,
					noWrap: true,
					bounds: [[-90, -180], [90, 180]],
					detectRetina: false,
					keepBuffer: 1,
					updateWhenIdle: true,
					updateWhenZooming: false,
					minNativeZoom: state.minZoom,
					maxNativeZoom: state.maxZoom,
					maxZoom: Math.max(8, state.maxZoom + 2),
					attribution: "CEDAR-GPP (Zenodo 11238533)"
				});
				layer._gppMonth = month;
				state.layerCache[month] = layer;
			} else if (layer._url !== url) {
				layer.setUrl(url);
				layer._gppMonth = month;
			}

			touchCache(month);
			return layer;
		}

		function cancelPendingTransition() {
			if (!state.pendingTransition) {
				return;
			}

			var pending = state.pendingTransition;
			if (pending.fadeFrameId !== null) {
				window.cancelAnimationFrame(pending.fadeFrameId);
			}
			if (pending.watchdogId !== null) {
				window.clearTimeout(pending.watchdogId);
			}
			pending.layer.off("load", pending.onLoad);
			pending.layer.off("tileerror", pending.onTileError);
			pending.layer.off("tileload", pending.onTileLoad);
			pending.layer.setOpacity(OVERLAY_OPACITY);

			if (pending.layer !== state.visibleLayer) {
				removeLayerFromMap(pending.layer);
			}
			debugTransition("Transition canceled", {
				transitionId: pending.token,
				month: pending.month
			});
			state.pendingTransition = null;
		}

		function fadeLayers(fromLayer, toLayer, transitionToken, done) {
			if (!fromLayer || fromLayer === toLayer) {
				toLayer.setOpacity(OVERLAY_OPACITY);
				done(true);
				return;
			}
			if (FADE_DURATION_MS <= 0) {
				fromLayer.setOpacity(OVERLAY_OPACITY);
				toLayer.setOpacity(OVERLAY_OPACITY);
				done(true);
				return;
			}

			var startTime = null;
			fromLayer.setOpacity(OVERLAY_OPACITY);
			toLayer.setOpacity(0);

			function step(timestamp) {
				if (
					transitionToken !== state.transitionToken ||
					!state.pendingTransition ||
					state.pendingTransition.token !== transitionToken
				) {
					done(false);
					return;
				}

				if (startTime === null) {
					startTime = timestamp;
				}

				var progress = (timestamp - startTime) / FADE_DURATION_MS;
				progress = clamp(progress, 0, 1);

				toLayer.setOpacity(OVERLAY_OPACITY * progress);

				if (progress < 1) {
					state.pendingTransition.fadeFrameId = window.requestAnimationFrame(step);
				} else {
					done(true);
				}
			}

			state.pendingTransition.fadeFrameId = window.requestAnimationFrame(step);
		}

		function rejectPendingTransition(pending, counts, warningText) {
			if (!state.pendingTransition || state.pendingTransition.token !== pending.token) {
				return;
			}
			if (pending.watchdogId !== null) {
				window.clearTimeout(pending.watchdogId);
			}
			pending.layer.off("load", pending.onLoad);
			pending.layer.off("tileerror", pending.onTileError);
			pending.layer.off("tileload", pending.onTileLoad);
			pending.layer.setOpacity(0);
			if (pending.layer !== state.visibleLayer) {
				removeLayerFromMap(pending.layer);
			}
			state.pendingTransition = null;

			if (state.currentIndex >= 0) {
				state.requestedIndex = state.currentIndex;
				monthSlider.value = String(state.currentIndex);
				monthLabel.textContent = state.months[state.currentIndex];
			}

			debugTransition("Transition rejected", {
				transitionId: pending.token,
				month: pending.month,
				required: counts.required,
				loaded: counts.loaded,
				errors: counts.errors,
				errorUrls: pending.errorUrls.slice(0, 3)
			});
			setStatus(statusEl, warningText, "error");
		}

		function finalizeTransition(pending, counts) {
			if (!state.pendingTransition || state.pendingTransition.token !== pending.token) {
				return;
			}
			if (pending.watchdogId !== null) {
				window.clearTimeout(pending.watchdogId);
			}
			pending.layer.off("load", pending.onLoad);
			pending.layer.off("tileerror", pending.onTileError);
			pending.layer.off("tileload", pending.onTileLoad);

			var previousLayer = state.visibleLayer;
			var nextLayer = pending.layer;
			nextLayer.bringToFront();
			if (previousLayer && previousLayer !== nextLayer) {
				previousLayer.setOpacity(OVERLAY_OPACITY);
			}

			runAfterFrames(Math.max(1, WAIT_AFTER_LOAD_FRAMES), function () {
				if (!state.pendingTransition || state.pendingTransition.token !== pending.token) {
					return;
				}
				fadeLayers(previousLayer, nextLayer, pending.token, function (completed) {
					if (!completed || pending.token !== state.transitionToken) {
						return;
					}

					nextLayer.setOpacity(OVERLAY_OPACITY);
					state.visibleLayer = nextLayer;
					state.currentIndex = pending.index;
					state.requestedIndex = pending.index;
					state.pendingTransition = null;

					monthSlider.value = String(pending.index);
					monthLabel.textContent = pending.month;
					prefetchNeighborTiles(state.currentIndex);

					window.requestAnimationFrame(function () {
						if (pending.token !== state.transitionToken) {
							return;
						}
						if (previousLayer && previousLayer !== nextLayer) {
							previousLayer.setOpacity(OVERLAY_OPACITY);
							removeLayerFromMap(previousLayer);
						}
					});

					debugTransition("Transition committed", {
						transitionId: pending.token,
						month: pending.month,
						required: counts.required,
						loaded: counts.loaded,
						errors: counts.errors,
						errorUrls: pending.errorUrls.slice(0, 3)
					});
					if (counts.errors > 0) {
						setStatus(statusEl, "Showing " + pending.month + " (" + counts.errors + " tile errors).", "error");
					} else {
						setStatus(statusEl, "Showing " + pending.month + ".", "ok");
					}
				});
			});
		}

		function evaluatePendingTransition(pending, reason) {
			if (!state.pendingTransition || state.pendingTransition.token !== pending.token) {
				return;
			}
			if (pending.finalizeScheduled) {
				return;
			}

			syncTransitionFromLayerTiles(pending);
			var counts = getTransitionCounts(pending);
			var debugKey = [
				reason,
				counts.required,
				counts.loaded,
				counts.errors,
				counts.resolved
			].join(":");
			if (debugKey !== pending.lastDebugKey) {
				pending.lastDebugKey = debugKey;
				debugTransition("Transition check", {
					transitionId: pending.token,
					month: pending.month,
					reason: reason,
					required: counts.required,
					loaded: counts.loaded,
					errors: counts.errors,
					resolved: counts.resolved,
					errorUrls: pending.errorUrls.slice(0, 3)
				});
			}

			if (counts.required <= 0) {
				rejectPendingTransition(
					pending,
					counts,
					"No visible tiles were requested for " + pending.month + ". Keeping current month visible."
				);
				return;
			}

			if (counts.errors > pending.errorThreshold && state.visibleLayer) {
				rejectPendingTransition(
					pending,
					counts,
					"Missing tiles for " + pending.month + "; keeping current month visible."
				);
				return;
			}

			if (counts.resolved < counts.required) {
				return;
			}

			pending.finalizeScheduled = true;
			finalizeTransition(pending, counts);
		}

		function requestMonth(index) {
			if (!state.months.length) {
				return;
			}
			if (!map._loaded) {
				window.requestAnimationFrame(function () {
					requestMonth(index);
				});
				return;
			}

			var targetIndex = clamp(index, 0, state.months.length - 1);
			var targetMonth = state.months[targetIndex];
			state.requestedIndex = targetIndex;
			monthSlider.value = String(targetIndex);
			monthLabel.textContent = targetMonth;

			if (state.currentIndex === targetIndex && state.visibleLayer && !state.pendingTransition) {
				setStatus(statusEl, "Showing " + targetMonth + ".", "ok");
				return;
			}

			cancelPrefetch();
			cancelPendingTransition();
			state.transitionToken += 1;
			var token = state.transitionToken;

			var nextLayer = getOrCreateLayer(targetMonth);
			nextLayer.setOpacity(0);
			if (!map.hasLayer(nextLayer)) {
				nextLayer.addTo(map);
			}
			if (state.visibleLayer && state.visibleLayer !== nextLayer) {
				state.visibleLayer.setOpacity(OVERLAY_OPACITY);
				state.visibleLayer.bringToFront();
			}
			nextLayer.bringToFront();

			var required = buildRequiredTileSetForView();
			var pending = {
				token: token,
				index: targetIndex,
				month: targetMonth,
				layer: nextLayer,
				viewKey: required.viewKey,
				requiredKeys: required.requiredKeys,
				requiredList: required.requiredList,
				loadedKeys: {},
				errorKeys: {},
				errorUrls: [],
				errorThreshold: TILE_ERROR_THRESHOLD,
				finalizeScheduled: false,
				lastDebugKey: "",
				onLoad: null,
				onTileError: null,
				onTileLoad: null,
				fadeFrameId: null,
				watchdogId: null
			};

			pending.onTileLoad = function (evt) {
				if (!state.pendingTransition || state.pendingTransition.token !== token) {
					return;
				}
				var loadedUrl = evt && evt.tile ? evt.tile.currentSrc || evt.tile.src : "";
				markPrefetchLoaded(loadedUrl);
				var key = resolveTileEventKey(evt);
				markTransitionLoaded(pending, key);
				evaluatePendingTransition(pending, "tileload");
			};

			pending.onTileError = function (evt) {
				if (!state.pendingTransition || state.pendingTransition.token !== token) {
					return;
				}
				var failedUrl = evt && evt.tile ? evt.tile.currentSrc || evt.tile.src : "";
				var key = resolveTileEventKey(evt) || tileKeyFromUrl(failedUrl);
				markTransitionError(pending, key, failedUrl);
				evaluatePendingTransition(pending, "tileerror");
			};

			pending.onLoad = function () {
				evaluatePendingTransition(pending, "load");
			};

			state.pendingTransition = pending;
			nextLayer.on("tileload", pending.onTileLoad);
			nextLayer.on("tileerror", pending.onTileError);
			nextLayer.on("load", pending.onLoad);

			pending.watchdogId = window.setTimeout(function () {
				if (!state.pendingTransition || state.pendingTransition.token !== token) {
					return;
				}
				syncTransitionFromLayerTiles(pending);
				var counts = getTransitionCounts(pending);
				debugTransition("Transition wait", {
					transitionId: pending.token,
					month: pending.month,
					required: counts.required,
					loaded: counts.loaded,
					errors: counts.errors,
					resolved: counts.resolved,
					errorUrls: pending.errorUrls.slice(0, 3)
				});
				if (counts.errors > pending.errorThreshold && state.visibleLayer) {
					rejectPendingTransition(
						pending,
						counts,
						"Missing tiles for " + pending.month + "; keeping current month visible."
					);
				}
			}, 5000);

			evaluatePendingTransition(pending, "start");
			window.setTimeout(function () {
				if (state.pendingTransition && state.pendingTransition.token === token) {
					evaluatePendingTransition(pending, "post-start");
				}
			}, 0);
		}

		function activeControlIndex() {
			if (state.requestedIndex >= 0) {
				return state.requestedIndex;
			}
			if (state.currentIndex >= 0) {
				return state.currentIndex;
			}
			return 0;
		}

		function displayedIndex() {
			if (state.currentIndex >= 0) {
				return state.currentIndex;
			}
			return 0;
		}

		function schedulePlaybackTick(delayMs) {
			if (!state.playing) {
				return;
			}
			if (state.timerId !== null) {
				window.clearTimeout(state.timerId);
				state.timerId = null;
			}
			var delay = Math.max(16, isFinite(delayMs) ? Math.round(delayMs) : Math.round(1000 / state.fps));
			state.timerId = window.setTimeout(function () {
				state.timerId = null;
				playbackTick();
			}, delay);
		}

		function stopPlayback() {
			if (state.timerId !== null) {
				window.clearTimeout(state.timerId);
				state.timerId = null;
			}
			state.playing = false;
			state.playbackBuffering = null;
			playPauseBtn.textContent = "Play";
		}

		function playbackTick() {
			if (!state.playing || !state.months.length) {
				return;
			}
			if (state.pendingTransition) {
				schedulePlaybackTick(Math.round(1000 / state.fps));
				return;
			}

			var currentIndex = displayedIndex();
			var nextIndex = currentIndex + 1;
			if (nextIndex >= state.months.length) {
				nextIndex = 0;
			}
			var nextMonth = state.months[nextIndex];
			prefetchForwardBuffer(currentIndex, PLAYBACK_PREFETCH_MONTHS);

			if (isMonthReadyForCurrentView(nextMonth)) {
				state.playbackBuffering = null;
				requestMonth(nextIndex);
				schedulePlaybackTick(Math.round(1000 / state.fps));
				return;
			}

			var now = Date.now();
			if (!state.playbackBuffering || state.playbackBuffering.month !== nextMonth) {
				state.playbackBuffering = { month: nextMonth, startMs: now };
			}

			var elapsed = now - state.playbackBuffering.startMs;
			if (elapsed >= PLAYBACK_READY_MAX_WAIT_MS) {
				state.playbackBuffering.startMs = now;
				schedulePlaybackTick(Math.max(PLAYBACK_BUFFER_RETRY_MS, Math.round(1000 / state.fps)));
			} else {
				schedulePlaybackTick(Math.min(PLAYBACK_BUFFER_RETRY_MS, Math.round(1000 / state.fps)));
			}
		}

		function startPlayback() {
			if (!state.months.length) {
				return;
			}
			stopPlayback();
			state.playing = true;
			playPauseBtn.textContent = "Pause";
			prefetchForwardBuffer(displayedIndex(), PLAYBACK_PREFETCH_MONTHS);
			schedulePlaybackTick(0);
		}

		function togglePlayback() {
			if (state.playing) {
				stopPlayback();
			} else {
				startPlayback();
			}
		}

		function updatePlaybackSpeed() {
			state.fps = parseFps(speedInput.value);
			speedInput.value = String(state.fps);
			if (state.playing) {
				schedulePlaybackTick(Math.round(1000 / state.fps));
			}
		}

		stepBackBtn.addEventListener("click", function () {
			stopPlayback();
			requestMonth(activeControlIndex() - 1);
		});

		stepForwardBtn.addEventListener("click", function () {
			stopPlayback();
			requestMonth(activeControlIndex() + 1);
		});

		playPauseBtn.addEventListener("click", function () {
			togglePlayback();
		});

		monthSlider.addEventListener("input", function () {
			stopPlayback();
			requestMonth(parseInt(monthSlider.value, 10));
		});

		speedInput.addEventListener("input", updatePlaybackSpeed);
		speedInput.addEventListener("change", updatePlaybackSpeed);

		document.addEventListener("keydown", function (event) {
			var activeTag = document.activeElement ? document.activeElement.tagName.toLowerCase() : "";
			if (activeTag === "input" || activeTag === "textarea") {
				return;
			}
			if (event.key === "ArrowLeft") {
				stopPlayback();
				requestMonth(activeControlIndex() - 1);
			} else if (event.key === "ArrowRight") {
				stopPlayback();
				requestMonth(activeControlIndex() + 1);
			} else if (event.key === " ") {
				event.preventDefault();
				togglePlayback();
			}
		});

		map.on("moveend zoomend", function () {
			var nextViewKey = currentViewSignature();
			if (nextViewKey === state.currentViewKey) {
				return;
			}
			state.currentViewKey = nextViewKey;
			state.playbackBuffering = null;
			cancelPrefetch();

			if (state.playing) {
				prefetchForwardBuffer(displayedIndex(), PLAYBACK_PREFETCH_MONTHS);
			} else if (state.currentIndex >= 0) {
				prefetchForwardBuffer(state.currentIndex, 1);
			}

			if (state.pendingTransition) {
				var pendingIndex = state.pendingTransition.index;
				window.setTimeout(function () {
					if (state.pendingTransition && state.pendingTransition.index === pendingIndex) {
						requestMonth(pendingIndex);
					}
				}, 0);
			}
		});

		var manifestPath = readManifestPath(root);
		fetchManifestWithFallback(manifestPath)
			.then(function (manifestResult) {
				var manifest = manifestResult.manifest;
				var monthsRaw =
					manifest &&
					manifest.month_range &&
					Array.isArray(manifest.month_range.months) &&
					manifest.month_range.months.length
						? manifest.month_range.months
						: null;

				if (!monthsRaw) {
					throw new Error("Manifest is missing month_range.months.");
				}
				var months = monthsRaw.filter(monthInCoverage);
				if (!months.length) {
					throw new Error(
						"No months in manifest match coverage window " +
							COVERAGE_START_MONTH +
							" to " +
							COVERAGE_END_MONTH +
							"."
					);
				}

				if (!manifest.tiles || !manifest.tiles.url_template) {
					throw new Error("Manifest is missing tiles.url_template.");
				}

				state.minZoom =
					manifest.tiles.zoom && isFinite(Number(manifest.tiles.zoom.min))
						? Number(manifest.tiles.zoom.min)
						: 0;
				state.maxZoom =
					manifest.tiles.zoom && isFinite(Number(manifest.tiles.zoom.max))
						? Number(manifest.tiles.zoom.max)
						: 5;
				state.overlayTemplate = normalizeTemplateExtension(
					resolveTemplateUrl(manifest.tiles.url_template, manifestResult.manifestUrl),
					manifest
				);
				console.info("Resolved tile template:", state.overlayTemplate);
				state.months = months.slice();
				state.currentIndex = -1;
				state.requestedIndex = -1;

				monthSlider.min = "0";
				monthSlider.max = String(state.months.length - 1);
				monthSlider.step = "1";
				monthSlider.value = "0";
				updateLegend(manifest, legendRampEl, legendMinEl, legendMaxEl);
				if (manifestResult.warning) {
					setStatus(statusEl, manifestResult.warning, "ok");
				}
					updatePlaybackSpeed();
					applyMapExtentConstraints(true, function () {
						requestMonth(0);
						window.setTimeout(function () {
							probeTileVisualContent(months[0], state.overlayTemplate, function (probe) {
								if (probe.loadedCount > 0 && probe.nonTransparentCount === 0) {
									console.warn("Tiles loaded but appear fully transparent.", probe);
									setStatus(
										statusEl,
										"Tiles loaded for " +
											months[0] +
											" but appear fully transparent. Check preprocessing alpha/nodata handling.",
										"error"
									);
								}
							});
						}, 1200);
					});
				})
				.catch(function (error) {
					console.error(error);
					if (state.months.length > 0 && state.overlayTemplate) {
						console.warn(
							"Viewer initialization warning after manifest load; keeping existing data state.",
							error
						);
						return;
					}
					setStatus(
						statusEl,
						"Could not load manifest from '" +
							manifestPath +
							"'. Ensure the URL is public and CORS-enabled, or pass ?manifest=... to a reachable manifest URL." +
						(window.location.protocol === "file:"
							? " If you opened this page via file://, try serving locally with an HTTP server."
							: ""),
					"error"
				);
			});
	});
})();
