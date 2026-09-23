/* FLUXNET author-list page. Combines two generated files:
   sites.js     the frozen site list, map points, rubric and headline totals (static), built by
                FLUXNET_coauthors/scripts/build_public_dashboard.py;
   progress.js  aggregate counts, rewritten by PublicStats.gs in the coordination Sheet whenever
                they change. Neither file holds names or progress for individual sites. */
(function () {
	"use strict";

	var DATA = combineData(window.FLUXNET_PAPER_SITES, window.FLUXNET_PAPER_PROGRESS);
	var LAND_URL = "https://cdn.jsdelivr.net/npm/world-atlas@2.0.2/land-110m.json";
	var COORDINATION_EMAIL = "fluxnet_coordination@berkeley.edu";
	var EXTRA_AUTHORS = "10–20";
	var MAP_LATITUDES = [-58, 84]; // crops Antarctica and the high Arctic, where there are no sites

	var root = document.querySelector(".fxp");
	var number = new Intl.NumberFormat("en-US");
	var networkByCode = {};
	var selectedNetwork = null;
	var mapState = null;
	var landPromise = null;
	var colors = {};

	function el(tag, className, text) {
		var node = document.createElement(tag);
		if (className) node.className = className;
		if (text !== undefined && text !== null) node.textContent = text;
		return node;
	}

	function fmt(value) {
		return value === null || value === undefined ? "–" : number.format(value);
	}

	/* Without progress.js the progress figures read as unknown, never as zero. */
	function combineData(sites, progress) {
		if (!sites) return null;
		var p = progress || {};
		if (progress && progress.site_list !== sites.site_list) {
			console.warn("FLUXNET page: progress.js was built from a different site list than sites.js");
		}
		var unknown = { sites_invited: null, sites_nominated: null, coauthors_named: null,
			coauthors_confirmed: null, confirmed_with_orcid: null };
		var counts = p.networks || {};
		return {
			hasProgress: Boolean(progress),
			snapshot_at: p.updated_at || null,
			campaign_start: p.campaign_start || null,
			timeline: p.timeline || [],
			totals: Object.assign({}, sites.totals, unknown, p.totals),
			networks: sites.networks.map(function (n) {
				return Object.assign({ invited: null, nominated: null }, n, counts[n.code]);
			}),
			rubric: sites.rubric,
			territories: sites.territories,
			countries: sites.countries,
			sites: sites.sites
		};
	}

	function readColors() {
		var style = getComputedStyle(root);
		["ink", "muted", "card", "soft", "hairline", "field", "nominated", "invited", "dot", "faded",
			"land", "highlight", "highlight-edge"].forEach(function (name) {
			colors[name] = style.getPropertyValue("--fxp-" + name).trim();
		});
	}

	/* Tooltip */

	var tooltip = document.getElementById("fxp-tooltip");

	function showTooltip(nodes, clientX, clientY) {
		tooltip.textContent = "";
		nodes.forEach(function (node) { tooltip.appendChild(node); });
		tooltip.hidden = false;
		var box = root.getBoundingClientRect();
		var width = tooltip.offsetWidth;
		var left = clientX - box.left + 14;
		if (left + width > box.width) left = clientX - box.left - width - 14;
		tooltip.style.left = Math.max(0, left) + "px";
		tooltip.style.top = (clientY - box.top + 14) + "px";
	}

	function hideTooltip() {
		tooltip.hidden = true;
	}

	function tipRow(value, label, color) {
		var row = el("div", "fxp-tip-row");
		if (color) {
			var key = el("span", "fxp-key-line");
			key.style.background = color;
			row.appendChild(key);
		}
		row.appendChild(el("b", null, value));
		row.appendChild(document.createTextNode(" " + label));
		return row;
	}

	/* Figures and notes */

	function renderTotals() {
		document.querySelectorAll("[data-total]").forEach(function (node) {
			node.textContent = fmt(DATA.totals[node.getAttribute("data-total")]);
		});
		var t = DATA.totals;
		document.getElementById("fxp-orcid-share").textContent = t.coauthors_confirmed
			? Math.round(100 * t.confirmed_with_orcid / t.coauthors_confirmed) + "%"
			: "–";
		document.getElementById("fxp-nominated-count").textContent =
			fmt(t.sites_nominated) + " of " + fmt(t.sites);

		var track = document.getElementById("fxp-meter-track");
		fillStages(track, t.sites_nominated, t.sites_invited, t.sites);
		var note = document.getElementById("fxp-invited-note");
		if (!DATA.hasProgress) {
			track.setAttribute("aria-label", "Progress counts are unavailable right now.");
			note.textContent = "Progress counts are unavailable right now. Please check back later.";
			return;
		}
		track.setAttribute("aria-label", fmt(t.sites_nominated) + " of " + fmt(t.sites) +
			" sites have nominated their team; " + fmt(t.sites_invited) + " have been invited.");
		note.textContent = t.sites_invited >= t.sites
			? "Every site has been invited."
			: "Invitations have gone to " + fmt(t.sites_invited) + " of " + fmt(t.sites) +
				" sites so far; the rest follow in batches.";
	}

	function fillStages(track, nominated, invited, total) {
		track.textContent = "";
		if (nominated === null || invited === null) return;
		[["fxp-seg-nominated", nominated], ["fxp-seg-invited", Math.max(0, invited - nominated)]]
			.forEach(function (part) {
				if (part[1] <= 0) return;
				var seg = el("span", "fxp-seg " + part[0]);
				seg.style.width = (100 * part[1] / total) + "%";
				track.appendChild(seg);
			});
	}

	function renderNotes() {
		var t = DATA.totals;
		var updated = document.getElementById("fxp-updated");
		if (DATA.snapshot_at) {
			updated.setAttribute("datetime", DATA.snapshot_at);
			updated.textContent = window.d3
				? d3.utcFormat("%-d %B %Y, %H:%M UTC")(new Date(DATA.snapshot_at))
				: DATA.snapshot_at.slice(0, 16).replace("T", " ") + " UTC";
		} else {
			updated.removeAttribute("datetime");
			updated.textContent = "time not available";
		}

		var status = document.getElementById("fxp-invite-status");
		var mail = el("a", null, COORDINATION_EMAIL);
		mail.href = "mailto:" + COORDINATION_EMAIL;
		status.textContent = "";
		if (t.sites_invited === null) {
			status.appendChild(document.createTextNode("These are the " + fmt(t.sites) + " sites whose teams are invited to co-author the paper. If you are a PI of a listed site and have not heard from us, email "));
		} else if (t.sites_invited >= t.sites) {
			status.appendChild(document.createTextNode("Every site's PIs have now been invited. If you are a PI of a site listed below and have not received your invitation, check your spam folder, then email "));
		} else {
			status.appendChild(document.createTextNode("These are the " + fmt(t.sites) + " sites whose teams are invited to co-author the paper. Invitations are going to site PIs in batches, and " + fmt(t.sites_invited) + " sites have been invited so far. Once every site has been invited, a PI of a listed site who has not heard from us should email "));
		}
		status.appendChild(mail);
		status.appendChild(document.createTextNode("."));

		document.getElementById("fxp-about-places").textContent = "The " + fmt(t.places) +
			" author places add up each site's allocation from the table above. The paper will also include " +
			EXTRA_AUTHORS + " authors from the data-processing and writing teams.";

		var territories = DATA.territories || [];
		document.getElementById("fxp-about-countries").textContent = territories.length
			? "The " + fmt(t.countries) + " countries count sites in " + listOf(territories.map(function (x) { return x[1]; })) +
				" with " + listOf(territories.map(function (x) { return withArticle(x[2]); }), true) + "."
			: "Sites are in " + fmt(t.countries) + " countries.";
	}

	function withArticle(country) {
		return /^(United |Netherlands|Czech Republic)/.test(country) ? "the " + country : country;
	}

	function listOf(items, keepRepeats) {
		var list = keepRepeats ? items : items.filter(function (x, i) { return items.indexOf(x) === i; });
		if (list.length < 2) return list.join("");
		return list.slice(0, -1).join(", ") + " and " + list[list.length - 1];
	}

	/* Networks */

	function renderNetworks() {
		var host = document.getElementById("fxp-networks");
		host.textContent = "";
		DATA.networks.forEach(function (n) {
			networkByCode[n.code] = n;
			var button = el("button", "fxp-net");
			button.type = "button";
			button.setAttribute("data-code", n.code);
			button.setAttribute("aria-pressed", "false");
			button.setAttribute("aria-label", n.name + ": " + fmt(n.nominated) + " of " + fmt(n.sites) +
				" sites nominated, " + fmt(n.invited) + " invited. Highlight on the map.");

			var name = el("span", "fxp-net-name");
			name.appendChild(el("strong", null, n.name));
			name.appendChild(el("span", null, fmt(n.sites) + " sites · " + fmt(n.places) + " places"));

			var bar = el("span", "fxp-net-bar");
			fillStages(bar, n.nominated, n.invited, n.sites);

			var value = el("span", "fxp-net-value");
			value.appendChild(el("span", null, fmt(n.nominated) + " of " + fmt(n.sites) + " nominated"));
			value.appendChild(el("span", "fxp-net-sub", n.invited === null ? "–" :
				n.invited >= n.sites ? "all invited" : n.invited ? fmt(n.invited) + " invited" : "not yet invited"));

			button.appendChild(name);
			button.appendChild(bar);
			button.appendChild(value);
			button.addEventListener("click", function () {
				selectNetwork(selectedNetwork === n.code ? null : n.code);
			});
			host.appendChild(button);
		});

		document.getElementById("fxp-network-table").appendChild(dataTable(
			["Network", "Sites", "Author places", "Invited", "Nominated"],
			DATA.networks.map(function (n) { return [n.name, n.sites, n.places, n.invited, n.nominated]; })
		));
	}

	function dataTable(headers, rows) {
		var table = el("table", "fxp-data-table");
		var head = table.createTHead().insertRow();
		headers.forEach(function (h, i) {
			var th = el("th", i ? "fxp-num" : null, h);
			th.scope = "col";
			head.appendChild(th);
		});
		var body = table.createTBody();
		rows.forEach(function (row) {
			var tr = body.insertRow();
			row.forEach(function (cell, i) {
				tr.appendChild(el("td", i ? "fxp-num" : null, typeof cell === "number" || cell === null ? fmt(cell) : cell));
			});
		});
		return table;
	}

	function selectNetwork(code) {
		selectedNetwork = code;
		document.querySelectorAll(".fxp-net").forEach(function (button) {
			button.setAttribute("aria-pressed", String(button.getAttribute("data-code") === code));
		});
		paintMap();
	}

	/* Map */

	function loadLand() {
		if (!landPromise) {
			landPromise = !window.topojson ? Promise.resolve(null) : fetch(LAND_URL)
				.then(function (response) {
					if (!response.ok) throw new Error("HTTP " + response.status);
					return response.json();
				})
				.then(function (topology) { return topojson.feature(topology, topology.objects.land); })
				.catch(function (error) {
					console.warn("FLUXNET map: land outline unavailable (" + error.message + ")");
					return null;
				});
		}
		return landPromise;
	}

	function renderMap() {
		var host = document.getElementById("fxp-map");
		host.textContent = "";
		var width = Math.max(280, Math.floor(host.clientWidth));
		var band = { type: "MultiPoint", coordinates: [[-180, 0], [180, 0], [0, MAP_LATITUDES[0]], [0, MAP_LATITUDES[1]]] };
		var projection = d3.geoEqualEarth().fitWidth(width, band);
		var path = d3.geoPath(projection);
		var height = Math.ceil(path.bounds(band)[1][1]);
		projection.clipExtent([[0, 0], [width, height]]);

		var svg = d3.select(host).append("svg")
			.attr("viewBox", "0 0 " + width + " " + height)
			.attr("aria-hidden", "true");
		svg.append("path").attr("d", path({ type: "Sphere" })).attr("fill", colors.soft);
		var landLayer = svg.append("g");
		var dotLayer = svg.append("g");
		loadLand().then(function (land) {
			if (land) landLayer.append("path").attr("d", path(land)).attr("fill", colors.land);
		});

		var points = [];
		DATA.sites.forEach(function (site) {
			var p = projection([site[5], site[4]]);
			if (p && isFinite(p[0]) && isFinite(p[1])) points.push({ site: site, x: p[0], y: p[1] });
		});
		var radius = width < 560 ? 2.4 : 3.1;
		var dots = dotLayer.selectAll("circle").data(points).join("circle")
			.attr("cx", function (d) { return d.x; })
			.attr("cy", function (d) { return d.y; })
			.attr("r", radius);
		var ring = svg.append("circle")
			.attr("r", radius + 3)
			.attr("fill", "none")
			.attr("stroke", colors.ink)
			.attr("stroke-width", 1.5)
			.attr("pointer-events", "none")
			.attr("visibility", "hidden");

		var delaunay = d3.Delaunay.from(points, function (d) { return d.x; }, function (d) { return d.y; });
		// pointerdown as well, so a tap on a phone shows the site too
		svg.on("pointermove pointerdown", function (event) {
			var m = d3.pointer(event);
			var p = points[delaunay.find(m[0], m[1])];
			if (!p || Math.hypot(p.x - m[0], p.y - m[1]) > 14) {
				ring.attr("visibility", "hidden");
				hideTooltip();
				return;
			}
			ring.attr("cx", p.x).attr("cy", p.y).attr("visibility", "visible");
			showTooltip(siteTip(p.site), event.clientX, event.clientY);
		}).on("pointerleave", function () {
			ring.attr("visibility", "hidden");
			hideTooltip();
		});

		mapState = { dots: dots, ring: ring };
		paintMap();
	}

	function paintMap() {
		var code = selectedNetwork;
		if (mapState) {
			mapState.dots
				.attr("fill", function (d) { return !code ? colors.dot : d.site[2] === code ? colors.highlight : colors.faded; })
				.attr("stroke", function (d) { return code && d.site[2] === code ? colors["highlight-edge"] : colors.card; })
				.attr("stroke-width", function (d) { return code && d.site[2] === code ? 1 : 0.75; });
			if (code) mapState.dots.filter(function (d) { return d.site[2] === code; }).raise();
			mapState.ring.raise();
		}
		var status = document.getElementById("fxp-map-status");
		status.textContent = "";
		if (code) {
			var n = networkByCode[code];
			status.appendChild(el("span", null, "Highlighting " + n.name + ": " + fmt(n.sites) + " sites."));
			var clear = el("button", "fxp-link-btn", "Show all networks");
			clear.type = "button";
			clear.addEventListener("click", function () { selectNetwork(null); });
			status.appendChild(clear);
		} else {
			status.appendChild(el("span", null, "Showing all " + fmt(DATA.totals.sites) +
				" sites. Point at or tap a site to see its name."));
		}
	}

	function siteTip(site) {
		var nodes = [el("strong", null, site[0]), el("div", null, site[1])];
		var network = networkByCode[site[2]];
		nodes.push(el("div", null, (network ? network.name : site[2]) + " · " + (DATA.countries[site[3]] || site[3])));
		if (site[6] && site[7]) nodes.push(el("div", null, "Data " + yearRange(site[6], site[7])));
		return nodes;
	}

	function yearRange(first, last) {
		return first === last ? String(first) : first + "–" + last;
	}

	/* Timeline */

	function cumulativeSeries() {
		var rows = DATA.timeline || [];
		if (!rows.length) return [];
		var named = 0;
		var confirmed = 0;
		var series = [{ t: new Date(DATA.campaign_start || rows[0][0]), named: 0, confirmed: 0 }];
		rows.forEach(function (row) {
			named += row[1];
			confirmed += row[2];
			series.push({ t: new Date(row[0]), named: named, confirmed: confirmed });
		});
		var end = new Date(DATA.snapshot_at);
		if (end > series[series.length - 1].t) series.push({ t: end, named: named, confirmed: confirmed });
		return series;
	}

	function renderTimeline() {
		var host = document.getElementById("fxp-timeline-chart");
		host.textContent = "";
		var series = cumulativeSeries();
		if (series.length < 2) {
			host.appendChild(el("p", "fxp-empty", DATA.hasProgress
				? "No co-authors have been named yet."
				: "Progress counts are unavailable right now."));
			return;
		}
		var width = Math.max(280, Math.floor(host.clientWidth));
		var height = width < 480 ? 200 : 230;
		var margin = { top: 12, right: 104, bottom: 28, left: 36 };
		var x = d3.scaleUtc()
			.domain([series[0].t, series[series.length - 1].t])
			.range([margin.left, width - margin.right]);
		var y = d3.scaleLinear()
			.domain([0, d3.max(series, function (d) { return d.named; }) || 1])
			.nice(4)
			.range([height - margin.bottom, margin.top]);

		var svg = d3.select(host).append("svg")
			.attr("viewBox", "0 0 " + width + " " + height)
			.attr("aria-hidden", "true");

		var yTicks = y.ticks(4).filter(Number.isInteger);
		svg.append("g").selectAll("line").data(yTicks.filter(Boolean)).join("line")
			.attr("x1", margin.left).attr("x2", width - margin.right)
			.attr("y1", y).attr("y2", y)
			.attr("stroke", colors.hairline)
			.attr("shape-rendering", "crispEdges");
		svg.append("line")
			.attr("x1", margin.left).attr("x2", width - margin.right)
			.attr("y1", y(0)).attr("y2", y(0))
			.attr("stroke", colors.field)
			.attr("shape-rendering", "crispEdges");
		svg.append("g").selectAll("text").data(yTicks).join("text")
			.attr("x", margin.left - 8).attr("y", y).attr("dy", "0.32em")
			.attr("text-anchor", "end")
			.text(fmt);

		var span = x.domain()[1] - x.domain()[0];
		var tickFormat = d3.utcFormat(span < 4 * 864e5 ? "%-d %b %H:%M" : "%-d %b");
		var xTicks = x.ticks(Math.max(2, Math.floor((width - margin.left - margin.right) / 120)));
		svg.append("g").selectAll("text").data(xTicks).join("text")
			.attr("x", x).attr("y", height - 8)
			.attr("text-anchor", "middle")
			.text(tickFormat);

		var area = d3.area().curve(d3.curveStepAfter)
			.x(function (d) { return x(d.t); })
			.y0(y(0))
			.y1(function (d) { return y(d.confirmed); });
		svg.append("path").attr("d", area(series)).attr("fill", colors.nominated).attr("fill-opacity", 0.1);

		var lines = [
			{ key: "named", label: "named", color: colors.invited },
			{ key: "confirmed", label: "confirmed", color: colors.nominated }
		];
		lines.forEach(function (s) {
			var line = d3.line().curve(d3.curveStepAfter)
				.x(function (d) { return x(d.t); })
				.y(function (d) { return y(d[s.key]); });
			svg.append("path").attr("d", line(series))
				.attr("fill", "none")
				.attr("stroke", s.color)
				.attr("stroke-width", 2)
				.attr("stroke-linejoin", "round")
				.attr("stroke-linecap", "round");
		});

		var last = series[series.length - 1];
		var labelsFit = Math.abs(y(last.named) - y(last.confirmed)) >= 14;
		lines.forEach(function (s) {
			svg.append("circle")
				.attr("cx", x(last.t)).attr("cy", y(last[s.key])).attr("r", 4)
				.attr("fill", s.color).attr("stroke", colors.card).attr("stroke-width", 2);
			if (labelsFit) {
				svg.append("text").attr("class", "fxp-end-label")
					.attr("x", x(last.t) + 9).attr("y", y(last[s.key])).attr("dy", "0.32em")
					.text(fmt(last[s.key]) + " " + s.label);
			}
		});

		var crosshair = svg.append("line")
			.attr("y1", margin.top).attr("y2", height - margin.bottom)
			.attr("stroke", colors.muted).attr("stroke-width", 1)
			.attr("pointer-events", "none").attr("visibility", "hidden");
		var bisect = d3.bisector(function (d) { return d.t; }).right;
		var timeLabel = d3.utcFormat("%-d %b %Y, %H:%M UTC");
		svg.append("rect")
			.attr("x", margin.left).attr("y", margin.top)
			.attr("width", width - margin.left - margin.right)
			.attr("height", height - margin.top - margin.bottom)
			.attr("fill", "transparent")
			.on("pointermove", function (event) {
				var t = x.invert(d3.pointer(event)[0]);
				var d = series[Math.max(0, bisect(series, t) - 1)];
				crosshair.attr("x1", x(t)).attr("x2", x(t)).attr("visibility", "visible");
				showTooltip([
					el("strong", null, timeLabel(t)),
					tipRow(fmt(d.named), "named by site teams", colors.invited),
					tipRow(fmt(d.confirmed), "confirmed their details", colors.nominated)
				], event.clientX, event.clientY);
			})
			.on("pointerleave", function () {
				crosshair.attr("visibility", "hidden");
				hideTooltip();
			});

		renderTimelineTable(series);
	}

	function renderTimelineTable(series) {
		var byDay = new Map();
		series.forEach(function (d) { byDay.set(d.t.toISOString().slice(0, 10), d); });
		var host = document.getElementById("fxp-timeline-table");
		host.textContent = "";
		host.appendChild(dataTable(
			["Date (UTC, end of day)", "Named by site teams", "Confirmed their details"],
			Array.from(byDay, function (entry) { return [entry[0], entry[1].named, entry[1].confirmed]; })
		));
	}

	/* Rubric */

	function renderRubric() {
		var r = DATA.rubric;
		var table = document.getElementById("fxp-rubric");
		table.textContent = "";
		table.appendChild(el("caption", "fxp-vh", "Author places per site, by years of data submitted and most recent year of data"));
		var head = table.createTHead();
		var top = head.insertRow();
		var corner = el("th", null, "Years of data");
		corner.rowSpan = 2;
		corner.scope = "col";
		top.appendChild(corner);
		var group = el("th", null, "Most recent year of data");
		group.colSpan = r.cols.length;
		group.scope = "colgroup";
		top.appendChild(group);
		var cols = head.insertRow();
		r.cols.forEach(function (label) {
			var th = el("th", null, label);
			th.scope = "col";
			cols.appendChild(th);
		});
		var body = table.createTBody();
		r.rows.forEach(function (label, i) {
			var tr = body.insertRow();
			var th = el("th", null, label);
			th.scope = "row";
			tr.appendChild(th);
			r.cols.forEach(function (_, j) {
				var td = el("td", "fxp-p" + r.places[i][j]);
				td.appendChild(el("span", "fxp-places", String(r.places[i][j])));
				td.appendChild(el("span", "fxp-cell-sites", fmt(r.sites[i][j]) + (r.sites[i][j] === 1 ? " site" : " sites")));
				tr.appendChild(td);
			});
		});
	}

	/* Site finder */

	function fold(value) {
		return String(value).normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase();
	}

	function renderSites() {
		var body = document.querySelector("#fxp-sites tbody");
		body.textContent = "";
		var fragment = document.createDocumentFragment();
		var rows = DATA.sites.map(function (site) {
			var network = networkByCode[site[2]] ? networkByCode[site[2]].name : site[2];
			var country = DATA.countries[site[3]] || site[3];
			var tr = document.createElement("tr");
			[site[0], site[1], network, country, site[6] && site[7] ? yearRange(site[6], site[7]) : ""]
				.forEach(function (value, i) { tr.appendChild(el("td", i === 4 ? "fxp-num" : null, value)); });
			tr.setAttribute("data-search", fold([site[0], site[1], network, country].join(" ")));
			fragment.appendChild(tr);
			return tr;
		});
		var none = document.createElement("tr");
		var noneCell = el("td", null, "No sites match that search. Sites join the paper by sharing data through a regional network; see below.");
		noneCell.colSpan = 5;
		none.appendChild(noneCell);
		fragment.appendChild(none);
		body.appendChild(fragment);

		var input = document.getElementById("fxp-search");
		var count = document.getElementById("fxp-search-count");
		function update() {
			var query = fold(input.value.trim());
			var shown = 0;
			rows.forEach(function (tr) {
				var hit = !query || tr.getAttribute("data-search").indexOf(query) !== -1;
				tr.hidden = !hit;
				if (hit) shown += 1;
			});
			none.hidden = shown > 0;
			count.textContent = "Showing " + fmt(shown) + " of " + fmt(rows.length) + " sites";
		}
		input.addEventListener("input", update);
		update();
	}

	/* Boot */

	function renderCharts() {
		if (!window.d3) {
			["fxp-timeline-chart", "fxp-map"].forEach(function (id) {
				var host = document.getElementById(id);
				host.textContent = "";
				host.appendChild(el("p", "fxp-empty", "This chart could not load. The same numbers are in the tables on this page."));
			});
			return;
		}
		renderTimeline();
		renderMap();
	}

	function init() {
		if (!root) return;
		if (!DATA) {
			root.insertBefore(el("p", "fxp-card fxp-empty", "The page data could not be loaded. Please try again later."), root.children[1]);
			return;
		}
		readColors();
		renderTotals();
		renderNetworks();
		renderRubric();
		renderSites();
		renderNotes();
		renderCharts();

		var lastWidth = root.clientWidth;
		var timer = null;
		window.addEventListener("resize", function () {
			clearTimeout(timer);
			timer = setTimeout(function () {
				if (root.clientWidth === lastWidth) return;
				lastWidth = root.clientWidth;
				hideTooltip();
				renderCharts();
			}, 150);
		});
	}

	init();
}());
