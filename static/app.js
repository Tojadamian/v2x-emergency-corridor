const byId = (id) => document.getElementById(id);
const vehicleIds = ["veh-001", "veh-002", "veh-003", "veh-004", "veh-005"];
const defaults = ["sector-A12-tunnel", "sector-A12-tunnel", "sector-A12-tunnel", "sector-B04-bridge", "sector-B04-bridge"];
const sectorGroups = {
  "sector-A12-tunnel": ["veh-001", "veh-002", "veh-003", "veh-004", "veh-005"],
  "sector-B04-bridge": ["veh-001", "veh-002", "veh-003", "veh-004", "veh-005"],
};
let latestRsuNodes = {};
const sectorActionMenuOpen = {};

function setTheme(theme) {
  document.documentElement.dataset.theme = theme;
  const button = byId("theme-toggle");
  button.textContent = theme === "light" ? "Night Mode" : "Day Mode";
  button.setAttribute("aria-label", `Switch to ${theme === "light" ? "night" : "day"} mode`);
}

function initTheme() {
  let theme = "dark";
  try {
    if (localStorage.getItem("theme") === "light") theme = "light";
  } catch (error) {
    console.warn("Could not read the saved theme:", error);
  }
  setTheme(theme);
  byId("theme-toggle").addEventListener("click", () => {
    const nextTheme = document.documentElement.dataset.theme === "light" ? "dark" : "light";
    setTheme(nextTheme);
    try {
      localStorage.setItem("theme", nextTheme);
    } catch (error) {
      console.warn("Could not save the selected theme:", error);
    }
  });
}

function addLine(parent, tag, text) {
  const element = document.createElement(tag);
  element.textContent = text;
  parent.appendChild(element);
  return element;
}

function renderVehicles(vehicles) {
  const container = byId("vehicles");
  container.replaceChildren();
  vehicleIds.forEach((id, index) => {
    const state = vehicles[id];
    const card = document.createElement("article");
    card.className = "vehicle";
    addLine(card, "strong", `${id}${id === "veh-005" ? " · Ambulance" : ""}`);
    addLine(card, "p", state ? state.location : `${defaults[index]} · Awaiting data`);
    if (state) {
      addLine(card, "p", `Speed: ${state.payload.speed_kmh ?? "—"} km/h`);
      addLine(card, "p", `Lane: ${state.payload.lane ?? "—"} · ${state.event_type.replaceAll("_", " ")}`);
    }
    container.appendChild(card);
  });
}

function renderDigitalTwin(vehicles, activeCorridors, sectorVehicles = {}) {
  const container = byId("twin-container");
  container.replaceChildren();
  const defaultLanes = { "veh-001": 1, "veh-002": 2, "veh-003": 3, "veh-004": 3, "veh-005": 2 };
  const laneNames = { 1: "Left", 2: "Center", 3: "Right" };
  const actionDefinitions = [
    {
      label: "Trigger Emergency Corridor",
      description: "Dispatches the ambulance; passenger vehicles yield to open the center lane.",
      title: "Dispatches the ambulance on Center Lane 2 (85-100 km/h); passenger vehicles in this sector decelerate to 0-5 km/h and animate pulling over to Lanes 1 & 3.",
      action: "scenario-emergency_corridor",
    },
    {
      label: "Trigger Sudden Braking",
      description: "Hard braking highlights a vehicle and triggers a collision risk alert.",
      title: "Causes a vehicle in this sector to brake hard (>= 6.5 m/s²), highlights it in yellow on its lane, and triggers a collision risk alert.",
      action: "scenario-sudden_braking",
    },
    {
      label: "Trigger Road Hazard",
      description: "Reports debris and highlights the affected vehicle.",
      title: "Reports road debris in this sector, highlights the vehicle, and triggers a road hazard warning.",
      action: "scenario-road_hazard",
    },
    {
      label: "Restore Normal Traffic",
      description: "Restores ordinary cruising speeds and clears this sector's corridor.",
      title: "Randomizes normal cruising speeds (55-85 km/h) and lanes (1-3) for vehicles in this sector and clears any active emergency corridor.",
      action: "scenario-normal_traffic",
    },
    {
      label: "Inject Duplicate Burst",
      description: "Tests duplicate filtering with three identical packets.",
      title: "Sends 3 identical packets with the same event_id to this sector's RSU to test duplicate filtering.",
      action: "scenario-duplicate_burst",
    },
  ];

  Object.entries(sectorGroups).forEach(([sector, vehicleList]) => {
    const sectorDiv = document.createElement("div");
    sectorDiv.className = "sector";

    const isEmergencyActive = activeCorridors.includes(sector);
    const rsuId = sector === "sector-A12-tunnel" ? "rsu-1" : "rsu-2";
    const header = document.createElement("div");
    header.className = "sector-header";

    const title = document.createElement("div");
    title.className = "sector-title";
    title.textContent = sector === "sector-A12-tunnel" ? "Sector A12 (Tunnel) · RSU 1" : "Sector B04 (Bridge) · RSU 2";
    header.appendChild(title);

    const headerActions = document.createElement("div");
    headerActions.className = "sector-header-actions";
    const disconnected = Boolean(latestRsuNodes[rsuId]?.cloud_disconnected);
    const rsuButton = document.createElement("button");
    rsuButton.type = "button";
    rsuButton.className = `sector-rsu-toggle${disconnected ? " active" : ""}`;
    rsuButton.textContent = `${rsuId.toUpperCase()} ${disconnected ? "Reconnect Cloud" : "Cloud Disconnect"}`;
    rsuButton.title = `Toggle the cloud connection for ${rsuId.toUpperCase()} serving this sector. Disconnecting buffers eligible events at the RSU.`;
    rsuButton.onclick = () => handleControl(`toggle-${rsuId}`);
    headerActions.appendChild(rsuButton);

    const actionsMenu = document.createElement("details");
    actionsMenu.className = "sector-actions-menu";
    actionsMenu.open = Boolean(sectorActionMenuOpen[sector]);
    actionsMenu.addEventListener("toggle", () => {
      sectorActionMenuOpen[sector] = actionsMenu.open;
    });
    const menuSummary = addLine(actionsMenu, "summary", "Sector Actions");
    menuSummary.title = `Open controls for ${title.textContent}.`;
    const menuContent = document.createElement("div");
    menuContent.className = "sector-actions-list";
    actionDefinitions.forEach(({ label, description, title: tooltip, action }) => {
      const actionButton = document.createElement("button");
      actionButton.type = "button";
      actionButton.className = "sector-action";
      actionButton.title = tooltip;
      addLine(actionButton, "span", label);
      addLine(actionButton, "small", description);
      actionButton.addEventListener("click", () => handleControl(action, sector));
      menuContent.appendChild(actionButton);
    });
    actionsMenu.appendChild(menuContent);
    headerActions.appendChild(actionsMenu);
    header.appendChild(headerActions);
    sectorDiv.appendChild(header);

    const lanesDiv = document.createElement("div");
    lanesDiv.className = "lanes";

    for (let lane = 1; lane <= 3; lane++) {
      if (isEmergencyActive && lane === 2) {
        const corridorBanner = document.createElement("div");
        corridorBanner.className = "emergency-corridor-banner";
        corridorBanner.textContent = "EMERGENCY CORRIDOR OPEN — LANE 1 PULL LEFT | LANES 2 & 3 PULL RIGHT";
        lanesDiv.appendChild(corridorBanner);
      }

      const laneDiv = document.createElement("div");
      laneDiv.className = "lane";
      if (isEmergencyActive && lane === 2) {
        laneDiv.className += " corridor-highlight";
      }

      const label = document.createElement("div");
      label.className = "lane-label";
      label.textContent = `Lane ${lane} (${laneNames[lane]})`;
      laneDiv.appendChild(label);

      const vehiclesDiv = document.createElement("div");
      vehiclesDiv.className = "vehicles-in-lane";

      vehicleList.forEach((vId) => {
        const state = sectorVehicles?.[sector]?.[vId] ?? vehicles[vId];
        if (vId === "veh-005" && !isEmergencyActive) return;
        const originalLane = state?.payload?.lane ?? defaultLanes[vId];
        const vehicleLane = isEmergencyActive
          ? vId === "veh-005" ? 2 : originalLane === 2 ? 3 : originalLane
          : originalLane;
        if (vehicleLane !== lane) return;

        const badge = document.createElement("div");
        badge.className = `vehicle-badge${state ? "" : " standby"}`;
        if (vId === "veh-005") badge.className += " ambulance";
        if (state?.event_type === "sudden_braking") badge.className += " braking";
        if (state?.event_type === "road_hazard") badge.className += " hazard";
        if (isEmergencyActive && lane === 1 && vId !== "veh-005") badge.className += " pull-left";
        if (isEmergencyActive && lane === 3 && vId !== "veh-005") {
          badge.className += " pull-right";
        }
        if (isEmergencyActive && vId === "veh-005") badge.className += " ambulance-overtake";

        const speed = state?.payload?.speed_kmh ?? "—";
        let badgeText;
        if (isEmergencyActive && vId === "veh-005") {
          badgeText = `${vId} · AMBULANCE · ${speed} km/h · PASSING CORRIDOR`;
        } else if (isEmergencyActive) {
          const direction = lane === 1 ? "PULL LEFT" : "PULL RIGHT";
          badgeText = `${vId} · YIELDED (STOPPED) · ${speed} km/h · ${direction}`;
        } else if (state?.event_type === "sudden_braking") {
          badgeText = `${vId} · HARD BRAKING · ${speed} km/h`;
        } else if (state?.event_type === "road_hazard") {
          badgeText = `${vId} · ROAD HAZARD · ${speed} km/h`;
        } else {
          badgeText = `${vId} · VEHICLE · ${speed} km/h`;
        }
        badge.textContent = badgeText;
        vehiclesDiv.appendChild(badge);
      });

      laneDiv.appendChild(vehiclesDiv);
      lanesDiv.appendChild(laneDiv);
    }

    sectorDiv.appendChild(lanesDiv);
    if (!isEmergencyActive) {
      const standby = document.createElement("div");
      standby.className = "ambulance-standby";
      standby.textContent = "veh-005 · Ambulance Standby";
      sectorDiv.appendChild(standby);
    }
    container.appendChild(sectorDiv);
  });
}

function renderRSUStatus(rsuNodes) {
  const container = byId("rsu-cards");
  container.replaceChildren();

  Object.entries(rsuNodes || {}).forEach(([rsuId, status]) => {
    const card = document.createElement("div");
    card.className = `rsu-card ${status.cloud_connected ? "online" : "offline"}`;

    const header = document.createElement("div");
    header.className = "rsu-header";
    const title = document.createElement("span");
    title.textContent = rsuId === "rsu-1" ? "RSU-1 · Tunnel A12" : "RSU-2 · Bridge B04";
    const statusBadge = document.createElement("span");
    statusBadge.className = `rsu-status ${status.cloud_connected ? "online" : "offline"}`;
    statusBadge.textContent = status.cloud_connected ? "ONLINE" : "OFFLINE (BUFFERING)";
    header.appendChild(title);
    header.appendChild(statusBadge);
    card.appendChild(header);

    const stats = document.createElement("div");
    stats.className = "rsu-stats";

    const recoveryTime = status.last_recovery_time_ms > 1_000_000_000 ? 1 : status.last_recovery_time_ms || 0;
    const statItems = [
      ["Events Received", status.received_events || 0],
      ["Bandwidth Saved", `${(status.bandwidth_saved_pct || 0).toFixed(1)}%`],
      ["Offline Buffer", status.buffered_count || 0],
      ["Recovery Time", `${recoveryTime} ms`],
    ];

    statItems.forEach(([label, value]) => {
      const item = document.createElement("div");
      item.className = "rsu-stat";
      addLine(item, "span", label);
      addLine(item, "strong", String(value));
      stats.appendChild(item);
    });

    card.appendChild(stats);
    container.appendChild(card);
  });
}

function renderExperimentMetrics(data) {
  const container = byId("experiment-bar");
  container.replaceChildren();

  const items = [
    ["p50 Latency (ms)", `${data.p50_latency_ms?.toFixed(1) || "0"}ms`],
    ["p95 Latency (ms)", `${data.p95_latency_ms?.toFixed(1) || "0"}ms`],
    ["Reordered Events", data.reordered_events || 0],
    ["Estimated Lost Packets", data.estimated_lost_events || 0],
    ["Traffic Density Reports", data.traffic_density_reports || 0],
    ["SNS Alerts Published", data.sns_alerts_published || 0],
  ];

  items.forEach(([label, value]) => {
    const item = document.createElement("div");
    item.className = "metric-item";
    item.innerHTML = `<div class="metric-label">${label}</div><div class="metric-value">${value}</div>`;
    container.appendChild(item);
  });
}

function renderControlPanel() {
  const container = byId("control-buttons");
  container.replaceChildren();

  const controls = [
    { rsuId: "rsu-1", action: "toggle-rsu-1" },
    { rsuId: "rsu-2", action: "toggle-rsu-2" },
    { label: "Reset All Metrics", action: "reset", className: "danger" },
  ];

  controls.forEach(({ rsuId, label, action, className, sector }) => {
    const disconnected = rsuId ? Boolean(latestRsuNodes[rsuId]?.cloud_disconnected) : false;
    const buttonLabel = rsuId
      ? `${rsuId.toUpperCase()} ${disconnected ? "Reconnect Cloud" : "Cloud Disconnect"}`
      : label;
    const btn = document.createElement("button");
    btn.textContent = buttonLabel;
    btn.className = [className || "", rsuId && disconnected ? "active" : ""].filter(Boolean).join(" ");
    btn.onclick = () => handleControl(action, sector);
    container.appendChild(btn);
  });
}

async function handleControl(action, sector) {
  try {
    if (action.startsWith("toggle-rsu-")) {
      const rsuId = action === "toggle-rsu-1" ? "rsu-1" : "rsu-2";
      const currentDisconnected = Boolean(latestRsuNodes[rsuId]?.cloud_disconnected);
      const disconnected = !currentDisconnected;
      const response = await fetch("/api/control/rsu-disconnection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rsu_id: rsuId, disconnected }),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      await update();
    } else if (action.startsWith("scenario-")) {
      const scenario = action.replace("scenario-", "");
      const response = await fetch("/api/control/simulate-scenario", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(sector ? { scenario, sector } : { scenario }),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const result = await response.json();
      if (result.status !== "ok") throw new Error(result.message || "Scenario failed");
      await update();
    } else if (action === "reset") {
      const response = await fetch("/api/control/reset", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      if (response.ok) {
        console.log("All metrics reset");
        await update();
      }
    }
  } catch (error) {
    console.error("Control action failed:", error);
  }
}

function renderFeed(id, entries, render) {
  const list = byId(id);
  list.replaceChildren();
  if (!entries.length) {
    const item = document.createElement("li");
    addLine(item, "span", "No activity yet.");
    list.appendChild(item);
  }
  entries.forEach((entry) => {
    const item = document.createElement("li");
    render(item, entry);
    list.appendChild(item);
  });
}

async function update() {
  try {
    const response = await fetch("/api/summary", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    const corridors = data.active_emergency_corridors;
    latestRsuNodes = data.rsu_nodes || {};
    byId("connection").textContent = "Live";
    byId("connection").classList.remove("offline");
    byId("processed").textContent = data.processed_events;
    byId("duplicates").textContent = data.duplicates_ignored;
    byId("corridor-count").textContent = corridors.length;
    byId("corridor").classList.toggle("active", corridors.length > 0);
    byId("corridor-title").textContent = corridors.length
      ? "EMERGENCY CORRIDOR ACTIVE — PULL LEFT / RIGHT"
      : "ROAD CLEAR — NORMAL TRAFFIC FLOW";
    byId("corridor-detail").textContent = corridors.length
      ? `${corridors.join(", ")} · Left lane move left; middle and right lanes move right.`
      : "Traffic monitoring is active.";
    renderVehicles(data.vehicles);
    renderDigitalTwin(data.vehicles, corridors, data.sector_vehicles);
    renderRSUStatus(data.rsu_nodes);
    renderExperimentMetrics(data);
    renderControlPanel();
    renderFeed("alerts", data.recent_alerts.slice(0, 5), (item, alert) => {
      if (alert.alert_type === "EMERGENCY_CORRIDOR_ALERT") item.className = "critical";
      addLine(item, "strong", alert.alert_type.replaceAll("_", " "));
      addLine(item, "span", `${alert.message} ${alert.action_instruction}`);
      addLine(item, "span", new Date(alert.created_at).toLocaleString());
    });
    renderFeed("events", data.recent_events, (item, event) => {
      addLine(item, "strong", `${event.source_id} · ${event.event_type.replaceAll("_", " ")}`);
      addLine(item, "span", `${event.location} · ${new Date(event.event_time).toLocaleString()}`);
    });
    byId("updated").textContent = `· Last update ${new Date().toLocaleTimeString()}`;
  } catch (error) {
    byId("connection").textContent = "Connection lost";
    byId("connection").classList.add("offline");
    console.error("Could not fetch dashboard summary:", error);
  }
}

async function poll() {
  await update();
  setTimeout(poll, 2000);
}
initTheme();
poll();
