const byId = (id) => document.getElementById(id);
const vehicleIds = ["veh-001", "veh-002", "veh-003", "veh-004", "veh-005"];
const defaults = ["sector-A12-tunnel", "sector-A12-tunnel", "sector-A12-tunnel", "sector-B04-bridge", "sector-B04-bridge"];
const sectorGroups = {
  "sector-A12-tunnel": ["veh-001", "veh-002", "veh-003"],
  "sector-B04-bridge": ["veh-004", "veh-005"],
};
let latestRsuNodes = {};

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

function renderDigitalTwin(vehicles, activeCorridors) {
  const container = byId("twin-container");
  container.replaceChildren();

  Object.entries(sectorGroups).forEach(([sector, vehicleList]) => {
    const sectorDiv = document.createElement("div");
    sectorDiv.className = "sector";
    
    const title = document.createElement("div");
    title.className = "sector-title";
    title.textContent = sector === "sector-A12-tunnel" ? "Sector A12 (Tunnel) · RSU 1" : "Sector B04 (Bridge) · RSU 2";
    sectorDiv.appendChild(title);

    const lanesDiv = document.createElement("div");
    lanesDiv.className = "lanes";

    const isEmergencyActive = activeCorridors.includes(sector);

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
      label.textContent = `Lane ${lane}`;
      laneDiv.appendChild(label);

      const vehiclesDiv = document.createElement("div");
      vehiclesDiv.className = "vehicles-in-lane";

      vehicleList.forEach((vId) => {
        const state = vehicles[vId];
        if (state && state.payload.lane === lane) {
          const badge = document.createElement("div");
          badge.className = "vehicle-badge";
          if (vId === "veh-005") badge.className += " ambulance";
          else if (state.event_type === "emergency_vehicle") badge.className += " emergency";
          if (isEmergencyActive && lane === 1) badge.className += " pull-left";
          if (isEmergencyActive && lane > 1 && vId !== "veh-005" && state.event_type !== "emergency_vehicle") {
            badge.className += " pull-right";
          }

          const speed = state.payload.speed_kmh ?? "—";
          badge.textContent = `${vId}: ${speed} km/h`;
          vehiclesDiv.appendChild(badge);
        }
      });

      laneDiv.appendChild(vehiclesDiv);
      lanesDiv.appendChild(laneDiv);
    }

    sectorDiv.appendChild(lanesDiv);
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
    title.textContent = rsuId.toUpperCase();
    const statusBadge = document.createElement("span");
    statusBadge.className = `rsu-status ${status.cloud_connected ? "online" : "offline"}`;
    statusBadge.textContent = status.cloud_connected ? "ONLINE" : "CLOUD DISCONNECTED — BUFFERING";
    header.appendChild(title);
    header.appendChild(statusBadge);
    card.appendChild(header);

    const stats = document.createElement("div");
    stats.className = "rsu-stats";

    const statItems = [
      ["Received Events", status.received_events || 0],
      ["Filtered Telemetry", status.filtered_telemetry_count || 0],
      ["Forwarded to SQS", status.forwarded_to_sqs || 0],
      ["Bandwidth Saved", `${(status.bandwidth_saved_pct || 0).toFixed(1)}%`],
      ["Buffered Offline", status.buffered_count || 0],
      ["Flushed After Reconnect", status.flushed_events_count || 0],
      ["Last Recovery Time", `${status.last_recovery_time_ms || 0}ms`],
      ["Local Edge Alerts", status.local_edge_alerts || 0],
      ["Relayed Cloud Alerts", status.relayed_cloud_alerts || 0],
    ];

    statItems.forEach(([label, value]) => {
      const item = document.createElement("div");
      item.className = "rsu-stat";
      item.innerHTML = `${label}<strong>${value}</strong>`;
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
    { label: "Trigger Ambulance Corridor", action: "scenario-emergency_corridor" },
    { label: "Trigger Severe Braking", action: "scenario-sudden_braking" },
    { label: "Trigger Road Hazard", action: "scenario-road_hazard" },
    { label: "Inject Duplicate Burst", action: "scenario-duplicate_burst" },
    { label: "Reset All Metrics", action: "reset", className: "danger" },
  ];

  controls.forEach(({ rsuId, label, action, className }) => {
    const disconnected = rsuId ? Boolean(latestRsuNodes[rsuId]?.cloud_disconnected) : false;
    const buttonLabel = rsuId
      ? `${rsuId.toUpperCase()} ${disconnected ? "Reconnect Cloud" : "Cloud Disconnect"}`
      : label;
    const btn = document.createElement("button");
    btn.textContent = buttonLabel;
    btn.className = [className || "", rsuId && disconnected ? "active" : ""].filter(Boolean).join(" ");
    btn.onclick = () => handleControl(action);
    container.appendChild(btn);
  });
}

async function handleControl(action) {
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
        body: JSON.stringify({ scenario }),
      });
      if (response.ok) {
        console.log(`Scenario '${scenario}' triggered successfully`);
      }
    } else if (action === "reset") {
      const response = await fetch("/api/control/reset", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      if (response.ok) {
        console.log("All metrics reset");
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
    byId("corridor-title").textContent = corridors.length ? "EMERGENCY CORRIDOR ACTIVE" : "No active emergency corridors";
    byId("corridor-detail").textContent = corridors.length
      ? `${corridors.join(", ")} · Left lane move left; middle and right lanes move right.`
      : "Traffic monitoring is active.";
    renderVehicles(data.vehicles);
    renderDigitalTwin(data.vehicles, corridors);
    renderRSUStatus(data.rsu_nodes);
    renderExperimentMetrics(data);
    renderControlPanel();
    renderFeed("alerts", data.recent_alerts, (item, alert) => {
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
poll();
