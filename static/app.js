const byId = (id) => document.getElementById(id);
const vehicleIds = ["veh-001", "veh-002", "veh-003", "veh-004", "veh-005"];
const defaults = ["sector-A12-tunnel", "sector-A12-tunnel", "sector-B04-bridge", "sector-B04-bridge", "sector-A12-tunnel"];

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
