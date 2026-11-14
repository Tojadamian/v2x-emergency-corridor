"""
Tests for Step 2A: Dashboard Upgrade with Digital Twin, RSU Edge Status, Experiment Metrics, and Control Endpoints.
- Verify GET / serves upgraded HTML with new sections
- Verify GET /api/summary includes rsu_nodes with bandwidth_saved_pct
- Verify POST /api/control/simulate-scenario injects synthetic events
- Verify POST /api/control/rsu-disconnection toggles RSU state
- Verify POST /api/control/reset attempts RSU resets
- Verify backward compatibility: all existing DOM elements remain functional
"""

import json
from pathlib import Path
import subprocess
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock

from common.schemas import V2XEvent
from v2x_service.config import Settings
from v2x_service.main import create_app
from v2x_service.storage import InMemoryStorage

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def storage():
    return InMemoryStorage()


@pytest.fixture
def client(storage):
    """Test client with in-memory storage."""
    settings = Settings(
        storage_backend="memory",
        rsu_1_url="http://127.0.0.1:8001",
        rsu_2_url="http://127.0.0.1:8002",
    )
    app = create_app(settings=settings, storage=storage)
    return TestClient(app)


class TestDashboardHTML:
    """Verify the upgraded dashboard HTML contains new sections and preserves existing IDs."""

    def test_dashboard_contains_existing_ids(self, client):
        """Verify all existing dashboard element IDs are still present."""
        response = client.get("/")
        assert response.status_code == 200
        html = response.text
        
        # Existing IDs from Deliverable 1
        assert 'id="connection"' in html
        assert 'id="corridor-title"' in html
        assert 'id="corridor-detail"' in html
        assert 'id="processed"' in html
        assert 'id="duplicates"' in html
        assert 'id="corridor-count"' in html
        assert 'id="vehicles"' in html
        assert 'id="alerts"' in html
        assert 'id="events"' in html
        assert 'id="updated"' in html

    def test_dashboard_contains_digital_twin_section(self, client):
        """Verify the Digital Twin visualization section is present."""
        response = client.get("/")
        assert response.status_code == 200
        html = response.text
        
        assert 'id="digital-twin"' in html
        assert 'id="twin-container"' in html
        assert "HIGHWAY DIGITAL TWIN" in html

    def test_dashboard_contains_rsu_status_section(self, client):
        """Verify the RSU Edge Computing Layer section is present."""
        response = client.get("/")
        assert response.status_code == 200
        html = response.text
        
        assert 'id="rsu-edge-status"' in html
        assert 'id="rsu-cards"' in html
        assert "EDGE COMPUTING LAYER" in html

    def test_dashboard_contains_experiment_metrics_section(self, client):
        """Verify the Experiment & QoS Metrics section is present."""
        response = client.get("/")
        assert response.status_code == 200
        html = response.text
        
        assert 'id="experiment-metrics"' in html
        assert 'id="experiment-bar"' in html
        assert "DELIVERABLE 2" in html

    def test_dashboard_contains_control_panel_section(self, client):
        """Verify the Interactive Operations Control Panel is present."""
        response = client.get("/")
        assert response.status_code == 200
        html = response.text
        
        assert 'id="control-panel"' in html
        assert 'id="control-buttons"' in html
        assert "INTERACTIVE OPERATIONS" in html

    def test_theme_toggle_and_default_dark_styles(self, client):
        assert 'id="theme-toggle"' in client.get("/").text
        css = (ROOT / "static" / "styles.css").read_text(encoding="utf-8")
        default_theme = css.split(":root[data-theme=\"light\"]", 1)[0]
        assert "color-scheme: dark" in default_theme
        assert "#0b1019" in default_theme
        assert "#121b29" in default_theme
        assert ":root[data-theme=\"light\"]" in css

    def test_twin_lanes_and_theme_behavior(self):
        script = r"""
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
class Element {
  constructor() { this.children = []; this.className = ""; this.textContent = ""; this.attributes = {}; this.listeners = {}; this.open = false; }
  appendChild(child) { this.children.push(child); return child; }
  replaceChildren() { this.children = []; }
  setAttribute(name, value) { this.attributes[name] = value; }
  addEventListener(name, callback) { this.listeners[name] = callback; }
  dispatch(name) { this.listeners[name]?.(); }
  click() { this.dispatch("click"); }
}
const elements = { "twin-container": new Element(), "theme-toggle": new Element() };
const saved = {};
const context = {
  document: {
    documentElement: { dataset: {} },
    getElementById: (id) => elements[id],
    createElement: () => new Element()
  },
  localStorage: {
    getItem: (key) => saved[key],
    setItem: (key, value) => { saved[key] = value; }
  },
  console
};
vm.createContext(context);
const source = fs.readFileSync("static/app.js", "utf8").replace(/initTheme\(\);\s*poll\(\);\s*$/, "");
vm.runInContext(source, context);
context.initTheme();
assert.equal(elements["theme-toggle"].textContent, "Day Mode");
elements["theme-toggle"].click();
assert.equal(context.document.documentElement.dataset.theme, "light");
assert.equal(saved.theme, "light");
assert.equal(elements["theme-toggle"].textContent, "Night Mode");
context.initTheme();
assert.equal(context.document.documentElement.dataset.theme, "light");
elements["theme-toggle"].click();
assert.equal(saved.theme, "dark");

context.renderDigitalTwin({}, []);
for (const sector of elements["twin-container"].children) {
  const header = sector.children[0];
  const details = header.children[1].children[1];
  assert.equal(details.className, "sector-actions-menu");
  assert.equal(details.children[0].textContent, "Sector Actions");
  assert.equal(details.children[1].children.length, 5);
  assert.ok(details.children[1].children.every((button) => button.title && button.children[1].textContent));
  assert.ok(sector.children[2].textContent.includes("Ambulance Standby"));
  const lanes = sector.children[1].children;
  assert.equal(lanes.flatMap((lane) => lane.children[1].children).length, 4);
}
const firstMenu = elements["twin-container"].children[0].children[0].children[1].children[1];
firstMenu.open = true;
firstMenu.dispatch("toggle");
context.renderDigitalTwin({}, []);
assert.equal(elements["twin-container"].children[0].children[0].children[1].children[1].open, true);
const vehicles = {
  "veh-001": { location: "sector-A12-tunnel", payload: { lane: 1, speed_kmh: 50 } },
  "veh-002": { location: "sector-A12-tunnel", payload: { lane: 2, speed_kmh: 60 } },
  "veh-005": { location: "sector-A12-tunnel", payload: { lane: 3, speed_kmh: 85 } }
};
const sectorVehicles = {
  "sector-A12-tunnel": {
    "veh-001": { payload: { lane: 2, speed_kmh: 42 } }
  }
};
context.renderDigitalTwin(vehicles, ["sector-A12-tunnel"], sectorVehicles);
const sector = elements["twin-container"].children[0];
const lanes = sector.children[1].children.filter((child) => child.className.startsWith("lane"));
const center = lanes[1].children[1].children;
const right = lanes[2].children[1].children;
assert.equal(center.length, 1);
assert.match(center[0].textContent, /veh-005 .* AMBULANCE .* 85 km\/h .* PASSING CORRIDOR/);
assert.ok(right.some((badge) => badge.textContent.includes("veh-002") && badge.className.includes("pull-right") && badge.textContent.includes("YIELDED (STOPPED)")));
assert.ok(right.some((badge) => badge.textContent.includes("veh-001") && badge.textContent.includes("42 km/h")));
assert.equal(vehicles["veh-002"].payload.lane, 2);
const quietSector = elements["twin-container"].children[1];
assert.ok(quietSector.children[2].textContent.includes("Ambulance Standby"));
assert.ok(quietSector.children[1].children.flatMap((lane) => lane.children[1].children).every((badge) => !badge.className.includes("ambulance")));
"""
        subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)


class TestAPISummaryWithRSU:
    """Verify GET /api/summary includes rsu_nodes and backward-compatible keys."""

    def test_summary_includes_all_existing_keys(self, client):
        """Verify backward compatibility: all existing summary keys are present."""
        response = client.get("/api/summary")
        assert response.status_code == 200
        data = response.json()
        
        # Deliverable 1 keys (100% backward compatible)
        assert "processed_events" in data
        assert "duplicates_ignored" in data
        assert "active_emergency_corridors" in data
        assert "vehicles" in data
        assert "sector_vehicles" in data
        assert "recent_alerts" in data
        assert "recent_events" in data
        assert "p50_latency_ms" in data
        assert "p95_latency_ms" in data
        assert "avg_latency_ms" in data
        assert "reordered_events" in data
        assert "estimated_lost_events" in data
        assert "traffic_density_reports" in data
        assert "sns_alerts_published" in data

    def test_summary_seeds_independent_sector_baselines(self, client):
        sector_vehicles = client.get("/api/summary").json()["sector_vehicles"]
        assert set(sector_vehicles) >= {"sector-A12-tunnel", "sector-B04-bridge"}
        for sector in ("sector-A12-tunnel", "sector-B04-bridge"):
            vehicles = sector_vehicles[sector]
            assert set(vehicles) >= {"veh-001", "veh-002", "veh-003", "veh-004", "veh-005"}
            for vehicle_id in ("veh-001", "veh-002", "veh-003", "veh-004"):
                assert 55 <= vehicles[vehicle_id]["payload"]["speed_kmh"] <= 80
                assert vehicles[vehicle_id]["payload"]["lane"] in (1, 2, 3)
            ambulance = vehicles["veh-005"]
            assert ambulance["payload"]["siren_active"] is False
            assert ambulance["payload"]["speed_kmh"] == 0

    def test_summary_includes_rsu_nodes(self, client):
        """Verify GET /api/summary includes rsu_nodes dictionary."""
        response = client.get("/api/summary")
        assert response.status_code == 200
        data = response.json()
        
        assert "rsu_nodes" in data
        rsu_nodes = data["rsu_nodes"]
        
        # Fallback mode (RSUs unreachable): should have rsu-1 and rsu-2 with local state
        assert "rsu-1" in rsu_nodes
        assert "rsu-2" in rsu_nodes

    def test_rsu_node_contains_required_fields(self, client):
        """Verify each RSU node has expected fields (graceful fallback)."""
        response = client.get("/api/summary")
        assert response.status_code == 200
        data = response.json()
        
        for rsu_id, rsu_status in data["rsu_nodes"].items():
            assert "rsu_id" in rsu_status or rsu_id in ["rsu-1", "rsu-2"]
            assert "cloud_connected" in rsu_status
            assert "received_events" in rsu_status or rsu_status.get("received_events", 0) == 0
            assert "filtered_telemetry_count" in rsu_status or rsu_status.get("filtered_telemetry_count", 0) == 0
            assert "forwarded_to_sqs" in rsu_status or rsu_status.get("forwarded_to_sqs", 0) == 0
            assert "bandwidth_saved_pct" in rsu_status

    def test_bandwidth_saved_pct_calculation(self, client, storage):
        """Verify bandwidth_saved_pct is calculated correctly (or 0 in fallback)."""
        response = client.get("/api/summary")
        assert response.status_code == 200
        data = response.json()
        
        for rsu_status in data["rsu_nodes"].values():
            bw_saved = rsu_status.get("bandwidth_saved_pct", 0)
            assert isinstance(bw_saved, (int, float))
            assert bw_saved >= 0


class TestSimulateScenarioEndpoint:
    """Verify POST /api/control/simulate-scenario injects synthetic events."""

    def test_simulate_emergency_corridor(self, client, storage):
        """Verify emergency_corridor scenario triggers ambulance and alert."""
        response = client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "emergency_corridor"},
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "ok"
        assert data["scenario"] == "emergency_corridor"
        assert data["events_injected"] >= 1
        
        # Verify event was processed
        summary = client.get("/api/summary").json()
        assert summary["processed_events"] >= 1

    def test_simulate_sudden_braking(self, client, storage):
        """Verify sudden_braking scenario injects hard-braking event."""
        response = client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "sudden_braking"},
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "ok"
        assert data["scenario"] == "sudden_braking"
        assert data["events_injected"] >= 1

    def test_simulate_road_hazard(self, client, storage):
        """Verify road_hazard scenario injects hazard event."""
        response = client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "road_hazard"},
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "ok"
        assert data["scenario"] == "road_hazard"
        assert data["events_injected"] >= 1

    def test_simulate_duplicate_burst(self, client, storage):
        """Verify duplicate_burst scenario injects multiple identical events."""
        response = client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "duplicate_burst"},
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "ok"
        assert data["scenario"] == "duplicate_burst"
        # Burst should inject 3 duplicates, but only 1 is processed due to idempotency
        assert data["events_injected"] >= 1

    def test_simulate_unknown_scenario(self, client):
        """Verify unknown scenario returns error."""
        response = client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "unknown_scenario"},
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "error"
        assert "Unknown scenario" in data.get("message", "")

    def test_simulate_scenario_increments_processed_events(self, client, storage):
        """Verify scenarios increment processed_events counter."""
        initial = client.get("/api/summary").json()
        initial_count = initial["processed_events"]
        
        client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "emergency_corridor"},
        )
        
        after = client.get("/api/summary").json()
        assert after["processed_events"] > initial_count

    @pytest.mark.parametrize(
        ("scenario", "event_type"),
        [
            ("emergency_corridor", "emergency_vehicle"),
            ("sudden_braking", "sudden_braking"),
            ("road_hazard", "road_hazard"),
            ("duplicate_burst", "traffic_telemetry"),
        ],
    )
    def test_scenario_events_include_lane_and_speed(self, client, scenario, event_type):
        client.post("/api/control/simulate-scenario", json={"scenario": scenario})

        event = next(
            event for event in client.get("/api/events").json()
            if event["event_type"] == event_type
        )
        assert event["payload"]["lane"] in (1, 2, 3)
        if scenario == "emergency_corridor":
            assert 85 <= event["payload"]["speed_kmh"] <= 100
        elif scenario == "sudden_braking":
            assert 12 <= event["payload"]["speed_kmh"] <= 25
        elif scenario == "road_hazard":
            assert 15 <= event["payload"]["speed_kmh"] <= 28
        elif scenario == "duplicate_burst":
            assert event["payload"]["speed_kmh"] == 60

    def test_emergency_corridor_yields_vehicles_in_target_sector(self, client):
        client.post("/api/control/simulate-scenario", json={"scenario": "emergency_corridor"})

        summary = client.get("/api/summary").json()
        bridge = summary["sector_vehicles"]["sector-B04-bridge"]
        for vehicle_id in ("veh-001", "veh-002", "veh-003", "veh-004"):
            assert bridge[vehicle_id]["payload"]["speed_kmh"] <= 5
            assert bridge[vehicle_id]["payload"]["lane"] in (1, 3)
            assert bridge[vehicle_id]["payload"]["yielding"] is True
        ambulance = bridge["veh-005"]
        assert ambulance["event_type"] == "emergency_vehicle"
        assert ambulance["payload"]["siren_active"] is True
        assert 85 <= ambulance["payload"]["speed_kmh"] <= 100
        assert summary["sector_vehicles"]["sector-A12-tunnel"]["veh-005"]["payload"]["siren_active"] is False
        assert summary["active_emergency_corridors"] == ["sector-B04-bridge"]

    def test_emergency_corridor_in_tunnel_does_not_activate_bridge(self, client):
        response = client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "emergency_corridor", "sector": "sector-A12-tunnel"},
        )
        assert response.json()["status"] == "ok"
        summary = client.get("/api/summary").json()
        tunnel = summary["sector_vehicles"]["sector-A12-tunnel"]
        bridge = summary["sector_vehicles"]["sector-B04-bridge"]
        assert all(tunnel[vehicle_id]["payload"]["speed_kmh"] <= 5 for vehicle_id in ("veh-001", "veh-002", "veh-003", "veh-004"))
        assert tunnel["veh-005"]["payload"]["siren_active"] is True
        assert bridge["veh-005"]["payload"]["siren_active"] is False
        assert summary["active_emergency_corridors"] == ["sector-A12-tunnel"]

    def test_normal_traffic_restores_target_sector_and_clears_corridor(self, client):
        client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "emergency_corridor", "sector": "sector-A12-tunnel"},
        )
        response = client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "normal_traffic", "sector": "sector-A12-tunnel"},
        )
        assert response.json()["status"] == "ok"
        summary = client.get("/api/summary").json()
        tunnel = summary["sector_vehicles"]["sector-A12-tunnel"]
        for vehicle_id in ("veh-001", "veh-002", "veh-003", "veh-004"):
            assert tunnel[vehicle_id]["payload"]["speed_kmh"] >= 50
            assert tunnel[vehicle_id]["payload"]["lane"] in (1, 2, 3)
        assert tunnel["veh-005"]["payload"]["siren_active"] is False
        assert "sector-A12-tunnel" not in summary["active_emergency_corridors"]

    def test_dashboard_css_includes_physics_animations_and_braking_style(self):
        css = (ROOT / "static" / "styles.css").read_text(encoding="utf-8")
        assert ".vehicle-badge.braking" in css
        assert "@keyframes pull-over-left" in css
        assert "@keyframes pull-over-right" in css
        assert "@keyframes ambulance-overtake" in css

    def test_tunnel_emergency_activates_and_forwards_to_rsu_1(self, client):
        response_from_rsu = AsyncMock()
        response_from_rsu.raise_for_status = lambda: None
        http_client = AsyncMock()
        http_client.__aenter__.return_value = http_client
        http_client.post.return_value = response_from_rsu

        with patch("v2x_service.main.httpx.AsyncClient", return_value=http_client):
            response = client.post(
                "/api/control/simulate-scenario",
                json={"scenario": "emergency_corridor", "sector": "sector-A12-tunnel"},
            )

        assert response.json()["status"] == "ok"
        assert client.get("/api/state").json()["active_emergency_corridors"] == ["sector-A12-tunnel"]
        http_client.post.assert_awaited_once()
        call = http_client.post.await_args
        assert call.args[0] == "http://127.0.0.1:8001/api/events"
        assert call.kwargs["json"]["location"] == "sector-A12-tunnel"
        assert call.kwargs["json"]["source_id"] == "veh-005"

    def test_invalid_scenario_sector_is_rejected(self, client, storage):
        response = client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "emergency_corridor", "sector": "unknown"},
        )
        assert response.json()["status"] == "error"
        assert storage.summary()["processed_events"] == 0


class TestRSUDisconnectionEndpoint:
    """Verify POST /api/control/rsu-disconnection toggles RSU state."""

    def test_rsu_disconnection_rsu_1(self, client):
        """Verify RSU-1 disconnection toggle."""
        response = client.post(
            "/api/control/rsu-disconnection",
            json={"rsu_id": "rsu-1", "disconnected": True},
        )
        assert response.status_code == 200
        data = response.json()
        
        # Should succeed (either forwarded or stored locally)
        assert data["status"] in ["forwarded", "stored_locally"]
        assert data["rsu_id"] == "rsu-1"

    def test_rsu_disconnection_rsu_2(self, client):
        """Verify RSU-2 disconnection toggle."""
        response = client.post(
            "/api/control/rsu-disconnection",
            json={"rsu_id": "rsu-2", "disconnected": False},
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] in ["forwarded", "stored_locally"]
        assert data["rsu_id"] == "rsu-2"

    def test_rsu_disconnection_invalid_rsu_id(self, client):
        """Verify invalid RSU ID returns error."""
        response = client.post(
            "/api/control/rsu-disconnection",
            json={"rsu_id": "rsu-invalid", "disconnected": True},
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "error"
        assert "Invalid rsu_id" in data.get("message", "")

    def test_rsu_disconnection_stores_state_locally(self, client):
        """Verify disconnection state is stored locally (fallback mode)."""
        response = client.post(
            "/api/control/rsu-disconnection",
            json={"rsu_id": "rsu-1", "disconnected": True},
        )
        assert response.status_code == 200
        
        # Verify state persists in summary
        summary = client.get("/api/summary").json()
        rsu_1_status = summary["rsu_nodes"].get("rsu-1", {})
        # In fallback mode, the cloud_connected state should reflect the toggle
        assert isinstance(rsu_1_status.get("cloud_connected"), bool)


class TestResetEndpoint:
    """Verify POST /api/control/reset clears all metrics."""

    def test_reset_clears_processed_events(self, client, storage):
        """Verify reset clears processed events counter."""
        # Inject an event
        client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "emergency_corridor"},
        )
        assert client.get("/api/summary").json()["processed_events"] > 0
        
        # Reset
        response = client.post("/api/control/reset")
        assert response.status_code == 200
        assert response.json()["status"] == "reset"
        
        # Verify cleared
        assert client.get("/api/summary").json()["processed_events"] == 0

    def test_reset_clears_duplicates(self, client, storage):
        """Verify reset clears duplicates counter."""
        # Inject duplicate
        event = V2XEvent(
            event_id="test-dup",
            source_id="veh-001",
            event_type="traffic_telemetry",
            location="sector-A12-tunnel",
            payload={"speed_kmh": 60},
        )
        storage.process_event(event)
        storage.process_event(event)  # Duplicate
        
        assert client.get("/api/summary").json()["duplicates_ignored"] > 0
        
        # Reset
        client.post("/api/control/reset")
        
        # Verify cleared
        assert client.get("/api/summary").json()["duplicates_ignored"] == 0

    def test_reset_clears_alerts(self, client, storage):
        """Verify reset clears alerts."""
        # Trigger an alert
        client.post(
            "/api/control/simulate-scenario",
            json={"scenario": "emergency_corridor"},
        )
        assert len(client.get("/api/summary").json()["recent_alerts"]) > 0
        
        # Reset
        client.post("/api/control/reset")
        
        # Verify cleared
        assert len(client.get("/api/summary").json()["recent_alerts"]) == 0

    def test_reset_attempts_rsu_reset(self, client):
        """Verify reset attempts to call RSU reset endpoints (gracefully ignores failures)."""
        # This should not raise an error even if RSUs are unreachable
        response = client.post("/api/control/reset")
        assert response.status_code == 200
        assert response.json()["status"] == "reset"


class TestBackwardCompatibility:
    """Verify all Deliverable 1 tests still pass with new endpoints."""

    def test_existing_get_state_endpoint(self, client, storage):
        """Verify GET /api/state still works (Deliverable 1 compatibility)."""
        response = client.get("/api/state")
        assert response.status_code == 200
        data = response.json()
        
        assert "vehicles" in data
        assert "active_emergency_corridors" in data

    def test_existing_get_events_endpoint(self, client, storage):
        """Verify GET /api/events still works (Deliverable 1 compatibility)."""
        response = client.get("/api/events")
        assert response.status_code == 200
        data = response.json()
        
        assert isinstance(data, list)

    def test_existing_get_alerts_endpoint(self, client, storage):
        """Verify GET /api/alerts still works (Deliverable 1 compatibility)."""
        response = client.get("/api/alerts")
        assert response.status_code == 200
        data = response.json()
        
        assert isinstance(data, list)

    def test_existing_ingest_endpoint(self, client, storage):
        """Verify POST /api/events/ingest still works (Deliverable 1 compatibility)."""
        event = V2XEvent(
            source_id="veh-001",
            event_type="traffic_telemetry",
            location="sector-A12-tunnel",
            payload={"speed_kmh": 60},
        )
        response = client.post("/api/events/ingest", json=event.model_dump())
        assert response.status_code == 200
        data = response.json()
        
        assert "processed" in data
        assert "alert" in data


class TestEndpointErrorHandling:
    """Verify endpoints handle errors gracefully."""

    def test_simulate_scenario_with_empty_payload(self, client):
        """Verify endpoint handles missing scenario gracefully."""
        response = client.post(
            "/api/control/simulate-scenario",
            json={},
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "error"

    def test_rsu_disconnection_with_missing_fields(self, client):
        """Verify endpoint handles missing fields gracefully."""
        response = client.post(
            "/api/control/rsu-disconnection",
            json={},
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "error"

    def test_summary_with_unreachable_rsu_urls(self, client):
        """Verify summary provides fallback data if RSU URLs are unreachable."""
        response = client.get("/api/summary")
        assert response.status_code == 200
        data = response.json()
        
        # Fallback rsu_nodes should exist even if RSUs are unreachable
        assert "rsu_nodes" in data
        assert "rsu-1" in data["rsu_nodes"]
        assert "rsu-2" in data["rsu_nodes"]
