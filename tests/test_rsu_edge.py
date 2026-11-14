import json
import pytest
from datetime import datetime, timezone

from common.schemas import V2XEvent, V2XAlert
from rsu_service.edge_processor import EdgeProcessor


def create_event(
    event_id: str = None,
    source_id: str = "veh-001",
    event_type: str = "traffic_telemetry",
    location: str = "sector-A12-tunnel",
    seq_num: int = None,
    rsu_id: str = None,
    **payload_fields,
) -> V2XEvent:
    """Helper to create V2XEvent for testing."""
    if event_id is None:
        import uuid
        event_id = f"evt-{uuid.uuid4().hex}"
    
    return V2XEvent(
        event_id=event_id,
        source_id=source_id,
        event_type=event_type,
        location=location,
        seq_num=seq_num,
        rsu_id=rsu_id,
        payload=payload_fields or {},
    )


class TestLocalAlertGeneration:
    """Test local alert generation on RSU for critical events."""

    def test_local_emergency_corridor_alert_on_ambulance_with_siren(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        event = create_event(
            event_id="emg-001",
            source_id="veh-005",
            event_type="emergency_vehicle",
            siren_active=True,
            speed_kmh=80,
        )
        
        forwarded, alert = processor.process_event(event)
        
        assert alert is not None
        assert alert.alert_type == "LOCAL_EMERGENCY_CORRIDOR_ALERT"
        assert alert.severity == "CRITICAL"
        assert alert.origin == "local"
        assert alert.rsu_id == "rsu-1"
        assert forwarded == event
        assert processor.local_alerts_count == 1

    def test_no_local_alert_on_ambulance_without_siren(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        event = create_event(
            event_id="emg-002",
            source_id="veh-005",
            event_type="emergency_vehicle",
            siren_active=False,
            speed_kmh=80,
        )
        
        forwarded, alert = processor.process_event(event)
        
        assert alert is None
        assert forwarded == event
        assert processor.local_alerts_count == 0

    def test_local_collision_risk_alert_on_severe_braking(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        event = create_event(
            event_id="brk-001",
            source_id="veh-002",
            event_type="sudden_braking",
            deceleration_ms2=7.5,
            speed_kmh=80,
        )
        
        forwarded, alert = processor.process_event(event)
        
        assert alert is not None
        assert alert.alert_type == "LOCAL_COLLISION_RISK_ALERT"
        assert alert.severity == "CRITICAL"
        assert alert.origin == "local"
        assert alert.rsu_id == "rsu-1"
        assert forwarded == event
        assert processor.local_alerts_count == 1

    def test_no_alert_on_moderate_braking(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        event = create_event(
            event_id="brk-002",
            source_id="veh-002",
            event_type="sudden_braking",
            deceleration_ms2=5.5,
            speed_kmh=80,
        )
        
        forwarded, alert = processor.process_event(event)
        
        assert alert is None
        assert forwarded == event
        assert processor.local_alerts_count == 0

    def test_local_road_hazard_alert(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        event = create_event(
            event_id="haz-001",
            source_id="rsu-1",
            event_type="road_hazard",
            hazard_type="pothole",
            location="sector-A12-tunnel",
        )
        
        forwarded, alert = processor.process_event(event)
        
        assert alert is not None
        assert alert.alert_type == "LOCAL_ROAD_HAZARD_ALERT"
        assert alert.severity == "WARNING"
        assert alert.origin == "local"
        assert alert.rsu_id == "rsu-1"
        assert forwarded == event
        assert processor.local_alerts_count == 1


class TestEdgeFilteringAndAggregation:
    """Test edge filtering and tumbling-window aggregation."""

    def test_critical_events_forwarded_immediately(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        
        emergency = create_event(
            event_id="emg-001",
            source_id="veh-005",
            event_type="emergency_vehicle",
            siren_active=True,
        )
        forwarded, _ = processor.process_event(emergency)
        assert forwarded == emergency
        assert processor.forwarded_events == 1
        
        braking = create_event(
            event_id="brk-001",
            source_id="veh-002",
            event_type="sudden_braking",
            deceleration_ms2=6.5,
        )
        forwarded, _ = processor.process_event(braking)
        assert forwarded == braking
        assert processor.forwarded_events == 2
        
        hazard = create_event(
            event_id="haz-001",
            source_id="rsu-1",
            event_type="road_hazard",
            hazard_type="accident",
        )
        forwarded, _ = processor.process_event(hazard)
        assert forwarded == hazard
        assert processor.forwarded_events == 3

    def test_traffic_telemetry_filtered_and_aggregated(self):
        processor = EdgeProcessor(
            rsu_id="rsu-1",
            assigned_sector="sector-A12-tunnel",
            aggregation_window_size=3,
        )
        
        for i in range(3):
            event = create_event(
                event_id=f"tel-{i}",
                source_id=f"veh-{i:03d}",
                event_type="traffic_telemetry",
                speed_kmh=50 + i * 10,
                lane=1,
            )
            forwarded, _ = processor.process_event(event)
            if i < 2:
                assert forwarded is None
                assert processor.filtered_telemetry_count == i + 1
            else:
                assert forwarded is not None
                assert forwarded.event_type == "traffic_density"
                assert forwarded.source_id == "rsu-1"
                assert forwarded.location == "sector-A12-tunnel"
                assert forwarded.rsu_id == "rsu-1"
                
                assert forwarded.payload["vehicle_count"] == 3
                assert forwarded.payload["sample_count"] == 3
                assert forwarded.payload["avg_speed_kmh"] == 60
                assert len(forwarded.payload["vehicles"]) == 3
                
                assert processor.filtered_telemetry_count == 2
                assert processor.forwarded_events == 1

    def test_telemetry_buffer_partial_window_not_forwarded(self):
        processor = EdgeProcessor(
            rsu_id="rsu-1",
            assigned_sector="sector-A12-tunnel",
            aggregation_window_size=5,
        )
        
        for i in range(3):
            event = create_event(
                event_id=f"tel-{i}",
                source_id=f"veh-{i:03d}",
                event_type="traffic_telemetry",
                speed_kmh=50,
            )
            forwarded, _ = processor.process_event(event)
            assert forwarded is None
        
        assert processor.filtered_telemetry_count == 3
        assert processor.forwarded_events == 0
        assert len(processor.telemetry_buffer) == 3


class TestDuplicateDetection:
    """Test duplicate detection and idempotency."""

    def test_duplicate_events_ignored(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        # Use emergency_vehicle which is forwarded immediately
        event = create_event(
            event_id="evt-001",
            source_id="veh-001",
            event_type="emergency_vehicle",
            siren_active=True,
        )
        
        forwarded1, _ = processor.process_event(event)
        assert forwarded1 == event
        assert processor.received_events == 1
        assert processor.forwarded_events == 1
        
        forwarded2, _ = processor.process_event(event)
        assert forwarded2 is None
        assert processor.received_events == 2
        assert processor.duplicates_ignored == 1
        assert processor.forwarded_events == 1


class TestSequenceTracking:
    """Test sequence number tracking for out-of-order and lost packet detection."""

    def test_detect_out_of_order_events(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        
        from datetime import datetime, timezone, timedelta
        
        now = datetime.now(timezone.utc)
        
        event1 = create_event(
            event_id="evt-001",
            source_id="veh-001",
            seq_num=1,
            event_type="traffic_telemetry",
            speed_kmh=50,
        )
        event1.event_time = now.isoformat()
        processor.process_event(event1)
        
        event2 = create_event(
            event_id="evt-002",
            source_id="veh-001",
            seq_num=3,
            event_type="traffic_telemetry",
            speed_kmh=55,
        )
        event2.event_time = (now + timedelta(seconds=2)).isoformat()
        processor.process_event(event2)
        
        event3 = create_event(
            event_id="evt-003",
            source_id="veh-001",
            seq_num=2,
            event_type="traffic_telemetry",
            speed_kmh=52,
        )
        event3.event_time = (now + timedelta(seconds=1)).isoformat()
        processor.process_event(event3)
        
        assert processor.reordered_events == 1

    def test_detect_lost_packets(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        
        event1 = create_event(event_id="evt-001", source_id="veh-001", seq_num=1)
        processor.process_event(event1)
        
        event2 = create_event(event_id="evt-002", source_id="veh-001", seq_num=5)
        processor.process_event(event2)
        
        assert processor.estimated_lost_events == 3

    def test_sequence_tracking_per_vehicle(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        
        event1 = create_event(event_id="evt-001", source_id="veh-001", seq_num=1)
        processor.process_event(event1)
        
        event2 = create_event(event_id="evt-002", source_id="veh-002", seq_num=10)
        processor.process_event(event2)
        
        event3 = create_event(event_id="evt-003", source_id="veh-001", seq_num=3)
        processor.process_event(event3)
        
        assert processor.estimated_lost_events == 1


class TestEdgeCloudDisconnectionBuffering:
    """Test edge-cloud disconnection buffering and recovery."""

    def test_buffer_events_when_disconnected(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        processor.cloud_disconnected = True
        
        event = create_event(
            event_id="evt-001",
            source_id="veh-001",
            event_type="emergency_vehicle",
            siren_active=True,
        )
        
        processor.buffer_event(event)
        assert processor.buffered_count == 1
        assert len(processor.buffered_events) == 1

    def test_buffer_overflow_rejected(self):
        processor = EdgeProcessor(
            rsu_id="rsu-1",
            assigned_sector="sector-A12-tunnel",
            buffer_max_size=2,
        )
        
        event1 = create_event(event_id="evt-001", source_id="veh-001")
        event2 = create_event(event_id="evt-002", source_id="veh-002")
        event3 = create_event(event_id="evt-003", source_id="veh-003")
        
        assert processor.buffer_event(event1) is True
        assert processor.buffer_event(event2) is True
        assert processor.buffer_event(event3) is False
        
        assert processor.buffered_count == 2
        assert processor.rejected_events == 1

    def test_buffer_flush_and_recovery(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        processor.cloud_disconnected = True
        
        events = [
            create_event(event_id=f"evt-{i}", source_id=f"veh-{i}")
            for i in range(3)
        ]
        
        for event in events:
            processor.buffer_event(event)
        
        assert processor.buffered_count == 3
        
        flushed = processor.flush_buffer()
        assert len(flushed) == 3
        assert flushed == events
        assert processor.buffered_count == 0
        assert processor.flushed_events_count == 3
        assert processor.last_recovery_time_ms > 0


class TestSNSAlertRelay:
    """Test SNS notification parsing and relay to MQTT."""

    def test_relay_cloud_alert(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        
        alert = V2XAlert(
            alert_type="EMERGENCY_CORRIDOR_ALERT",
            severity="CRITICAL",
            location="sector-A12-tunnel",
            triggered_by_event_id="evt-001",
            source_id="cloud",
            message="Emergency corridor activated",
            action_instruction="Move to emergency lane",
            origin="cloud",
        )
        
        processor.relay_cloud_alert(alert)
        
        assert processor.relayed_cloud_alerts_count == 1
        assert len(processor.recent_relayed_alerts) == 1
        assert processor.recent_relayed_alerts[0]["alert_type"] == "EMERGENCY_CORRIDOR_ALERT"

    def test_multiple_relayed_alerts_retained(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        
        for i in range(60):
            alert = V2XAlert(
                alert_type="EMERGENCY_CORRIDOR_ALERT",
                severity="CRITICAL",
                location="sector-A12-tunnel",
                triggered_by_event_id=f"evt-{i}",
                source_id="cloud",
                message=f"Alert {i}",
                action_instruction="Move to emergency lane",
                origin="cloud",
            )
            processor.relay_cloud_alert(alert)
        
        assert processor.relayed_cloud_alerts_count == 60
        assert len(processor.recent_relayed_alerts) == 50


class TestRSUStatusAndCounters:
    """Test RSU status endpoint and counter accuracy."""

    def test_get_status_returns_all_counters(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        
        status = processor.get_status()
        
        assert status["rsu_id"] == "rsu-1"
        assert status["sector"] == "sector-A12-tunnel"
        assert status["received_events"] == 0
        assert status["forwarded_events"] == 0
        assert status["duplicates_ignored"] == 0
        assert status["filtered_telemetry_count"] == 0
        assert status["buffered_count"] == 0
        assert status["rejected_events"] == 0
        assert status["flushed_events_count"] == 0
        assert status["last_recovery_time_ms"] == 0
        assert status["cloud_disconnected"] is False
        assert status["local_alerts_count"] == 0
        assert status["relayed_cloud_alerts_count"] == 0

    def test_status_reflects_processing_activities(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        
        event = create_event(
            event_id="evt-001",
            source_id="veh-001",
            event_type="emergency_vehicle",
            siren_active=True,
        )
        processor.process_event(event)
        
        status = processor.get_status()
        assert status["received_events"] == 1
        assert status["forwarded_events"] == 1
        assert status["local_alerts_count"] == 1


class TestBackwardCompatibility:
    """Test backward compatibility with MVP schemas."""

    def test_event_without_rsu_id_and_seq_num_valid(self):
        """Events without rsu_id and seq_num should still be valid (backward compatible)."""
        event = create_event(event_id="evt-001", source_id="veh-001")
        
        assert event.rsu_id is None
        assert event.seq_num is None

    def test_alert_with_default_origin(self):
        """Alerts default to origin='cloud' (backward compatible)."""
        alert = V2XAlert(
            alert_type="EMERGENCY_CORRIDOR_ALERT",
            severity="CRITICAL",
            location="sector-A12-tunnel",
            triggered_by_event_id="evt-001",
            source_id="veh-005",
            message="Test",
            action_instruction="Test",
        )
        
        assert alert.origin == "cloud"
        assert alert.rsu_id is None

    def test_new_event_types_accepted(self):
        """New event types (road_hazard, traffic_density) should be accepted."""
        hazard = create_event(
            event_id="haz-001",
            event_type="road_hazard",
            hazard_type="pothole",
        )
        assert hazard.event_type == "road_hazard"
        
        density = create_event(
            event_id="den-001",
            event_type="traffic_density",
            avg_speed_kmh=45,
        )
        assert density.event_type == "traffic_density"

    def test_old_tests_unchanged(self):
        """Verify existing MVP test patterns still work."""
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        
        event = create_event(
            event_id="evt-001",
            source_id="veh-001",
            event_type="traffic_telemetry",
            speed_kmh=50,
        )
        
        forwarded, alert = processor.process_event(event)
        assert forwarded is None
        assert alert is None


class TestComplexIntegrationScenarios:
    """Test complex scenarios combining multiple RSU features."""

    def test_mixed_event_processing_with_aggregation_and_buffering(self):
        processor = EdgeProcessor(
            rsu_id="rsu-1",
            assigned_sector="sector-A12-tunnel",
            aggregation_window_size=2,
            buffer_max_size=10,
        )
        processor.cloud_disconnected = True
        
        emergency = create_event(
            event_id="emg-001",
            source_id="veh-005",
            event_type="emergency_vehicle",
            siren_active=True,
        )
        forwarded, alert = processor.process_event(emergency)
        assert alert is not None
        processor.buffer_event(forwarded)
        
        telemetry1 = create_event(
            event_id="tel-001",
            source_id="veh-001",
            event_type="traffic_telemetry",
            speed_kmh=60,
        )
        forwarded, _ = processor.process_event(telemetry1)
        assert forwarded is None
        
        telemetry2 = create_event(
            event_id="tel-002",
            source_id="veh-002",
            event_type="traffic_telemetry",
            speed_kmh=50,
        )
        forwarded, _ = processor.process_event(telemetry2)
        assert forwarded is not None
        assert forwarded.event_type == "traffic_density"
        processor.buffer_event(forwarded)
        
        assert processor.buffered_count == 2
        assert processor.filtered_telemetry_count == 1
        
        processor.cloud_disconnected = False
        flushed = processor.flush_buffer()
        assert len(flushed) == 2

    def test_sequence_gap_detection_with_lost_packets_estimation(self):
        processor = EdgeProcessor(rsu_id="rsu-1", assigned_sector="sector-A12-tunnel")
        
        for seq_num in [1, 3, 4, 7, 8, 9]:
            event = create_event(
                event_id=f"evt-{seq_num}",
                source_id="veh-001",
                event_type="traffic_telemetry",
                seq_num=seq_num,
                speed_kmh=50,
            )
            processor.process_event(event)
        
        # Gap 1->3: 1 lost (seq 2)
        # Gap 3->4: 0 lost
        # Gap 4->7: 2 lost (seq 5, 6)
        # Gap 7->8: 0 lost
        # Gap 8->9: 0 lost
        # Total: 3 lost
        assert processor.estimated_lost_events == 3
