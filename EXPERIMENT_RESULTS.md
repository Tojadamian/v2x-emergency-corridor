# Controlled Experiment Results

## Methodology

Execution target: local isolated in-process runner. Each run uses fresh, isolated in-memory state, two real edge processors, simulator-generated events, and the cloud rule engine. Random seeds and workloads are fixed; elapsed time, memory and generated alert IDs are not deterministic.

Delivery trials use 100 critical hazard events so aggregation cannot hide loss. The real anomaly injector applies independently seeded controls; percentages are probabilities, not exact counts. Delay uses virtual sleep and is added to observed processing latency; it is not a network or wall-clock delay. Reordering holds events per vehicle.

Disconnection buffers forwarded events and measures flush plus cloud processing. Component failure pauses a simulated FIFO queue, injects duplicate message deliveries, then uses the real SQS worker's message handler; it does not stop a deployed process or model SQS timing or ordering guarantees.

Load sends generated events in bursts to both RSUs, queues forwarded events per burst, then drains into the cloud. Throughput is input events divided by elapsed wall time; latency is generation-to-cloud completion for forwarded events only, not filtered events. Bandwidth savings are the reduction in event count, not measured bytes. Memory is the increase in process peak RSS, so zero does not mean no allocation. Queue depth is the in-process burst queue, not an AWS queue. Timing and memory vary by host.

Final-state consistency compares latest event IDs by source with the generated input for delivery trials, and with unique queued events for worker trials. Sequence gaps are estimates and can overcount reordered or late packets; tail losses cannot be detected from a subsequent sequence number.

## Experiment 1: Delivery Anomalies

| condition | level | controls | generated_events | accepted_by_rsu | forwarded_to_cloud | processed_by_cloud | estimated_lost_events | duplicates_ignored | reordered_events | p50_latency_ms | p95_latency_ms | final_state_consistent |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | baseline | {} | 100 | 100 | 100 | 100 | 0 | 0 | 0 | 0.113 | 0.205 | True |
| delay | level_1 | {"delay_ms": 50} | 100 | 100 | 100 | 100 | 0 | 0 | 0 | 50.12 | 50.203 | True |
| delay | level_2 | {"delay_ms": 250} | 100 | 100 | 100 | 100 | 0 | 0 | 0 | 250.118 | 250.208 | True |
| loss | level_1 | {"loss_rate": 0.1} | 100 | 90 | 90 | 90 | 10 | 0 | 0 | 0.107 | 0.197 | True |
| loss | level_2 | {"loss_rate": 0.3} | 100 | 68 | 68 | 68 | 27 | 0 | 0 | 0.087 | 0.149 | False |
| duplication | level_1 | {"duplicate_rate": 0.15} | 100 | 100 | 100 | 100 | 0 | 13 | 0 | 0.122 | 0.213 | True |
| duplication | level_2 | {"duplicate_rate": 0.4} | 100 | 100 | 100 | 100 | 0 | 36 | 0 | 0.12 | 0.213 | True |
| reordering | level_1 | {"reorder_rate": 0.15} | 100 | 100 | 100 | 100 | 21 | 0 | 12 | 0.131 | 0.952 | True |
| reordering | level_2 | {"reorder_rate": 0.4} | 100 | 100 | 100 | 100 | 61 | 0 | 33 | 0.165 | 1.141 | True |

## Experiment 2: Edge-Cloud Disconnection

| scenario | buffer_max_size | events_sent_during_outage | local_alerts_during_outage | local_alert_types | buffered_events | rejected_events | flushed_events | data_loss_count | order_preserved | recovery_time_ms | final_cloud_processed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A | 1000 | 40 | 40 | ["LOCAL_COLLISION_RISK_ALERT", "LOCAL_EMERGENCY_CORRIDOR_ALERT"] | 40 | 0 | 40 | 0 | True | 2.13 | 40 |
| B | 15 | 40 | 40 | ["LOCAL_COLLISION_RISK_ALERT", "LOCAL_EMERGENCY_CORRIDOR_ALERT"] | 15 | 25 | 15 | 25 | True | 0.451 | 15 |

## Experiment 3: Component Failure

| level | queue_peak_depth | unprocessed_during_outage | retry_deliveries_injected | duplicates_ignored | processed_unique_events | recovery_time_ms | final_state_consistent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| level_1 | 30 | 25 | 5 | 5 | 25 | 1.11 | True |
| level_2 | 90 | 75 | 15 | 15 | 75 | 6.801 | True |

## Experiment 4: Load Increase

| level | vehicles | burst_size | total_events | rsu_filtered_telemetry | bandwidth_saved_pct | cloud_processed_events | throughput_events_per_sec | p50_latency_ms | p95_latency_ms | max_queue_depth | error_rate_pct | memory_delta_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| level_1 | 5 | 1 | 100 | 65 | 65.0 | 35 | 34901.98 | 0.06 | 0.093 | 1 | 0.0 | 0 |
| level_2 | 15 | 3 | 300 | 194 | 64.67 | 106 | 18066.62 | 0.162 | 0.454 | 3 | 0.0 | 240.0 |
| level_3 | 30 | 6 | 600 | 399 | 66.5 | 201 | 10635.41 | 0.348 | 0.968 | 5 | 0.0 | 624.0 |

## Engineering Conclusions

The higher loss trial accepted 68 of 100 events and reported 27 estimated gaps. The higher reordering trial reported 33 reordered events and 61 estimated gaps despite no injected loss. Gap estimates therefore cannot be treated as exact loss counts.

With capacities 1000 and 15, the edge retained 40 and 15 events respectively, rejecting 0 and 25. Local safety alerts remained available during the partition, but bounded queues trade availability for eventual data loss.

On restart, the worker ignored 5 and 15 retry deliveries while processing 25 and 75 unique events. Idempotency suppresses repeated deliveries, while timestamp-based state selection mitigates reordering; neither recovers lost data.

At 100 and 600 input events, edge filtering reduced forwarded event counts by 65.0% and 66.5%. Aggregation reduces cloud traffic at the cost of individual telemetry visibility; local measurements do not establish production capacity.
