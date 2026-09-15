import json
import math
import os
import re
from datetime import datetime, timezone
from typing import Iterator, Optional

from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.time import Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import (
    ProcessFunction,
    KeyedProcessFunction,
    KeyedCoProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.datastream.window import TumblingEventTimeWindows, EventTimeSessionWindows


SENSOR_ID_PATTERN = re.compile(r"^truck-\d{3}$")
SHIPMENT_ID_PATTERN = re.compile(r"^ship-\d+$")
ROUTE_PATTERN = re.compile(r"^[A-Z]{2,4}-[A-Z]{2,4}$")
ISO8601_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")

# Superset enum agreed on: docs updated to match every status either the
# original simulator or data-model.md's original draft ever needed —
# see data-model.md, logistics-events section.
VALID_LOGISTICS_STATUSES = {
    "pending", "dispatched", "in_transit", "out_for_delivery",
    "delayed", "delivered", "cancelled",
}

SENSOR_DEAD_LETTER_TAG = OutputTag("sensor-dead-letter", Types.STRING())
LOGISTICS_DEAD_LETTER_TAG = OutputTag("logistics-dead-letter", Types.STRING())
LATE_DATA_TAG = OutputTag("late-events", Types.STRING())

# Config from environment — see instructions.md §1, "No hardcoded config"
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
TOPIC_SENSOR_RAW = os.environ.get("TOPIC_SENSOR_RAW", "sensor-raw")
TOPIC_SENSOR_DEADLETTER = os.environ.get("TOPIC_SENSOR_DEADLETTER", "sensor-raw-dead-letter")
TOPIC_LOGISTICS_EVENTS = os.environ.get("TOPIC_LOGISTICS_EVENTS", "logistics-events")
TOPIC_LOGISTICS_DEADLETTER = os.environ.get("TOPIC_LOGISTICS_DEADLETTER", "logistics-events-dead-letter")
TOPIC_NORMALIZED_EVENTS = os.environ.get("TOPIC_NORMALIZED_EVENTS", "normalized-events")
TOPIC_LATE_EVENTS = os.environ.get("TOPIC_LATE_EVENTS", "late-events")
DEDUP_STATE_TTL_MS = int(os.environ.get("DEDUP_STATE_TTL_MS", "300000"))
FLINK_ALLOWED_LATENESS_MS = int(os.environ.get("FLINK_ALLOWED_LATENESS_MS", "120000"))
WATERMARK_MAX_OUT_OF_ORDERNESS_MS = int(os.environ.get("WATERMARK_MAX_OUT_OF_ORDERNESS_MS", "2000"))
COLD_CHAIN_WINDOW_SECONDS = int(os.environ.get("COLD_CHAIN_WINDOW_SECONDS", "10"))
GPS_SESSION_GAP_SECONDS = int(os.environ.get("GPS_SESSION_GAP_SECONDS", "5"))

# ASSUMPTION (flagged, not derived from data): static truck-to-shipment
# pairing, since neither sensor-raw nor logistics-events carries a shared
# key. Paired by list order against the fleet/shipment arrays in the
# Phase 1 simulators. Replace with a real dispatch/TMS mapping when
# architecture.md's "planned carrier/freight APIs" integration lands —
# see architecture.md §5 and §6.
TRUCK_TO_SHIPMENT = {
    "truck-001": "ship-88213",
    "truck-002": "ship-90412",
    "truck-003": "ship-33019",
    "truck-004": "ship-55102",
    "truck-005": "ship-77341",
}


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two GPS points, in kilometers."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(d_lon / 2) ** 2
    return r * 2 * math.asin(math.sqrt(a))


def validate_sensor_event(raw: dict) -> Optional[str]:
    """Validates against data-model.md's sensor-raw schema."""
    required = ["sensor_id", "timestamp", "temperature", "humidity", "gps", "vibration"]
    for field in required:
        if field not in raw:
            return f"missing_field:{field}"

    if not isinstance(raw["sensor_id"], str) or not SENSOR_ID_PATTERN.match(raw["sensor_id"]):
        return "invalid_sensor_id"
    if not isinstance(raw["timestamp"], str) or not ISO8601_PATTERN.match(raw["timestamp"]):
        return "invalid_timestamp"
    if not _is_number(raw["temperature"]):
        return "invalid_temperature"
    if not _is_number(raw["humidity"]):
        return "invalid_humidity"

    gps = raw.get("gps")
    if not isinstance(gps, dict) or not _is_number(gps.get("lat")) or not _is_number(gps.get("lon")):
        return "invalid_gps"
    if not _is_number(raw["vibration"]):
        return "invalid_vibration"

    return None


def validate_logistics_event(raw: dict) -> Optional[str]:
    """Validates against data-model.md's logistics-events schema."""
    required = ["shipment_id", "carrier", "status", "expected_eta", "route", "updated_at"]
    for field in required:
        if field not in raw:
            return f"missing_field:{field}"

    if not isinstance(raw["shipment_id"], str) or not SHIPMENT_ID_PATTERN.match(raw["shipment_id"]):
        return "invalid_shipment_id"
    if not isinstance(raw["carrier"], str) or not raw["carrier"]:
        return "invalid_carrier"
    if raw["status"] not in VALID_LOGISTICS_STATUSES:
        return "invalid_status"
    if not isinstance(raw["expected_eta"], str) or not ISO8601_PATTERN.match(raw["expected_eta"]):
        return "invalid_expected_eta"
    if not isinstance(raw["route"], str) or not ROUTE_PATTERN.match(raw["route"]):
        return "invalid_route"
    if not isinstance(raw["updated_at"], str) or not ISO8601_PATTERN.match(raw["updated_at"]):
        return "invalid_updated_at"

    return None


class SensorSchemaValidationFunction(ProcessFunction):
    """
    Validates sensor-raw events; dead-letters malformed ones (instructions.md).

    NOTE: PyFlink's ProcessFunction.Context has no .output() method (that's
    Java-API only). Side outputs in PyFlink are emitted by yielding a
    (output_tag, value) tuple from this generator, same as a normal yield.
    """

    def process_element(self, value: str, ctx: "ProcessFunction.Context") -> Iterator:
        try:
            raw = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            yield SENSOR_DEAD_LETTER_TAG, json.dumps({"reason": "invalid_json", "raw": value})
            return

        reason = validate_sensor_event(raw)
        if reason is not None:
            yield SENSOR_DEAD_LETTER_TAG, json.dumps({"reason": reason, "raw": raw})
            return

        yield json.dumps(raw)


class LogisticsSchemaValidationFunction(ProcessFunction):
    """Validates logistics-events; dead-letters malformed ones (instructions.md)."""

    def process_element(self, value: str, ctx: "ProcessFunction.Context") -> Iterator:
        try:
            raw = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            yield LOGISTICS_DEAD_LETTER_TAG, json.dumps({"reason": "invalid_json", "raw": value})
            return

        reason = validate_logistics_event(raw)
        if reason is not None:
            yield LOGISTICS_DEAD_LETTER_TAG, json.dumps({"reason": reason, "raw": raw})
            return

        yield json.dumps(raw)


class DedupFunction(KeyedProcessFunction):
    """Keyed by sensor_id. Drops exact-duplicate (same timestamp) re-deliveries."""

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("last_seen_timestamp", Types.STRING())
        ttl_config = (
            StateTtlConfig.new_builder(Time.milliseconds(DEDUP_STATE_TTL_MS))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        descriptor.enable_time_to_live(ttl_config)
        self.last_seen_state = runtime_context.get_state(descriptor)

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context") -> Iterator[str]:
        event = json.loads(value)
        incoming_ts = event["timestamp"]
        last_ts = self.last_seen_state.value()

        if last_ts is not None and last_ts == incoming_ts:
            return

        self.last_seen_state.update(incoming_ts)
        yield value


class RemapToShipmentFunction(ProcessFunction):
    """
    Attaches shipment_id to each sensor event via the static TRUCK_TO_SHIPMENT
    map, so the stream can be re-keyed by shipment_id for the join. Sensors
    with no known mapping are dead-lettered rather than dropped silently.
    """

    def process_element(self, value: str, ctx: "ProcessFunction.Context") -> Iterator:
        event = json.loads(value)
        shipment_id = TRUCK_TO_SHIPMENT.get(event["sensor_id"])

        if shipment_id is None:
            yield SENSOR_DEAD_LETTER_TAG, json.dumps({"reason": "unmapped_shipment", "raw": event})
            return

        event["shipment_id"] = shipment_id
        yield json.dumps(event)


class SensorLogisticsJoinFunction(KeyedCoProcessFunction):
    """
    Stream-table join, keyed by shipment_id (architecture.md §4).
    process_element1 = logistics-events: updates the reference "table"
    state (route, carrier, status, expected_eta) for this shipment.
    process_element2 = sensor readings: enriched with the latest known
    shipment reference data and emitted.
    """

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("shipment_reference", Types.STRING())
        self.shipment_state = runtime_context.get_state(descriptor)

    def process_element1(self, value: str, ctx: "KeyedCoProcessFunction.Context") -> Iterator[str]:
        self.shipment_state.update(value)
        return
        yield  # pragma: no cover — makes this a generator, emits nothing

    def process_element2(self, value: str, ctx: "KeyedCoProcessFunction.Context") -> Iterator[str]:
        sensor_event = json.loads(value)
        reference_raw = self.shipment_state.value()

        if reference_raw is None:
            enriched = {
                **sensor_event,
                "carrier": None,
                "route": None,
                "status": None,
                "expected_eta": None,
                "enriched": False,
            }
        else:
            reference = json.loads(reference_raw)
            enriched = {
                **sensor_event,
                "carrier": reference["carrier"],
                "route": reference["route"],
                "status": reference["status"],
                "expected_eta": reference["expected_eta"],
                "enriched": True,
            }

        yield json.dumps(enriched)


class EnrichedTimestampAssigner(TimestampAssigner):
    """
    Extracts event time from the enriched record's original sensor
    'timestamp' field (ISO8601, always millisecond-precision from
    TruckSensor's toISOString() output), for use by the watermark
    strategy driving window Step 5.
    """

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        event = json.loads(value)
        dt = datetime.strptime(event["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


class ColdChainWindowFunction(ProcessWindowFunction):
    """
    Tumbling 10s window per sensor_id (architecture.md's "fixed sensor"
    style aggregation), summarizing temperature/humidity/vibration —
    the cold-chain telemetry fields, as opposed to positional GPS data.
    """

    def process(self, key: str, context: "ProcessWindowFunction.Context", elements) -> Iterator[str]:
        temps, humidities, vibrations = [], [], []
        shipment_id = carrier = route = status = expected_eta = None
        last_ts = None

        for value in elements:
            event = json.loads(value)
            temps.append(event["temperature"])
            humidities.append(event["humidity"])
            vibrations.append(event["vibration"])
            shipment_id = event.get("shipment_id")
            carrier = event.get("carrier")
            route = event.get("route")
            status = event.get("status")
            expected_eta = event.get("expected_eta")
            last_ts = event["timestamp"]

        if not temps:
            return

        window = context.window()
        summary = {
            "window_type": "cold_chain_tumbling",
            "sensor_id": key,
            "shipment_id": shipment_id,
            "carrier": carrier,
            "route": route,
            "status": status,
            "expected_eta": expected_eta,
            "window_start": window.start,
            "window_end": window.end,
            "reading_count": len(temps),
            "avg_temperature": round(sum(temps) / len(temps), 3),
            "min_temperature": round(min(temps), 3),
            "max_temperature": round(max(temps), 3),
            "avg_humidity": round(sum(humidities) / len(humidities), 3),
            "avg_vibration": round(sum(vibrations) / len(vibrations), 3),
            "max_vibration": round(max(vibrations), 3),
            "last_event_timestamp": last_ts,
            "is_late": False,
        }
        yield json.dumps(summary)


class GpsSessionWindowFunction(ProcessWindowFunction):
    """
    Session window (5s inactivity gap) per sensor_id (architecture.md's
    "GPS/mobile sensor" style aggregation), summarizing a continuous
    movement segment into a start/end point pair and total distance.
    """

    def process(self, key: str, context: "ProcessWindowFunction.Context", elements) -> Iterator[str]:
        points = []
        shipment_id = carrier = route = None

        for value in elements:
            event = json.loads(value)
            points.append({
                "lat": event["gps"]["lat"],
                "lon": event["gps"]["lon"],
                "timestamp": event["timestamp"],
            })
            shipment_id = event.get("shipment_id")
            carrier = event.get("carrier")
            route = event.get("route")

        if not points:
            return

        points.sort(key=lambda p: p["timestamp"])
        distance_km = 0.0
        for i in range(1, len(points)):
            distance_km += _haversine_km(
                points[i - 1]["lat"], points[i - 1]["lon"],
                points[i]["lat"], points[i]["lon"],
            )

        window = context.window()
        summary = {
            "window_type": "gps_session",
            "sensor_id": key,
            "shipment_id": shipment_id,
            "carrier": carrier,
            "route": route,
            "window_start": window.start,
            "window_end": window.end,
            "point_count": len(points),
            "start_point": points[0],
            "end_point": points[-1],
            "distance_km": round(distance_km, 4),
            "is_late": False,
        }
        yield json.dumps(summary)


def _wrap_late_event(window_type: str, raw_value: str) -> str:
    """Wraps a raw enriched event that missed its window (see
    instructions.md, Error Handling Strategy — Late data)."""
    return json.dumps({
        "reason": "late_arrival",
        "window_type": window_type,
        "is_late": True,
        "raw": json.loads(raw_value),
    })


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # revisit once ML scoring (Phase 3) is added

    sensor_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(TOPIC_SENSOR_RAW)
        .set_group_id("normalize-enrich-sensor")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    logistics_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(TOPIC_LOGISTICS_EVENTS)
        .set_group_id("normalize-enrich-logistics")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sensor_stream = env.from_source(sensor_source, WatermarkStrategy.no_watermarks(), "sensor-raw-source")
    logistics_stream = env.from_source(
        logistics_source, WatermarkStrategy.no_watermarks(), "logistics-events-source"
    )

    # --- Sensor side: validate -> dead-letter sink -> dedup -> remap to shipment_id ---
    sensor_validated = sensor_stream.process(SensorSchemaValidationFunction(), Types.STRING())
    sensor_dead_letter = sensor_validated.get_side_output(SENSOR_DEAD_LETTER_TAG)

    sensor_deduped = (
        sensor_validated
        .key_by(lambda event: json.loads(event)["sensor_id"], key_type=Types.STRING())
        .process(DedupFunction(), Types.STRING())
    )

    sensor_remapped = sensor_deduped.process(RemapToShipmentFunction(), Types.STRING())
    sensor_remap_dead_letter = sensor_remapped.get_side_output(SENSOR_DEAD_LETTER_TAG)

    # --- Logistics side: validate -> dead-letter sink ---
    logistics_validated = logistics_stream.process(LogisticsSchemaValidationFunction(), Types.STRING())
    logistics_dead_letter = logistics_validated.get_side_output(LOGISTICS_DEAD_LETTER_TAG)

    # --- Dead-letter sinks ---
    sensor_dead_letter_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_SENSOR_DEADLETTER)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    sensor_dead_letter.sink_to(sensor_dead_letter_sink)
    sensor_remap_dead_letter.sink_to(sensor_dead_letter_sink)

    logistics_dead_letter_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_LOGISTICS_DEADLETTER)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    logistics_dead_letter.sink_to(logistics_dead_letter_sink)

    # --- Stream-table join, keyed by shipment_id ---
    joined = (
        logistics_validated.connect(sensor_remapped)
        .key_by(
            lambda event: json.loads(event)["shipment_id"],
            lambda event: json.loads(event)["shipment_id"],
        )
        .process(SensorLogisticsJoinFunction(), Types.STRING())
    )

    # --- Step 5: assign event-time watermarks on the enriched stream ---
    # Bounded-out-of-orderness is deliberately small (default 2s) since
    # the simulators emit in near-order over a local Docker network;
    # this is distinct from FLINK_ALLOWED_LATENESS_MS, which controls
    # how long a *window* stays open for stragglers, not watermark drift.
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_millis(WATERMARK_MAX_OUT_OF_ORDERNESS_MS))
        .with_timestamp_assigner(EnrichedTimestampAssigner())
    )
    enriched_with_watermarks = joined.assign_timestamps_and_watermarks(watermark_strategy)

    # --- Branch A: cold-chain tumbling window (temperature/humidity/vibration) ---
    cold_chain_windowed = (
        enriched_with_watermarks
        .key_by(lambda event: json.loads(event)["sensor_id"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(COLD_CHAIN_WINDOW_SECONDS)))
        .allowed_lateness(FLINK_ALLOWED_LATENESS_MS)
        .side_output_late_data(LATE_DATA_TAG)
        .process(ColdChainWindowFunction(), Types.STRING())
    )
    cold_chain_late = cold_chain_windowed.get_side_output(LATE_DATA_TAG).map(
        lambda raw: _wrap_late_event("cold_chain_tumbling", raw), output_type=Types.STRING()
    )

    # --- Branch B: GPS session window (movement segments) ---
    gps_windowed = (
        enriched_with_watermarks
        .key_by(lambda event: json.loads(event)["sensor_id"], key_type=Types.STRING())
        .window(EventTimeSessionWindows.with_gap(Time.seconds(GPS_SESSION_GAP_SECONDS)))
        .allowed_lateness(FLINK_ALLOWED_LATENESS_MS)
        .side_output_late_data(LATE_DATA_TAG)
        .process(GpsSessionWindowFunction(), Types.STRING())
    )
    gps_late = gps_windowed.get_side_output(LATE_DATA_TAG).map(
        lambda raw: _wrap_late_event("gps_session", raw), output_type=Types.STRING()
    )

    # --- Sinks: normalized-events (both window shapes) and late-events ---
    normalized_events_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_NORMALIZED_EVENTS)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    cold_chain_windowed.sink_to(normalized_events_sink)
    gps_windowed.sink_to(normalized_events_sink)

    late_events_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_LATE_EVENTS)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    cold_chain_late.sink_to(late_events_sink)
    gps_late.sink_to(late_events_sink)

    # Temporary: print windowed output so we can visually confirm both
    # window types populate correctly before moving to Phase 3 (ML scoring).
    cold_chain_windowed.print()
    gps_windowed.print()

    env.execute("normalize-enrich-phase2-full-pipeline")


if __name__ == "__main__":
    main()