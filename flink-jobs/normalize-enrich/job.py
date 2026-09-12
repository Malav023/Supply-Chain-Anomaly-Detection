import json
import os
import re
from typing import Iterator, Optional

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.serialization import SimpleStringSchema
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
)
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig


SENSOR_ID_PATTERN = re.compile(r"^truck-\d{3}$")
SHIPMENT_ID_PATTERN = re.compile(r"^ship-\d+$")
ROUTE_PATTERN = re.compile(r"^[A-Z]{2,4}-[A-Z]{2,4}$")
ISO8601_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")

VALID_LOGISTICS_STATUSES = {"pending", "in_transit", "delayed", "delivered", "cancelled"}

SENSOR_DEAD_LETTER_TAG = OutputTag("sensor-dead-letter", Types.STRING())
LOGISTICS_DEAD_LETTER_TAG = OutputTag("logistics-dead-letter", Types.STRING())

# Config from environment — see instructions.md §1, "No hardcoded config"
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
TOPIC_SENSOR_RAW = os.environ.get("TOPIC_SENSOR_RAW", "sensor-raw")
TOPIC_SENSOR_DEADLETTER = os.environ.get("TOPIC_SENSOR_DEADLETTER", "sensor-raw-dead-letter")
TOPIC_LOGISTICS_EVENTS = os.environ.get("TOPIC_LOGISTICS_EVENTS", "logistics-events")
TOPIC_LOGISTICS_DEADLETTER = os.environ.get("TOPIC_LOGISTICS_DEADLETTER", "logistics-events-dead-letter")
DEDUP_STATE_TTL_MS = int(os.environ.get("DEDUP_STATE_TTL_MS", "300000"))

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

    If no logistics data has arrived yet for a shipment_id, the sensor
    event is still emitted (not dropped — instructions.md's general rule
    is fail gracefully, not silently discard) with enrichment fields null
    and enriched=false.
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


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # revisit once windowing is added

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

    # Temporary: print enriched events so we can confirm the join end-to-end
    # before wiring windowing (next step) and the final normalized-events sink.
    joined.print()

    env.execute("normalize-enrich-sensor-logistics-join")


if __name__ == "__main__":
    main()