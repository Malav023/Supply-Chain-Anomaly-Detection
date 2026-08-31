const { Kafka, Partitioners } = require('kafkajs');
const TruckSensor = require('./TruckSensor');

class FleetSimulatorService {
  constructor(brokerAddress, topicName, fleet) {
    this.topicName = topicName;
    this.fleet = fleet;
    this.timer = null;

    this.kafka = new Kafka({
      clientId: 'sensor-simulator-client',
      brokers: [brokerAddress],
      retry: { initialRetryTime: 300, retries: 8 }
    });

    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner
    });
  }

  async initialize() {
    console.log('[Sensor Simulator] Connecting to Kafka broker...');
    await this.producer.connect();
    console.log('[Sensor Simulator] Connected successfully.');
  }

  async #emitBatch() {
    const messages = this.fleet.map((sensor) => {
      const payload = sensor.generateReading();
      return {
        key: payload.sensor_id,
        value: JSON.stringify(payload)
      };
    });

    try {
      await this.producer.send({
        topic: this.topicName,
        messages
      });
      console.log(`[${new Date().toISOString()}] Emitted ${messages.length} sensor readings to ${this.topicName}`);
    } catch (err) {
      console.error('[Sensor Simulator] Emit error:', err.message);
    }
  }

  start(intervalMs = 1000) {
    console.log(`[Sensor Simulator] Emitting telemetry every ${intervalMs}ms...`);
    this.timer = setInterval(() => this.#emitBatch(), intervalMs);
  }

  async stop() {
    if (this.timer) clearInterval(this.timer);
    console.log('[Sensor Simulator] Disconnecting producer...');
    await this.producer.disconnect();
    console.log('[Sensor Simulator] Stopped.');
  }
}

async function main() {
  const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
  const TOPIC = 'sensor-raw';

  const fleet = [
    new TruckSensor('truck-001', 40.7128, -74.0060, 4.2, 62.0),
    new TruckSensor('truck-002', 34.0522, -118.2437, 2.8, 58.0),
    new TruckSensor('truck-003', 41.8781, -87.6298, -1.5, 70.0),
    new TruckSensor('truck-004', 29.7604, -95.3698, 8.4, 80.0),
    new TruckSensor('truck-005', 47.6062, -122.3321, 3.1, 65.0)
  ];

  const simulator = new FleetSimulatorService(KAFKA_BROKER, TOPIC, fleet);
  await simulator.initialize();
  simulator.start(1000);

  const shutdown = async () => {
    await simulator.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[Sensor Simulator] Fatal error:', err);
  process.exit(1);
});