const { Kafka, Partitioners } = require('kafkajs');
const Shipment = require('./Shipment');

class LogisticsSimulatorService {
  constructor(brokerAddress, topicName, shipments) {
    this.topicName = topicName;
    this.shipments = shipments;
    this.timer = null;

    this.kafka = new Kafka({
      clientId: 'logistics-simulator-client',
      brokers: [brokerAddress],
      retry: { initialRetryTime: 300, retries: 8 }
    });

    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner
    });
  }

  async initialize() {
    console.log('[Logistics Simulator] Connecting to Kafka broker...');
    await this.producer.connect();
    console.log('[Logistics Simulator] Producer connected successfully.');
  }

  async #emitEvent() {
    // Pick one random shipment to update
    const targetShipment = this.shipments[Math.floor(Math.random() * this.shipments.length)];
    const payload = targetShipment.generateEvent();

    try {
      await this.producer.send({
        topic: this.topicName,
        messages: [
          {
            key: payload.shipment_id,
            value: JSON.stringify(payload)
          }
        ]
      });
      console.log(`[${new Date().toISOString()}] Logistics Event [${payload.shipment_id}]: ${payload.status} (${payload.route})`);
    } catch (err) {
      console.error('[Logistics Simulator] Emit error:', err.message);
    }
  }

  start(intervalMs = 15000) {
    console.log(`[Logistics Simulator] Emitting events every ${intervalMs / 1000}s...`);
    // Emit initial event immediately, then set interval
    this.#emitEvent();
    this.timer = setInterval(() => this.#emitEvent(), intervalMs);
  }

  async stop() {
    if (this.timer) clearInterval(this.timer);
    console.log('[Logistics Simulator] Disconnecting producer...');
    await this.producer.disconnect();
    console.log('[Logistics Simulator] Stopped.');
  }
}

async function main() {
  const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
  const TOPIC = 'logistics-events';

  const shipments = [
    new Shipment('ship-88213', 'carrier-A', 'NYC-BOS'),
    new Shipment('ship-90412', 'carrier-B', 'CHI-DET'),
    new Shipment('ship-33019', 'carrier-FastTrack', 'LAX-SFO'),
    new Shipment('ship-55102', 'carrier-C', 'SEA-PDX'),
    new Shipment('ship-77341', 'carrier-A', 'MIA-ATL')
  ];

  const simulator = new LogisticsSimulatorService(KAFKA_BROKER, TOPIC, shipments);
  await simulator.initialize();
  simulator.start(12000); // Emits every 12 seconds

  const shutdown = async () => {
    await simulator.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[Logistics Simulator] Fatal error:', err);
  process.exit(1);
});