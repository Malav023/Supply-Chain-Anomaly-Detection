class Shipment {
  static STATUSES = ['pending', 'dispatched', 'in_transit', 'out_for_delivery', 'delivered'];
  static CARRIERS = ['carrier-A', 'carrier-B', 'carrier-C', 'carrier-FastTrack'];
  static ROUTES = ['NYC-BOS', 'CHI-DET', 'LAX-SFO', 'SEA-PDX', 'MIA-ATL'];

  constructor(shipmentId, carrier, route) {
    this.shipmentId = shipmentId;
    this.carrier = carrier || Shipment.CARRIERS[Math.floor(Math.random() * Shipment.CARRIERS.length)];
    this.route = route || Shipment.ROUTES[Math.floor(Math.random() * Shipment.ROUTES.length)];
    this.statusIndex = 0;
  }

  generateEvent() {
    // 20% chance of a delay, 2% chance of a cancellation (rare, non-linear
    // overrides). Otherwise, cycle forward through the normal lifecycle:
    // pending -> dispatched -> in_transit -> out_for_delivery -> delivered.
    const roll = Math.random();
    let status;

    if (roll < 0.02) {
      status = 'cancelled';
    } else if (roll < 0.22) {
      status = 'delayed';
    } else {
      status = Shipment.STATUSES[this.statusIndex % Shipment.STATUSES.length];
      this.statusIndex++;
    }

    // Expected ETA set 4 to 8 hours ahead
    const eta = new Date(Date.now() + (4 + Math.floor(Math.random() * 5)) * 3600 * 1000).toISOString();

    return {
      shipment_id: this.shipmentId,
      carrier: this.carrier,
      status: status,
      expected_eta: eta,
      route: this.route,
      updated_at: new Date().toISOString()
    };
  }
}

module.exports = Shipment;