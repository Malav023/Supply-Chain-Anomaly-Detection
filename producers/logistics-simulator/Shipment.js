class Shipment {
  static STATUSES = ['dispatched', 'in_transit', 'out_for_delivery', 'delayed', 'delivered'];
  static CARRIERS = ['carrier-A', 'carrier-B', 'carrier-C', 'carrier-FastTrack'];
  static ROUTES = ['NYC-BOS', 'CHI-DET', 'LAX-SFO', 'SEA-PDX', 'MIA-ATL'];

  constructor(shipmentId, carrier, route) {
    this.shipmentId = shipmentId;
    this.carrier = carrier || Shipment.CARRIERS[Math.floor(Math.random() * Shipment.CARRIERS.length)];
    this.route = route || Shipment.ROUTES[Math.floor(Math.random() * Shipment.ROUTES.length)];
    this.statusIndex = 0;
  }

  generateEvent() {
    // Advance status or pick a random active status
    const isDelayed = Math.random() < 0.2;
    let status;

    if (isDelayed) {
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