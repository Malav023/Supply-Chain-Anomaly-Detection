class TruckSensor {
  constructor(sensorId, initialLat, initialLon, baseTemp = 4.0, baseHumidity = 60.0) {
    this.sensorId = sensorId;
    this.lat = initialLat;
    this.lon = initialLon;
    this.temperature = baseTemp;
    this.humidity = baseHumidity;
  }

  #randomDelta(min, max) {
    return Math.random() * (max - min) + min;
  }

  generateReading() {
    this.temperature = +(this.temperature + this.#randomDelta(-0.2, 0.2)).toFixed(2);
    this.humidity = Math.min(100, Math.max(0, +(this.humidity + this.#randomDelta(-0.5, 0.5)).toFixed(1)));
    this.lat = +(this.lat + this.#randomDelta(-0.0005, 0.0005)).toFixed(4);
    this.lon = +(this.lon + this.#randomDelta(-0.0005, 0.0005)).toFixed(4);
    const vibration = +(Math.random() * 0.08).toFixed(3);

    return {
      sensor_id: this.sensorId,
      timestamp: new Date().toISOString(),
      temperature: this.temperature,
      humidity: this.humidity,
      gps: {
        lat: this.lat,
        lon: this.lon
      },
      vibration: vibration
    };
  }
}

module.exports = TruckSensor;