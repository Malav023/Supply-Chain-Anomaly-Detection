const {kafka, Partitioners}=require('kafkajs');
const Trucksensor=require('./Trucksensor');

class FleetSimulatorService{
	constructor(brokerAddress, topicName, fleet){
		this.topicName=topicName; 
		this.fleet=fleet; 
		this.timer=null; 

		this.kafka=new Kafka({
			clientId:'sensor-simulator-client',
			brokers:[brokerAddress];
			retry:{initialRetryTime:300, retries:8}
		});


		this.producer=this.kafka.producer({
			createPartitioner:Partitioners.DefaultPartitioner
		});
	}

	async initialize(){
		console.log('[Sensor Simulator] Connecting to kafka broker...');
		await this.producer.connect(); 
		console.log('[Sensor Simulator] connected succesfully.');
	}

	async #emitBatch(){
		const messages=this.fleet.map((sensor)=>{
			const payload=sensor.generateReading(); 
			return{
				key:payload.sensor_id, 
				value:JSON.stringify(payload)
			};
		});

		try
	}
}