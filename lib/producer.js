"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_rdkafka_1 = __importDefault(require("node-rdkafka"));
const client_1 = __importDefault(require("./client"));
let _kafkaProducer = null;
class KafkaProducer extends client_1.default {
    config;
    topicConfig;
    producer;
    /**
     * Initializes a KafkaProducer.
     * @param {String} clientId: id to identify a client producing the message.
     * @param {import('node-rdkafka').ProducerGlobalConfig} config: configs for producer.
     * @param {import('node-rdkafka').ProducerTopicConfig} topicConfig: topic configs.
     * @param {EventEmitter} emitter: to emit log messages
     */
    constructor(clientId, config, topicConfig, emitter) {
        // producer config defaults should go here.
        config = Object.assign({
            'retry.backoff.ms': 200,
            'message.send.max.retries': 10,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.ms': 1000,
            'batch.num.messages': 1000000,
            'dr_cb': true
        }, config);
        // producer topic config defaults should go here.
        topicConfig = Object.assign({ 'acks': 1 }, topicConfig);
        super(clientId, 'producer', config, topicConfig, emitter);
        this.config = config;
        this.topicConfig = topicConfig;
        this.producer = new node_rdkafka_1.default.Producer(this.config, this.topicConfig);
    }
    /**
     * Asynchronous function which connects to kafka cluster.
     * Resolves when connection is ready.
     *
     * @returns {Promise}
     */
    connect() {
        return new Promise((resolve, reject) => {
            try {
                this.producer
                    .connect()
                    .on('ready', (info, metadata) => {
                    this.success('Producer connected to kafka cluster...', {
                        name: info.name,
                    });
                    // set automating polling to every second for delivery reports
                    this.producer.setPollInterval(1000);
                    resolve(this);
                })
                    .on('delivery-report', (err, report) => {
                    if (err) {
                        this.error('Error producing message: ', err);
                    }
                    else {
                        this.log(`Produced event: key=${report.key}, timestamp=${report.timestamp}.`);
                    }
                })
                    .on('event.error', (err) => {
                    this.error('Producer encountered error: ', err);
                    reject(err);
                })
                    .on('event.log', (eventData) => this.log('Logging consumer event: ', eventData))
                    .on('disconnected', (metrics) => {
                    this.log('Producer disconnected. Client metrics are: ', metrics.connectionOpened);
                });
            }
            catch (err) {
                this.error('Producer encountered while connecting to kafka server.', err);
                reject(err);
            }
        });
    }
    /**
     * Produce a message to a topic-partition.
     * @param {String} topic: name of topic
     * @param {import('node-rdkafka').NumberNullUndefined} partition: partition number to produce to.
     * @param {any} message: message to be produced.
     * @param {import('node-rdkafka').MessageKey} key: key associated with the message.
     * @param {import('node-rdkafka').NumberNullUndefined} timestamp: timestamp to send with the message.
     * @returns {import('../types').BooleanOrNumber}: returns boolean or librdkafka error code.
     */
    produce({ topic, message, partition = null, key = null, timestamp = null }) {
        try {
            const stringifiedMsg = JSON.stringify(message);
            const isSuccess = this.producer.produce(topic, partition, Buffer.from(stringifiedMsg), key, timestamp, null);
            return isSuccess;
        }
        catch (err) {
            this.error(`Producer encountered error while producing message to topic=${topic}, partition=${partition} with key=${key}`, err);
            return false;
        }
    }
    /**
     * Flush everything on the internal librdkafka buffer.
     * Good to perform before disconnect.
     * @param {import('node-rdkafka').NumberNullUndefined}} timeout
     * @param {import('../types').ErrorHandlingFunction} postFlushAction
     * @returns {KafkaProducer}
     */
    flush(timeout, postFlushAction) {
        try {
            this.producer.flush(timeout, postFlushAction);
        }
        catch (err) {
            this.error('Producer encountered error while flusing events.', err);
        }
        return this;
    }
    /**
     * Disconnects producer.
     * @param {import('../types').DisconnectFunction} postDisconnectAction
     * @returns {KafkaProducer}
     */
    disconnect(postDisconnectAction) {
        try {
            this.producer.disconnect(postDisconnectAction);
        }
        catch (err) {
            this.error('Producer encountered error while disconnecting.', err);
        }
        return this;
    }
}
function getKafkaProducer(clientId, config, topicConfig, emitter) {
    if (!_kafkaProducer) {
        _kafkaProducer = new KafkaProducer(clientId, config, topicConfig, emitter);
    }
    return _kafkaProducer;
}
exports.default = getKafkaProducer;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicHJvZHVjZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvcHJvZHVjZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFDQSxnRUFBaUo7QUFDakosc0RBQXlEO0FBVXpELElBQUksY0FBYyxHQUFrQixJQUFJLENBQUM7QUFFekMsTUFBTSxhQUFjLFNBQVEsZ0JBQU07SUFVUTtJQUFzQztJQVRwRSxRQUFRLENBQWlCO0lBRWpDOzs7Ozs7T0FNRztJQUNILFlBQVksUUFBZ0IsRUFBVSxNQUE0QixFQUFVLFdBQWdDLEVBQUUsT0FBcUI7UUFDL0gsMkNBQTJDO1FBQzNDLE1BQU0sR0FBRyxNQUFNLENBQUMsTUFBTSxDQUFDO1lBQ25CLGtCQUFrQixFQUFFLEdBQUc7WUFDdkIsMEJBQTBCLEVBQUUsRUFBRTtZQUM5Qiw4QkFBOEIsRUFBRSxNQUFNO1lBQ3RDLHdCQUF3QixFQUFFLElBQUk7WUFDOUIsb0JBQW9CLEVBQUUsT0FBTztZQUM3QixPQUFPLEVBQUUsSUFBSTtTQUNkLEVBQ0QsTUFBTSxDQUNQLENBQUM7UUFDRixpREFBaUQ7UUFDakQsV0FBVyxHQUFHLE1BQU0sQ0FBQyxNQUFNLENBQUMsRUFBRSxNQUFNLEVBQUcsQ0FBQyxFQUFFLEVBQUUsV0FBVyxDQUFDLENBQUM7UUFFekQsS0FBSyxDQUFDLFFBQVEsRUFBRSxVQUFVLEVBQUUsTUFBTSxFQUFFLFdBQVcsRUFBRSxPQUFPLENBQUMsQ0FBQztRQWZ4QixXQUFNLEdBQU4sTUFBTSxDQUFzQjtRQUFVLGdCQUFXLEdBQVgsV0FBVyxDQUFxQjtRQWdCeEcsSUFBSSxDQUFDLFFBQVEsR0FBRyxJQUFJLHNCQUFLLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxNQUFNLEVBQUUsSUFBSSxDQUFDLFdBQVcsQ0FBQyxDQUFDO0lBQ3RFLENBQUM7SUFFRDs7Ozs7T0FLRztJQUNILE9BQU87UUFDSCxPQUFPLElBQUksT0FBTyxDQUFDLENBQUMsT0FBTyxFQUFFLE1BQU0sRUFBRSxF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