const Kafka = require('node-rdkafka');
const Client = require('./client');

class KafkaProducer extends Client {

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
          }, 
          config
        );
        // producer topic config defaults should go here.
        topicConfig = Object.assign({ 'acks' : 1 }, topicConfig);

        super(clientId, 'producer', config, topicConfig, emitter);
        this.producer = new Kafka.Producer(this.config, this.topicConfig);
    }

    /**
     * Asynchronous function which connects to kafka cluster. 
     * Resolves when connection is ready.
     *
     * @returns {Promise} 
     */
    connect() {
        this.producer
        .on('ready', (info, metadata) => {
            this.success('Successfully connected to kafka.', {
                name: info.name,
                metadata: {
                    orig_broker_id: metadata.orig_broker_id,
                    orig_broker_name: metadata.orig_broker_name,
                    brokers: metadata.brokers
                }
            });
        })
        .on('connection.failure', (err, metrics) => {
            this.error(`Encountered connection failure with kafka. Client metrics: ${metrics.connectionOpened}`, JSON.stringify(err));
        })
        .on('delivery-report', (err, report) => {
            // not logging successful delivery reports to not bombard log collector with too many messages
            if (err) {
                this.error(`Error while producing the message: ${err}`, {
                    message: report.value,
                    timestamp: report.timestamp,
                    key: report.key,
                    topic: report.topic,
                    partition: report.partition,
                    offset: report.offset,
                });
            } 
        })
        .on('event.error', (err) => {
            this.error('Producer encountered error: ', err);
        })
        .on('disconnected', (metrics) => {
            this.log(`Disconnected from kafka. Client metrics are:  ${metrics.connectionOpened}`);
        });
        // set automating polling to every second for delivery reports
        this.producer.setPollInterval(1000);
        return new Promise((resolve, reject) => {
                this.producer.connect({}, (err, data) => {
                    if (err) {
                        this.error('Encountered error while connecting to kafka.');
                        return reject(err);
                    }
                    resolve(data);
                });
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
        } catch (err) {
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
        } catch (err) {
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
        } catch (err) {
            this.error('Producer encountered error while disconnecting.', err);
        }
        return this;
    }
}

module.exports = KafkaProducer;