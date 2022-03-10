import EventEmitter from 'events';
import Kafka, { ClientMetrics, LibrdKafkaError, MessageKey, NumberNullUndefined, ProducerGlobalConfig, ProducerTopicConfig } from 'node-rdkafka';
import Client, { ErrorHandlingFunction } from './client';

interface ProduceParameters{
    topic: string;
    message: any;
    partition?: NumberNullUndefined;
    key?: MessageKey;
    timestamp?: NumberNullUndefined;
}

let _kafkaProducer: KafkaProducer = null;

class KafkaProducer extends Client {
    private producer: Kafka.Producer;

    /**
     * Initializes a KafkaProducer.
     * @param {String} clientId: id to identify a client producing the message.
     * @param {import('node-rdkafka').ProducerGlobalConfig} config: configs for producer.
     * @param {import('node-rdkafka').ProducerTopicConfig} topicConfig: topic configs.
     * @param {EventEmitter} emitter: to emit log messages
     */
    constructor(clientId: string, private config: ProducerGlobalConfig, private topicConfig: ProducerTopicConfig, emitter: EventEmitter) {
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
    connect(): Promise<this | LibrdKafkaError> {
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
                    } else {
                        this.log(`Produced event: key=${report.key}, timestamp=${report.timestamp}.`);
                    }
                })
                .on('event.error', (err) => {
                    this.error('Producer encountered error: ', err);
                    reject(err);
                })
                .on('event.log',  (eventData) => this.log('Logging consumer event: ', eventData))
                .on('disconnected', (metrics) => {
                    this.log('Producer disconnected. Client metrics are: ', metrics.connectionOpened);
                });
            } catch (err) {
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
    produce({ topic, message, partition = null, key = null, timestamp = null }: ProduceParameters): boolean | number {
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
    flush(timeout?: NumberNullUndefined, postFlushAction?: ErrorHandlingFunction): this {
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
    disconnect(postDisconnectAction?: (err: any, data: ClientMetrics) => any): this {
        try {
            this.producer.disconnect(postDisconnectAction);
        } catch (err) {
            this.error('Producer encountered error while disconnecting.', err);
        }
        return this;
    }
}

function getKafkaProducer(clientId: string, config: ProducerGlobalConfig, topicConfig: ProducerTopicConfig, emitter: EventEmitter): KafkaProducer {
    if (!_kafkaProducer) {
        _kafkaProducer = new KafkaProducer(clientId, config, topicConfig, emitter);
    }
    return _kafkaProducer;
}

export default getKafkaProducer;