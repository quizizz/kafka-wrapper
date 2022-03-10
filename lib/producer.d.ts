/// <reference types="node" />
import EventEmitter from 'events';
import { ClientMetrics, LibrdKafkaError, MessageKey, NumberNullUndefined, ProducerGlobalConfig, ProducerTopicConfig } from 'node-rdkafka';
import Client, { ErrorHandlingFunction } from './client';
export interface ProduceParameters {
    topic: string;
    message: any;
    partition?: NumberNullUndefined;
    key?: MessageKey;
    timestamp?: NumberNullUndefined;
}
declare class KafkaProducer extends Client {
    private producer;
    /**
     * Initializes a KafkaProducer.
     * @param {String} clientId: id to identify a client producing the message.
     * @param {import('node-rdkafka').ProducerGlobalConfig} config: configs for producer.
     * @param {import('node-rdkafka').ProducerTopicConfig} topicConfig: topic configs.
     * @param {EventEmitter} emitter: to emit log messages
     */
    constructor(clientId: string, config: ProducerGlobalConfig, topicConfig: ProducerTopicConfig, emitter: EventEmitter);
    /**
     * Asynchronous function which connects to kafka cluster.
     * Resolves when connection is ready.
     *
     * @returns {Promise}
     */
    connect(): Promise<this | LibrdKafkaError>;
    /**
     * Produce a message to a topic-partition.
     * @param {String} topic: name of topic
     * @param {import('node-rdkafka').NumberNullUndefined} partition: partition number to produce to.
     * @param {any} message: message to be produced.
     * @param {import('node-rdkafka').MessageKey} key: key associated with the message.
     * @param {import('node-rdkafka').NumberNullUndefined} timestamp: timestamp to send with the message.
     * @returns {import('../types').BooleanOrNumber}: returns boolean or librdkafka error code.
     */
    produce({ topic, message, partition, key, timestamp }: ProduceParameters): boolean | number;
    /**
     * Flush everything on the internal librdkafka buffer.
     * Good to perform before disconnect.
     * @param {import('node-rdkafka').NumberNullUndefined}} timeout
     * @param {import('../types').ErrorHandlingFunction} postFlushAction
     * @returns {KafkaProducer}
     */
    flush(timeout?: NumberNullUndefined, postFlushAction?: ErrorHandlingFunction): this;
    /**
     * Disconnects producer.
     * @param {import('../types').DisconnectFunction} postDisconnectAction
     * @returns {KafkaProducer}
     */
    disconnect(postDisconnectAction?: (err: any, data: ClientMetrics) => any): this;
}
declare function getKafkaProducer(clientId: string, config: ProducerGlobalConfig, topicConfig: ProducerTopicConfig, emitter: EventEmitter): KafkaProducer;
export default getKafkaProducer;
