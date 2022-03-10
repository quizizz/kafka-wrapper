/// <reference types="node" />
import { ConsumerGlobalConfig, ConsumerTopicConfig, LibrdKafkaError, Message, SubscribeTopicList } from 'node-rdkafka';
import { EventEmitter } from 'stream';
import Client from './client';
export declare type ConsumeActionFunction = (err: LibrdKafkaError, messages: Message[]) => void;
export declare type ListenActionFunction = (arg: Message) => void;
declare class KafkaConsumer extends Client {
    private config;
    private topicConfig;
    private consumer;
    /**
     * Initializes a KafkaConsumer.
     * @param {String} clientId: id to identify a client consuming the message.
     * @param {String} groupId: consumer group id, the consumer belongs to.
     * @param {import('node-rdkafka').ConsumerGlobalConfig} config: configs for consumer.
     * @param {import('node-rdkafka').ConsumerTopicConfig} topicConfig: topic configs
     * @param {EventEmitter} emitter: to emit log events
     */
    constructor(clientId: string, groupId: string, config: ConsumerGlobalConfig, topicConfig: ConsumerTopicConfig, emitter: EventEmitter);
    /**
     * Asynchronous function which connects to kafka cluster.
     * Resolves when connection is ready.
     *
     * @returns {Promise}
     */
    connect(): Promise<this | LibrdKafkaError>;
    /**
     * Subscribe to topics.
     * @param {import('node-rdkafka').SubscribeTopicList} topics: array of topic names.
     * @returns {KafkaConsumer}
     */
    subscribe(topics: SubscribeTopicList): this;
    /**
     * Unsubscribe from all the subscribed topics.s
     * @returns {KafkaConsumer}
     */
    unsubscribe(): this;
    /**
     * Consumes message one-by-one and executes actionsOnData callback
     * on the message read.
     *
     * NOTE: Needs to be called in infinite loop to have it consuming messages continuously.
     *
     * @param {Function} actionOnData: callback to return when message is read.
     */
    consume(actionOnData: ConsumeActionFunction): void;
    /**
     * Consumes messages in a batch and executes actionsOnData callback
     * on the message read.
     *
     * NOTE: Needs to be called in infinite loop to have it consuming messages continuously.
     *
     * @param {Number} msgCount: number of messages to read.
     * @param {Function} actionOnData: callback to be executed for each message.
     */
    consumeBatch(msgCount: number, actionOnData: ConsumeActionFunction): void;
    /**
     * Listens to subscribed topic in flowing mode. Triggers a thread in background which keeps polling for events.
     *
     * @param {Function} actionOnData
     */
    listen(actionOnData: ListenActionFunction): void;
    _wrapConsumeCallbackWrapper(actionOnData: any): (err: any, msgs: any) => void;
    _wrapListenCallbackWrapper(actionOnData: any): (msg: any) => void;
    /**
     * Parses message before passing it to consumer callback.
     * @param {Object} msg - expects it to be in node-rdkafka msg format.
     * @returns
     */
    _parseMessage(msg: any): any;
}
declare function getKafkaConsumer(clientId: string, groupId: string, config: ConsumerGlobalConfig, topicConfig: ConsumerTopicConfig, emitter: EventEmitter): KafkaConsumer;
export default getKafkaConsumer;
