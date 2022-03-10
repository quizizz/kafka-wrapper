/// <reference types="node" />
import Client from './client';
import { GlobalConfig } from 'node-rdkafka';
import EventEmitter from 'events';
declare class KafkaAdmin extends Client {
    private config;
    private adminClient;
    /**
     * Initialzes a KafkaAdmin client with config.
     * Requires using connect() function after initalizing.
     * @param {string} clientId - id of client performing request
     * @param {object} config - global kafka config
     * @param {object} emitter - emitter to emit log event
     */
    constructor(clientId: string, config: GlobalConfig, emitter: EventEmitter);
    /**
     * Connect to kafka server as admin.
     */
    connect(): Promise<void>;
    createTopic(topic: any, timeout: any, actionPostTopicCreation: any): void;
    deleteTopic(topic: any, timeout: any, actionPostTopicDeletion: any): void;
    /**
     * Create new partitions for a topic.
     * @param {string} `topic
     * @param {number} totalPartitions: The total number of partitions topic should have after request.
     * @param {number} timeout
     * @param {function} actionPostPartitionCreation
     */
    createPartitions(topic: any, totalPartitions: any, timeout: any, actionPostPartitionCreation: any): void;
    /**
     * Synchronous method.
     */
    disconnect(): void;
}
export default KafkaAdmin;
