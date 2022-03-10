import Client from './client';
import { AdminClient, GlobalConfig } from 'node-rdkafka';
import EventEmitter from 'events';

class KafkaAdmin extends Client {
    private adminClient;

    /**
     * Initialzes a KafkaAdmin client with config.
     * Requires using connect() function after initalizing.
     * @param {string} clientId - id of client performing request
     * @param {object} config - global kafka config
     * @param {object} emitter - emitter to emit log event
     */
    constructor(clientId: string, private config: GlobalConfig, emitter: EventEmitter) {
        super(clientId, 'admin', config, {}, emitter);
        this.adminClient = null;
    }

    /**
     * Connect to kafka server as admin.
     */
    async connect(): Promise<void> {
        try {
            if (this.adminClient === null) {
                this.adminClient = await AdminClient.create(this.config);
            }
            this.success('Successfully connected to kafka as admin');
        } catch (err) {
            this.error('Encountered error while connecting to kafka as admin', err);
        }
    }

    createTopic(topic, timeout, actionPostTopicCreation) {
        try {
            this.adminClient.createTopic(topic, timeout, actionPostTopicCreation);
            this.success('Successfully created new topic.', topic.topic);
        } catch (err) {
            this.error(`Encountered error while creating topic=${topic}:`, err);
        }
    }

    deleteTopic(topic, timeout, actionPostTopicDeletion) {
        try {
            this.adminClient.deleteTopic(topic, timeout, actionPostTopicDeletion);
            this.success('Successfully deleted a topic.', topic);
        } catch (err) {
            this.error(`Encountered error while deleting topic=${topic}.`, err);
        }

    }

    /**
     * Create new partitions for a topic.
     * @param {string} `topic 
     * @param {number} totalPartitions: The total number of partitions topic should have after request. 
     * @param {number} timeout 
     * @param {function} actionPostPartitionCreation 
     */
    createPartitions(topic, totalPartitions, timeout, actionPostPartitionCreation) {
        try {
            this.adminClient.createPartitions(topic, totalPartitions, timeout, actionPostPartitionCreation);
            this.success(`Successfully created new topic partitons: topic=${topic}, totalParitions=${totalPartitions}`);
        } catch (err) {
            this.error(
                `Encountered error while creating new partitions for topic: topic=${topic}, totalPartitons=${totalPartitions}`,
                err
            );
        }
    }

    /**
     * Synchronous method.
     */
    disconnect() {
        this.adminClient.disconnect();
    }
}

export default KafkaAdmin;
