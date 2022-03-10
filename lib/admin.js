"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const client_1 = __importDefault(require("./client"));
const node_rdkafka_1 = require("node-rdkafka");
class KafkaAdmin extends client_1.default {
    adminClient;
    /**
     * Initialzes a KafkaAdmin client with config.
     * Requires using connect() function after initalizing.
     * @param {string} clientId - id of client performing request
     * @param {object} config - global kafka config
     * @param {object} emitter - emitter to emit log event
     */
    constructor(clientId, config, emitter) {
        super(clientId, 'admin', config, {}, emitter);
        this.adminClient = null;
    }
    /**
     * Connect to kafka server as admin.
     */
    async connect() {
        try {
            if (this.adminClient === null) {
                this.adminClient = await node_rdkafka_1.AdminClient.create(this.config);
            }
            this.success('Successfully connected to kafka as admin');
        }
        catch (err) {
            this.error('Encountered error while connecting to kafka as admin', err);
        }
    }
    createTopic(topic, timeout, actionPostTopicCreation) {
        try {
            this.adminClient.createTopic(topic, timeout, actionPostTopicCreation);
            this.success('Successfully created new topic.', topic.topic);
        }
        catch (err) {
            this.error(`Encountered error while creating topic=${topic}:`, err);
        }
    }
    deleteTopic(topic, timeout, actionPostTopicDeletion) {
        try {
            this.adminClient.deleteTopic(topic, timeout, actionPostTopicDeletion);
            this.success('Successfully deleted a topic.', topic);
        }
        catch (err) {
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
        }
        catch (err) {
            this.error(`Encountered error while creating new partitions for topic: topic=${topic}, totalPartitons=${totalPartitions}`, err);
        }
    }
    /**
     * Synchronous method.
     */
    disconnect() {
        this.adminClient.disconnect();
    }
}
exports.default = KafkaAdmin;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYWRtaW4uanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvYWRtaW4udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSxzREFBOEI7QUFDOUIsK0NBQXlEO0FBR3pELE1BQU0sVUFBVyxTQUFRLGdCQUFNO0lBQ25CLFdBQVcsQ0FBQztJQUVwQjs7Ozs7O09BTUc7SUFDSCxZQUFZLFFBQWdCLEVBQUUsTUFBb0IsRUFBRSxPQUFxQjtRQUNyRSxLQUFLLENBQUMsUUFBUSxFQUFFLE9BQU8sRUFBRSxNQUFNLEVBQUUsRUFBRSxFQUFFLE9BQU8sQ0FBQyxDQUFDO1FBQzlDLElBQUksQ0FBQyxXQUFXLEdBQUcsSUFBSSxDQUFDO0lBQzVCLENBQUM7SUFFRDs7T0FFRztJQUNILEtBQUssQ0FBQyxPQUFPO1FBQ1QsSUFBSTtZQUNBLElBQUksSUFBSSxDQUFDLFdBQVcsS0FBSyxJQUFJLEVBQUU7Z0JBQzNCLElBQUksQ0FBQyxXQUFXLEdBQUcsTUFBTSwwQkFBVyxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLENBQUM7YUFDNUQ7WUFDRCxJQUFJLENBQUMsT0FBTyxDQUFDLDBDQUEwQyxDQUFDLENBQUM7U0FDNUQ7UUFBQyxPQUFPLEdBQUcsRUFBRTtZQUNWLElBQUksQ0FBQyxLQUFLLENBQUMsc0RBQXNELEVBQUUsR0FBRyxDQUFDLENBQUM7U0FDM0U7SUFDTCxDQUFDO0lBRUQsV0FBVyxDQUFDLEtBQUssRUFBRSxPQUFPLEVBQUUsdUJBQXVCO1FBQy9DLElBQUk7WUFDQSxJQUFJLENBQUMsV0FBVyxDQUFDLFdBQVcsQ0FBQyxLQUFLLEVBQUUsT0FBTyxFQUFFLHVCQUF1QixDQUFDLENBQUM7WUFDdEUsSUFBSSxDQUFDLE9BQU8sQ0FBQyxpQ0FBaUMsRUFBRSxLQUFLLENBQUMsS0FBSyxDQUFDLENBQUM7U0FDaEU7UUFBQyxPQUFPLEdBQUcsRUFBRTtZQUNWLElBQUksQ0FBQyxLQUFLLENBQUMsMENBQTBDLEtBQUssR0FBRyxFQUFFLEdBQUcsQ0FBQyxDQUFDO1NBQ3ZFO0lBQ0wsQ0FBQztJQUVELFdBQVcsQ0FBQyxLQUFLLEVBQUUsT0FBTyxFQUFFLHVCQUF1QjtRQUMvQyxJQUFJO1lBQ0EsSUFBSSxDQUFDLFdBQVcsQ0FBQyxXQUFXLENBQUMsS0FBSyxFQUFFLE9BQU8sRUFBRSx1QkFBdUIsQ0FBQyxDQUFDO1lBQ3RFLElBQUksQ0FBQyxPQUFPLENBQUMsK0JBQStCLEVBQUUsS0FBSyxDQUFDLENBQUM7U0FDeEQ7UUFBQyxPQUFPLEdBQUcsRUFBRTtZQUNWLElBQUksQ0FBQyxLQUFLLENBQUMsMENBQTBDLEtBQUssR0FBRyxFQUFFLEdBQUcsQ0FBQyxDQUFDO1NBQ3ZFO0lBRUwsQ0FBQztJQUVEOzs7Ozs7T0FNRztJQUNILGdCQUFnQixDQUFDLEtBQUssRUFBRSxlQUFlLEVBQUUsT0FBTyxFQUFFLDJCQUEyQjtRQUN6RSxJQUFJO1lBQ0EsSUFBSSxDQUFDLFdBQVcsQ0FBQyxnQkFBZ0IsQ0FBQyxLQUFLLEVBQUUsZUFBZSxFQUFFLE9BQU8sRUFBRSwyQkFBMkIsQ0FBQyxDQUFDO1lBQ2hHLElBQUksQ0FBQyxPQUFPLENBQUMsbURBQW1ELEtBQUssb0JBQW9CLGVBQWUsRUFBRSxDQUFDLENBQUM7U0FDL0c7UUFBQyxPQUFPLEdBQUcsRUFBRTtZQUNWLElBQUksQ0FBQyxLQUFLLENBQ04sb0VBQW9FLEtBQUssb0JBQW9CLGVBQWUsRUFBRSxFQUM5RyxHQUFHLENBQ04sQ0FBQztTQUNMO0lBQ0wsQ0FBQztJQUVEOztPQUVHO0lBQ0gsVUFBVTtRQUNOLElBQUksQ0FBQyxXQUFXLENBQUMsVUFBVSxFQUFFLENBQUM7SUFDbEMsQ0FBQztDQUNKO0FBRUQsa0JBQWUsVUFBVSxDQUFDIn0=