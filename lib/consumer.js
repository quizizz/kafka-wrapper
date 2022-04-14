"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_rdkafka_1 = __importDefault(require("node-rdkafka"));
const client_1 = __importDefault(require("./client"));
let _kafkaConsumer = null;
class KafkaConsumer extends client_1.default {
    consumer;
    /**
     * Initializes a KafkaConsumer.
     * @param {String} clientId: id to identify a client consuming the message.
     * @param {String} groupId: consumer group id, the consumer belongs to.
     * @param {import('node-rdkafka').ConsumerGlobalConfig} config: configs for consumer.
     * @param {import('node-rdkafka').ConsumerTopicConfig} topicConfig: topic configs
     * @param {EventEmitter} emitter: to emit log events
     */
    constructor(clientId, groupId, config, topicConfig, emitter) {
        // consumer specific default configs we would like to have
        config = Object.assign({
            'allow.auto.create.topics': true,
        }, config, {
            'group.id': groupId,
        });
        super(clientId, 'consumer', config, topicConfig, emitter);
        this.consumer = new node_rdkafka_1.default.KafkaConsumer(this.config, this.topicConfig);
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
                this.consumer
                    .connect()
                    .on('ready', (info, metadata) => {
                    this.success('Consumer connected to kafka cluster....', {
                        name: info.name,
                    });
                    resolve(this);
                })
                    .on('connection.failure', (err, clientMetrics) => {
                    this.error('Consumer encountered error while connecting to Kafka.', JSON.stringify(err));
                    reject(err);
                })
                    .on('event.error', (err) => {
                    this.error('Consumer encountered error.', JSON.stringify(err));
                    reject(err);
                })
                    .on('event.log', (eventData) => this.log('Logging consumer event: ', eventData))
                    .on('disconnected', (metrics) => {
                    this.log('Consumer disconnected. Client metrics are: ' + metrics.connectionOpened);
                })
                    .on('offset.commit', (err, topicPartitions) => {
                    if (err) {
                        this.error('Encountered error while committing offset.', JSON.stringify(err));
                        return;
                    }
                    this.log('Commited offset for topic-partitions: ' + JSON.stringify(topicPartitions));
                })
                    .on('subscribed', (topics) => {
                    this.log('Subscribed to topics: ' + JSON.stringify(topics));
                });
            }
            catch (err) {
                this.error('Consumer encountered while connecting to kafka server.', err);
                reject(err);
            }
        });
    }
    /**
     * Subscribe to topics.
     * @param {import('node-rdkafka').SubscribeTopicList} topics: array of topic names.
     * @returns {KafkaConsumer}
     */
    subscribe(topics) {
        try {
            this.consumer.subscribe(topics);
        }
        catch (err) {
            this.error(`Consumer encountered error while subscribing to topics=${topics}`, err);
        }
        return this;
    }
    /**
     * Unsubscribe from all the subscribed topics.s
     * @returns {KafkaConsumer}
     */
    unsubscribe() {
        try {
            this.consumer.unsubscribe();
        }
        catch (err) {
            this.error('Consumer encountered error while unsubscribing', err);
        }
        return this;
    }
    /**
     * Consumes message one-by-one and executes actionsOnData callback
     * on the message read.
     *
     * NOTE: Needs to be called in infinite loop to have it consuming messages continuously.
     *
     * @param {Function} actionOnData: callback to return when message is read.
     */
    consume(actionOnData) {
        try {
            // reset 'data' event listener to no-op callback. 
            this.consumer.removeAllListeners('data');
            this.consumer.consume(this._wrapConsumeCallbackWrapper(actionOnData));
        }
        catch (err) {
            this.error('Consumer encountered error while consuming messages', err);
        }
    }
    /**
     * Consumes messages in a batch and executes actionsOnData callback
     * on the message read.
     *
     * NOTE: Needs to be called in infinite loop to have it consuming messages continuously.
     *
     * @param {Number} msgCount: number of messages to read.
     * @param {Function} actionOnData: callback to be executed for each message.
     */
    consumeBatch(msgCount, actionOnData) {
        try {
            // reset 'data' event listener to no-op callback. 
            this.consumer.removeAllListeners('data');
            this.consumer.consume(msgCount, this._wrapConsumeCallbackWrapper(actionOnData));
        }
        catch (err) {
            this.error(`Consumer encountered error while consuming messages in batch of size=${msgCount}`, err);
        }
    }
    /**
     * Listens to subscribed topic in flowing mode. Triggers a thread in background which keeps polling for events.
     *
     * @param {Function} actionOnData
     */
    listen(actionOnData) {
        try {
            this.consumer.on('data', this._wrapListenCallbackWrapper(actionOnData));
            this.consumer.consume();
        }
        catch (err) {
            this.error('Consumer encountered error while starting to listen to messages.', err);
        }
    }
    _wrapConsumeCallbackWrapper(actionOnData) {
        const wrapper = (err, msgs) => {
            if (err) {
                actionOnData(err, msgs);
                return;
            }
            if (!Array.isArray(msgs)) {
                msgs = [msgs];
            }
            const parsedMsgs = msgs.map((msg) => this._parseMessage(msg));
            actionOnData(err, parsedMsgs);
        };
        return wrapper;
    }
    _wrapListenCallbackWrapper(actionOnData) {
        const wrapper = (msg) => {
            try {
                msg = this._parseMessage(msg);
                actionOnData(msg);
            }
            catch (e) {
                this.error(e);
            }
        };
        return wrapper;
    }
    /**
     * Parses message before passing it to consumer callback.
     * @param {Object} msg - expects it to be in node-rdkafka msg format.
     * @returns
     */
    _parseMessage(msg) {
        msg.value = msg.value == null ? null : JSON.parse(msg.value.toString());
        msg.key = msg.key != null && Buffer.isBuffer(msg.key) ? msg.key.toString() : msg.key;
        return msg;
    }
}
function getKafkaConsumer(clientId, groupId, config, topicConfig, emitter) {
    if (!_kafkaConsumer) {
        _kafkaConsumer = new KafkaConsumer(clientId, groupId, config, topicConfig, emitter);
    }
    return _kafkaConsumer;
}
exports.default = getKafkaConsumer;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29uc3VtZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvY29uc3VtZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSxnRUFBOEg7QUFFOUgsc0RBQThCO0FBTTlCLElBQUksY0FBYyxHQUFrQixJQUFJLENBQUM7QUFFekMsTUFBTSxhQUFjLFNBQVEsZ0JBQU07SUFDdEIsUUFBUSxDQUFzQjtJQUV0Qzs7Ozs7OztPQU9HO0lBQ0gsWUFBWSxRQUFnQixFQUFFLE9BQWUsRUFBRSxNQUE0QixFQUFFLFdBQWdDLEVBQUUsT0FBcUI7UUFDaEksMERBQTBEO1FBQzFELE1BQU0sR0FBRyxNQUFNLENBQUMsTUFBTSxDQUFDO1lBQ25CLDBCQUEwQixFQUFFLElBQUk7U0FDbkMsRUFDRyxNQUFNLEVBQ047WUFDSSxVQUFVLEVBQUUsT0FBTztTQUN0QixDQUFDLENBQUM7UUFDUCxLQUFLLENBQUMsUUFBUSxFQUFFLFVBQVUsRUFBRSxNQUFNLEVBQUUsV0FBVyxFQUFFLE9BQU8sQ0FBQyxDQUFDO1FBQzFELElBQUksQ0FBQyxRQUFRLEdBQUcsSUFBSSxzQkFBSyxDQUFDLGFBQWEsQ0FBQyxJQUFJLENBQUMsTUFBTSxFQUFFLElBQUksQ0FBQyxXQUFXLENBQUMsQ0FBQztJQUMzRSxDQUFDO0lBRUQ7Ozs7O09BS0c7SUFDSCxPQUFPO1FBQ0gsT0FBTyxJQUFJLE9BQU8sQ0FBQyxDQUFDLE9BQU8sRUFBRSxNQUFNLEVBQUUsRUFBRTtZQUNuQyxJQUFJO2dCQUNBLElBQUksQ0FBQyxRQUFRO3FCQUNSLE9BQU8sRUFBRTtxQkFDVCxFQUFFLENBQUMsT0FBTyxFQUFFLENBQUMsSUFBSSxFQUFFLFFBQVEsRUFBRSxFQUFFO29CQUM1QixJQUFJLENBQUMsT0FBTyxDQUFDLHlDQUF5QyxFQUFFO3dCQUNwRCxJQUFJLEVBQUUsSUFBSSxDQUFDLElBQUk7cUJBQ2xCLENBQUMsQ0FBQztvQkFDSCxPQUFPLENBQUMsSUFBSSxDQUFDLENBQUM7Z0JB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