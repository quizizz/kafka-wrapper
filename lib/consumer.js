"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_rdkafka_1 = __importDefault(require("node-rdkafka"));
const client_1 = __importDefault(require("./client"));
let _kafkaConsumer = null;
class KafkaConsumer extends client_1.default {
    config;
    topicConfig;
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
        this.config = config;
        this.topicConfig = topicConfig;
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
            msgs.forEach((msg) => {
                msg = this._parseMessage(msg);
                actionOnData(err, msg);
            });
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY29uc3VtZXIuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvY29uc3VtZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSxnRUFBOEg7QUFFOUgsc0RBQThCO0FBTTlCLElBQUksY0FBYyxHQUFrQixJQUFJLENBQUM7QUFFekMsTUFBTSxhQUFjLFNBQVEsZ0JBQU07SUFXeUI7SUFBc0M7SUFWckYsUUFBUSxDQUFzQjtJQUV0Qzs7Ozs7OztPQU9HO0lBQ0gsWUFBWSxRQUFnQixFQUFFLE9BQWUsRUFBVSxNQUE0QixFQUFVLFdBQWdDLEVBQUUsT0FBcUI7UUFDaEosMERBQTBEO1FBQzFELE1BQU0sR0FBRyxNQUFNLENBQUMsTUFBTSxDQUFDO1lBQ25CLDBCQUEwQixFQUFFLElBQUk7U0FDbkMsRUFDRyxNQUFNLEVBQ047WUFDSSxVQUFVLEVBQUUsT0FBTztTQUN0QixDQUFDLENBQUM7UUFDUCxLQUFLLENBQUMsUUFBUSxFQUFFLFVBQVUsRUFBRSxNQUFNLEVBQUUsV0FBVyxFQUFFLE9BQU8sQ0FBQyxDQUFDO1FBVFAsV0FBTSxHQUFOLE1BQU0sQ0FBc0I7UUFBVSxnQkFBVyxHQUFYLFdBQVcsQ0FBcUI7UUFVekgsSUFBSSxDQUFDLFFBQVEsR0FBRyxJQUFJLHNCQUFLLENBQUMsYUFBYSxDQUFDLElBQUksQ0FBQyxNQUFNLEVBQUUsSUFBSSxDQUFDLFdBQVcsQ0FBQyxDQUFDO0lBQzNFLENBQUM7SUFFRDs7Ozs7T0FLRztJQUNILE9BQU87UUFDSCxPQUFPLElBQUksT0FBTyxDQUFDLENBQUMsT0FBTyxFQUFFLE1BQU0sRUFBRSxFQUFFO1lBQ25DLElBQUk7Z0JBQ0EsSUFBSSxDQUFDLFFBQVE7cUJBQ1IsT0FBTyxFQUFFO3FCQUNULEVBQUUsQ0FBQyxPQUFPLEVBQUUsQ0FBQyxJQUFJLEVBQUUsUUFBUSxFQUFFLEVBQUU7b0JBQzVCLElBQUksQ0FBQyxPQUFPLENBQUMseUNBQXlDLEVBQUU7d0JBQ3BELElBQUksRUFBRSxJ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