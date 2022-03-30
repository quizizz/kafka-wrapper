const Kafka = require('node-rdkafka');
const Client = require('./client');

class KafkaConsumer extends Client {

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
        },
            config,
            {
                'group.id': groupId,
            });
        super(clientId, 'consumer', config, topicConfig, emitter);
        this.consumer = new Kafka.KafkaConsumer(this.config, this.topicConfig);
    }

    /**
     * Asynchronous function which connects to kafka cluster. 
     * Resolves when connection is ready.
     *
     * @returns {Promise} 
     */
    connect() {
        this.consumer
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
            this.error(`Encountered connection failure with kafka. Client metrics: ${metrics.connectionOpened}`, err);
        })
        .on('event.error', (err) => {
            this.error('Encountered error event.', err);
        })
        .on('disconnected', (metrics) => {
            this.log(`Disconnected from kafka. Client metrics: ${metrics.connectionOpened}`);
        })
        .on('offset.commit', (err, topicPartitions) => {
            if (err) {
                this.error('Encountered error while committing offset.', err);
                return;
            }
            this.log(`Commited offset for topic-partitions: ${JSON.stringify(topicPartitions)}`);
        })
        .on('subscribed', (topics) => {
            this.log(`Subscribed to topics: ${topics}`);
        });
        return new Promise((resolve, reject) => {
                this.consumer.connect({}, (err, data) => {
                    if (err) {
                        this.error('Encountered error while connecting to kafka.');
                        return reject(err);
                    }
                    resolve(data);
                })
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
        } catch (err) {
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
        } catch (err) {
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
        } catch (err) {
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
        } catch (err) {
            this.error(`Consumer encountered error while consuming messages in batch of size=${msgCount}`, err)
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
        } catch (err) {
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
            } catch (e) {
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

module.exports = KafkaConsumer;