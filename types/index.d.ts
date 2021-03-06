import { ClientMetrics, ConsumerGlobalConfig, ConsumerTopicConfig, GlobalConfig, LibrdKafkaError, Message, MessageKey, NewTopic, NumberNullUndefined, ProducerGlobalConfig, ProducerTopicConfig, SubscribeTopicList } from "node-rdkafka";

export type ConsumeActionFunction = (err: LibrdKafkaError, messages: Message[]) => void;

export type ListenActionFunction = (arg: Message) => void;

export type ErrorHandlingFunction = (err: LibrdKafkaError) => void;

export type DisconnectFunction = (err: any, data: ClientMetrics) => any; 

export type BooleanOrNumber = boolean | number;

export interface ProduceParameters{
    topic: string;
    message: any;
    partition?: NumberNullUndefined;
    key?: MessageKey;
    timestamp?: NumberNullUndefined;
}

export class KafkaConsumer {
    constructor(clientId: string, groupId: string, config: ConsumerGlobalConfig, topicConfig: ConsumerTopicConfig, emitter: any);
    connect(): Promise<this | LibrdKafkaError>;
    subscribe(topics: SubscribeTopicList): this;
    unsubscribe(): this;
    consume(actionOnData: ConsumeActionFunction): void;
    consumeBatch(msgCount: number, actionOnData: ConsumeActionFunction): void;
    listen(actionOnData: ListenActionFunction): void;
}

export class KafkaProducer {
    constructor(clientId: string, config: ProducerGlobalConfig, topicConfig: ProducerTopicConfig, emitter: any);
    connect(): Promise<this | LibrdKafkaError>;
    produce(args: ProduceParameters): BooleanOrNumber;
    flush(timeout?: NumberNullUndefined, postFlushAction?: ErrorHandlingFunction): this;
    disconnect(postDisconnectAction?: DisconnectFunction): this;
}

export class KafkaAdmin {
    constructor(clientId: string, config: GlobalConfig, emitter: any);
    connect(): void;
    createTopic(topic: NewTopic, actionPostTopicCreation?: ErrorHandlingFunction): void;
    createTopic(topic: NewTopic, timeout?: number, actionPostTopicCreation?: ErrorHandlingFunction): void;
    deleteTopic(topic: string, actionPostTopicDeletion?: ErrorHandlingFunction): void;
    deleteTopic(topic: string, timeout?: number, actionPostTopicDeletion?: ErrorHandlingFunction): void;
    createPartitions(topic: string, totalPartitions: number, actionPostPartitionCreation?: ErrorHandlingFunction): void;
    createPartitions(topic: string, totalPartitions: number, timeout?: number, actionPostPartitionCreation?: ErrorHandlingFunction): void;
    disconnect(): void;
}