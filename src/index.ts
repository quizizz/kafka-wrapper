import getKafkaProducer, { ProduceParameters } from './producer'
import getKafkaConsumer, { ConsumeActionFunction, ListenActionFunction } from './consumer';
import KafkaAdmin from './admin';
import { ClientMetrics, LibrdKafkaError, NumberNullUndefined, SubscribeTopicList } from 'node-rdkafka';
import { ErrorHandlingFunction } from './client';

export {
  getKafkaConsumer,
  getKafkaProducer,
  KafkaAdmin,
}

export interface KafkaConsumer {
    connect(): Promise<KafkaConsumer | LibrdKafkaError>;
    subscribe(topics: SubscribeTopicList): this;
    unsubscribe(): this;
    consume(actionOnData: ConsumeActionFunction): void;
    consumeBatch(msgCount: number, actionOnData: ConsumeActionFunction): void;
    listen(actionOnData: ListenActionFunction): void;
}

export interface KafkaProducer {
    connect(): Promise<KafkaProducer | LibrdKafkaError>;
    produce(args: ProduceParameters): boolean | number;
    flush(timeout?: NumberNullUndefined, postFlushAction?: ErrorHandlingFunction): this;
    disconnect(postDisconnectAction?: (err: any, data: ClientMetrics) => any): this;
}
