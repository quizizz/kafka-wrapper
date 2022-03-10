import getKafkaProducer, { ProduceParameters } from './producer';
import getKafkaConsumer, { ConsumeActionFunction, ListenActionFunction } from './consumer';
import KafkaAdmin from './admin';
import { ClientMetrics, LibrdKafkaError, NumberNullUndefined, SubscribeTopicList } from 'node-rdkafka';
import { ErrorHandlingFunction } from './client';
interface KafkaConsumer {
    connect(): Promise<KafkaConsumer | LibrdKafkaError>;
    subscribe(topics: SubscribeTopicList): this;
    unsubscribe(): this;
    consume(actionOnData: ConsumeActionFunction): void;
    consumeBatch(msgCount: number, actionOnData: ConsumeActionFunction): void;
    listen(actionOnData: ListenActionFunction): void;
}
interface KafkaProducer {
    connect(): Promise<KafkaProducer | LibrdKafkaError>;
    produce(args: ProduceParameters): boolean | number;
    flush(timeout?: NumberNullUndefined, postFlushAction?: ErrorHandlingFunction): this;
    disconnect(postDisconnectAction?: (err: any, data: ClientMetrics) => any): this;
}
export { getKafkaConsumer, getKafkaProducer, KafkaAdmin, KafkaConsumer, KafkaProducer, };
