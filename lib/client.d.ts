/// <reference types="node" />
import EventEmitter from "events";
import { GlobalConfig, LibrdKafkaError, TopicConfig } from "node-rdkafka";
export default class Client {
    private clientId;
    private clientType;
    private _config;
    private _topicConfig;
    private emitter;
    constructor(clientId: string, clientType: string, _config: GlobalConfig, _topicConfig: TopicConfig, emitter: EventEmitter);
    _logMessage(msgType: 'log' | 'success' | 'error', message: string, data: any): void;
    log(message: string, data?: any): void;
    success(message: string, data?: any): void;
    error(err: string, data?: any): void;
}
export declare type ErrorHandlingFunction = (err: LibrdKafkaError) => void;
