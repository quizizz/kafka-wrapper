import EventEmitter from "events";
import { GlobalConfig, LibrdKafkaError, TopicConfig } from "node-rdkafka";

export default class Client {
    constructor(private clientId: string,
        private clientType: string, protected config: GlobalConfig, protected topicConfig: TopicConfig, private emitter: EventEmitter) {
        this.clientId = clientId;
        this.clientType = clientType;
        
        // common config defaults should go here.
        this.config = Object.assign({
            'metadata.broker.list': 'localhost:9092',
            'socket.keepalive.enable': true,
          }, 
          config,
          { 'client.id': clientId }
        );
        // commong topic configs defaults should go here. 
        this.topicConfig = topicConfig;
        this.emitter = emitter;
    }

    _logMessage(msgType: 'log' | 'success' | 'error', message: string, data: any) {
        if (this.emitter != null) {
            this.emitter.emit(msgType, {
                clientId: this.clientId,
                clientType: this.clientType,
                message,
                data,
            });
        } else if (msgType === 'error') {
            console.error(this.clientId, this.clientType, message, typeof data !== 'undefined' ? data : '');
        } else {
            console.log(this.clientId, this.clientType, message, typeof data !== 'undefined' ? data : '');
        }
    }

    log(message: string, data?: any) {
        this._logMessage('log', message, data);
    }

    success(message: string, data?: any) {
        this._logMessage('success', message, data);
    }

    error(err: string, data?: any) {
        this._logMessage('error', err, data);
    }
}

export type ErrorHandlingFunction = (err: LibrdKafkaError) => void;
