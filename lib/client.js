"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
class Client {
    clientId;
    clientType;
    _config;
    _topicConfig;
    emitter;
    constructor(clientId, clientType, _config, _topicConfig, emitter) {
        this.clientId = clientId;
        this.clientType = clientType;
        this._config = _config;
        this._topicConfig = _topicConfig;
        this.emitter = emitter;
        this.clientId = clientId;
        this.clientType = clientType;
        // common config defaults should go here.
        this._config = Object.assign({
            'metadata.broker.list': 'localhost:9092',
            'socket.keepalive.enable': true,
        }, _config, { 'client.id': clientId });
        // commong topic configs defaults should go here. 
        this._topicConfig = _topicConfig;
        this.emitter = emitter;
    }
    _logMessage(msgType, message, data) {
        if (this.emitter != null) {
            this.emitter.emit(msgType, {
                clientId: this.clientId,
                clientType: this.clientType,
                message,
                data,
            });
        }
        else if (msgType === 'error') {
            console.error(this.clientId, this.clientType, message, typeof data !== 'undefined' ? data : '');
        }
        else {
            console.log(this.clientId, this.clientType, message, typeof data !== 'undefined' ? data : '');
        }
    }
    log(message, data) {
        this._logMessage('log', message, data);
    }
    success(message, data) {
        this._logMessage('success', message, data);
    }
    error(err, data) {
        this._logMessage('error', err, data);
    }
}
exports.default = Client;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY2xpZW50LmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vc3JjL2NsaWVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOztBQUdBLE1BQXFCLE1BQU07SUFDSDtJQUNSO0lBQTRCO0lBQStCO0lBQW1DO0lBRDFHLFlBQW9CLFFBQWdCLEVBQ3hCLFVBQWtCLEVBQVUsT0FBcUIsRUFBVSxZQUF5QixFQUFVLE9BQXFCO1FBRDNHLGFBQVEsR0FBUixRQUFRLENBQVE7UUFDeEIsZUFBVSxHQUFWLFVBQVUsQ0FBUTtRQUFVLFlBQU8sR0FBUCxPQUFPLENBQWM7UUFBVSxpQkFBWSxHQUFaLFlBQVksQ0FBYTtRQUFVLFlBQU8sR0FBUCxPQUFPLENBQWM7UUFDM0gsSUFBSSxDQUFDLFFBQVEsR0FBRyxRQUFRLENBQUM7UUFDekIsSUFBSSxDQUFDLFVBQVUsR0FBRyxVQUFVLENBQUM7UUFFN0IseUNBQXlDO1FBQ3pDLElBQUksQ0FBQyxPQUFPLEdBQUcsTUFBTSxDQUFDLE1BQU0sQ0FBQztZQUN6QixzQkFBc0IsRUFBRSxnQkFBZ0I7WUFDeEMseUJBQXlCLEVBQUUsSUFBSTtTQUNoQyxFQUNELE9BQU8sRUFDUCxFQUFFLFdBQVcsRUFBRSxRQUFRLEVBQUUsQ0FDMUIsQ0FBQztRQUNGLGtEQUFrRDtRQUNsRCxJQUFJLENBQUMsWUFBWSxHQUFHLFlBQVksQ0FBQztRQUNqQyxJQUFJLENBQUMsT0FBTyxHQUFHLE9BQU8sQ0FBQztJQUMzQixDQUFDO0lBRUQsV0FBVyxDQUFDLE9BQW9DLEVBQUUsT0FBZSxFQUFFLElBQVM7UUFDeEUsSUFBSSxJQUFJLENBQUMsT0FBTyxJQUFJLElBQUksRUFBRTtZQUN0QixJQUFJLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxPQUFPLEVBQUU7Z0JBQ3ZCLFFBQVEsRUFBRSxJQUFJLENBQUMsUUFBUTtnQkFDdkIsVUFBVSxFQUFFLElBQUksQ0FBQyxVQUFVO2dCQUMzQixPQUFPO2dCQUNQLElBQUk7YUFDUCxDQUFDLENBQUM7U0FDTjthQUFNLElBQUksT0FBTyxLQUFLLE9BQU8sRUFBRTtZQUM1QixPQUFPLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxRQUFRLEVBQUUsSUFBSSxDQUFDLFVBQVUsRUFBRSxPQUFPLEVBQUUsT0FBTyxJQUFJLEtBQUssV0FBVyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDO1NBQ25HO2FBQU07WUFDSCxPQUFPLENBQUMsR0FBRyxDQUFDLElBQUksQ0FBQyxRQUFRLEVBQUUsSUFBSSxDQUFDLFVBQVUsRUFBRSxPQUFPLEVBQUUsT0FBTyxJQUFJLEtBQUssV0FBVyxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDO1NBQ2pHO0lBQ0wsQ0FBQztJQUVELEdBQUcsQ0FBQyxPQUFlLEVBQUUsSUFBVTtRQUMzQixJQUFJLENBQUMsV0FBVyxDQUFDLEtBQUssRUFBRSxPQUFPLEVBQUUsSUFBSSxDQUFDLENBQUM7SUFDM0MsQ0FBQztJQUVELE9BQU8sQ0FBQyxPQUFlLEVBQUUsSUFBVTtRQUMvQixJQUFJLENBQUMsV0FBVyxDQUFDLFNBQVMsRUFBRSxPQUFPLEVBQUUsSUFBSSxDQUFDLENBQUM7SUFDL0MsQ0FBQztJQUVELEtBQUssQ0FBQyxHQUFXLEVBQUUsSUFBVTtRQUN6QixJQUFJLENBQUMsV0FBVyxDQUFDLE9BQU8sRUFBRSxHQUFHLEVBQUUsSUFBSSxDQUFDLENBQUM7SUFDekMsQ0FBQztDQUNKO0FBN0NELHlCQTZDQyJ9