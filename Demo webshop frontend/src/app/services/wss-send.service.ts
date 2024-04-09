import { Injectable } from '@angular/core';
import { BaseWssService } from './wss-base.service';
import { EnvironmentVariableService } from './environment-variable.service';

@Injectable({
  providedIn: 'root'
})
export class WssSendService extends BaseWssService {

  constructor(private environmentVariables: EnvironmentVariableService) {
    super();
  }

  public connectAndSend(topic: string, streamId: string, data: any): void {
    var baseUrl = this.environmentVariables.buildUrl('web-socket-publisher');
    var url = `${baseUrl}/topic/${topic}/stream/${streamId}`;
    this.connect(url);
    this.webSocketSubject.subscribe();
    this.webSocketSubject.next(data);
    this.disconnect();
  }
}