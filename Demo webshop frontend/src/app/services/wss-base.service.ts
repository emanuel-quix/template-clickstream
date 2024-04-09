import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export abstract class BaseWssService {
  
  protected webSocketSubject: WebSocketSubject<any>;

  protected connect(url: string): void {
    this.webSocketSubject = webSocket(url);
  }

  public disconnect(): void {
    if (this.webSocketSubject) {
      this.webSocketSubject.complete();
    }
  }
}