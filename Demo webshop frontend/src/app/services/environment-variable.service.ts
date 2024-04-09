import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class EnvironmentVariableService {
  // this is the token that will authenticate the user into the ungated product experience.
  // ungated means no password or login is needed.
  // the token is locked down to the max and everything is read only.
  public ungatedToken: string = 'pat-b88b3caf912641a1b0fa8b47b262868b';

  /*~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-*/
  /*WORKING LOCALLY? UPDATE THESE!*/
  // set this to the URL of your running websocket services. 
  // e.g. for services running in Quix platform: orgname-projectname-environmentname.deployments.quix.io
  //      for local: please update the wss-receive.service.ts and wss-send.service.ts files
  public workspaceUrl: string = 'demo-clickstreamanalysis-migration.deployments.quix.io';  

  public clickTopic: string = environment.CLICK_TOPIC || 'click-data'; // get topic name from the Topics page in the Quix portal
  public offersTopic: string = environment.OFFERS_TOPIC || 'special-offers'; // get topic name from the Topics page in the Quix portal
  /* optional */
  /*~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-*/

  constructor(private httpClient: HttpClient) {

    const headers = new HttpHeaders().set('Content-Type', 'text/plain; charset=utf-8');

    if(this.clickTopic === ''){
      this.httpClient.get('click_topic', { headers, responseType: 'text' })
        .subscribe(clickTopic => {
          this.clickTopic = this.stripLineFeed(clickTopic);
        });
    }
  
    if(this.offersTopic === ''){
      this.httpClient.get('offers_topic', { headers, responseType: 'text' })
        .subscribe(offersTopic => {
          this.offersTopic = this.stripLineFeed(offersTopic);
        });
    }

  }

  private extractUrlPart(url: string): string | null {
    const match = url.match(/demo-webshop-frontend-(.*)/);
    return match ? match[1] : null;
  }

  private getBaseUrl(){
    if(this.workspaceUrl !== ''){
      return this.workspaceUrl;
    }
    else{
      const fullUrl = window.location.href;
      const match = fullUrl.match(/(.*quix\.io)/);
      var strippedUrl = match ? match[0] : '';

      const extractedPart = this.extractUrlPart(strippedUrl);
      console.log(extractedPart);
      return extractedPart
    }
  }

  public buildUrl(serviceName: string): string{
    return `wss://${serviceName}-${this.getBaseUrl()}`;
  }

  private stripLineFeed(s: string): string {
    return s.replace('\n', '');
  }
}