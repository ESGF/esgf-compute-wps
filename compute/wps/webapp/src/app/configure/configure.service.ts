import { Http, Headers } from '@angular/http';
import { Injectable } from '@angular/core';

import { WPSService, WPSResponse } from '../core/wps.service';

@Injectable()
export class ConfigureService extends WPSService {
  constructor(
    http: Http
  ) { 
    super(http); 
  }

  processes(): Promise<any> {
    return this.http.get('/wps/processes')
      .toPromise()
      .then(response => response.json())
      .catch(this.handleError);
  }

  searchESGF(params: any): Promise<any> {
    return this.http.get('/wps/search', {
      params: params 
    })
      .toPromise()
      .then(response => response.json())
      .catch(this.handleError);
  }

  execute(config: string): Promise<any> {
    return this.http.post('/wps/execute/', config, {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => response.json())
      .catch(this.handleError);
  }

  downloadScript(config: string): Promise<any> {
    return this.http.post('/wps/generate/', config, {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => response.json())
      .catch(this.handleError);
  }
}
