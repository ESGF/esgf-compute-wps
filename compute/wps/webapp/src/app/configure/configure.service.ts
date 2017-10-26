import { Http, Headers } from '@angular/http';
import { Injectable, Inject} from '@angular/core';
import { DOCUMENT } from '@angular/platform-browser';

@Injectable()
export class ConfigureService {
  constructor(
    @Inject(DOCUMENT) private doc: any,
    private http: Http
  ) { }

  getCookie(name: string): string {
    let cookieValue: string = null;

    if (this.doc.cookie && this.doc.cookie !== '') {
      let cookies: string[] = this.doc.cookie.split(';');

      for (let cookie of cookies) {
        if (cookie.trim().substring(0, name.length + 1) === (name + '=')) {
          cookieValue = decodeURIComponent(cookie.trim().substring(name.length + 1));

          break;
        }
      }
    }

    return cookieValue;
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

  private handleError(error: any): Promise<any> {
    return Promise.reject(error.message || error);
  }
}
