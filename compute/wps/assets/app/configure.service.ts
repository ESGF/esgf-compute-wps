import { Http, Headers } from '@angular/http';
import { Injectable, Inject } from '@angular/core';
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

  searchESGF(params: any): Promise<string> {
    return this.http.get('/wps/search', {
      params: params 
    })
      .toPromise()
      .then(response => response.json())
      .catch(this.handleError);
  }

  downloadScript(config: any): Promise<any> {
    let data = '';

    for (let k in config) {
      data += `${k}=${config[k]}&`;
    }

    return this.http.post('/wps/generate/', data, {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => {
        let pattern = /\"(.*)\"/;
        let url = URL.createObjectURL(new Blob([response.text()]));

        let cd = response.headers.get('Content-Disposition');

        let filename = cd.split(' ')[1].split('=')[1]; 

        let a = this.doc.createElement('a');

        a.href = url;
        a.target = '_blank';
        a.download = filename.match(pattern)[1];

        a.click();
      })
      .catch(this.handleError);
  }

  private handleError(error: any): Promise<any> {
    return Promise.reject(error.message || error);
  }
}
