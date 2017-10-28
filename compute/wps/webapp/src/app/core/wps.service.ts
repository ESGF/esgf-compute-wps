import { Injectable } from '@angular/core';
import { Http, URLSearchParams, RequestOptionsArgs, Headers } from '@angular/http';

export interface WPSResponse {
  status: string;
  error?: string;
  data?: any;
}

@Injectable()
export class WPSService {
  constructor(
    protected http: Http
  ) { }

  getCookie(name: string): string {
    let cookieValue: string = null;

    if (document.cookie && document.cookie !== '') {
      let cookies: string[] = document.cookie.split(';');

      for (let cookie of cookies) {
        if (cookie.trim().substring(0, name.length + 1) === (name + '=')) {
          cookieValue = decodeURIComponent(cookie.trim().substring(name.length + 1));

          break;
        }
      }
    }

    return cookieValue;
  }

  getCSRF(url: string, params: URLSearchParams = null, headers: Headers = new Headers()) {
    headers.append('X-CSRFToken', this.getCookie('csrftoken'));

    return this.get(url, params, headers);
  }

  get(url: string, params: URLSearchParams = null, headers: Headers = new Headers()) {
    return this.http.get(url, {
      params: params,
      headers: headers 
    })
      .toPromise()
      .then(response => response.json() as WPSResponse)
      .catch(this.handleError);
  }

  postCSRF(url: string, data: string = '', headers: Headers = new Headers()) {
    headers.append('X-CSRFToken', this.getCookie('csrftoken'));

    return this.post(url, data, headers);
  }

  post(url: string, data: string = '', headers: Headers = new Headers()) {
    headers.append('Content-Type', 'application/x-www-form-urlencoded');

    return this.http.post(url, data, {
      headers: headers 
    })
      .toPromise()
      .then(response => response.json() as WPSResponse)
      .catch(this.handleError);
  }

  notification(): Promise<WPSResponse> {
    return this.http.get('/wps/notification')
      .toPromise()
      .then(response => response.json() as WPSResponse)
      .catch(this.handleError);
  }

  protected handleError(error: any): Promise<any> {
    return Promise.reject(error.message || error);
  }
}
