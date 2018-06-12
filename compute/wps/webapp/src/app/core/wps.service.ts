import { Injectable } from '@angular/core';
import { Http, URLSearchParams, RequestOptionsArgs, Headers } from '@angular/http';
import { Params } from '@angular/router';

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

  getUnmodified(url: string, params: URLSearchParams|Params = null, headers: Headers = new Headers()) {
    return this.http.get(url, {
      params: params,
      headers: headers
    })
      .toPromise();
  }

  get(url: string, params: URLSearchParams|Params = null, headers: Headers = new Headers()) {
    return this.getUnmodified(url, params, headers)
      .then(result => {
        let response = result.json() as WPSResponse;

        if (response.status === 'failed') {
          throw response.error;
        }
     
        return response;
      });
  }

  postCSRF(url: string, data: string = '', headers: Headers = new Headers()) {
    headers.append('X-CSRFToken', this.getCookie('csrftoken'));

    return this.post(url, data, headers);
  }

  postCSRFUnmodified(url: string, data: string = '', params: URLSearchParams = new URLSearchParams()) {
    let headers = new Headers();

    headers.append('X-CSRFToken', this.getCookie('csrftoken'));

    return this.postUnmodified(url, data, headers, params);
  }

  postUnmodified(url: string, data: string = '', headers = new Headers(), params = new URLSearchParams()) {
    headers.append('Content-Type', 'application/x-www-form-urlencoded');

    return this.http.post(url, data, {
      headers: headers,
      params: params
    })
      .toPromise();
  }

  post(url: string, data: string = '', headers: Headers = new Headers()) {
    return this.postUnmodified(url, data, headers)
      .then(result => {
        let response = result.json() as WPSResponse;

        if (response.status === 'failed') {
          throw response.error;
        }

        return response;
      });
  }

  protected handleError(error: any): Promise<any> {
    return Promise.reject(error.message || error);
  }
}
