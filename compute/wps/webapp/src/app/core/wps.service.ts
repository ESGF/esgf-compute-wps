import { Injectable } from '@angular/core';
import { Http, URLSearchParams, RequestOptionsArgs, Headers } from '@angular/http';
import { Params } from '@angular/router';

import { Process } from '../configure/process';

export interface WPSResponse {
  status: string;
  error?: string;
  data?: any;
}

@Injectable()
export class WPSService {
  OWS_NS = 'http://www.opengis.net/ows/1.1';
  WPS_NS = 'http://www.opengis.net/wps/1.0.0';
  XLINK_NS = 'http://www.w3.org/1999/xlink';

  constructor(
    protected http: Http
  ) { }

  getCapabilities(url: string): Promise<Process[]> {
    let params = {
      service: 'WPS',
      request: 'GetCapabilities',
    };

    return this.getUnmodified(url, params).then((response: any) => {
      let parser = new DOMParser();

      let xmlDoc = parser.parseFromString(response.text(), 'text/xml');

      let processes = Array.from(xmlDoc.getElementsByTagNameNS(this.WPS_NS, 'Process')).map((item: any) => {
        let identifier = item.getElementsByTagNameNS(this.OWS_NS, 'Identifier');

        return new Process(identifier[0].innerHTML);
      });

      return new Promise<Process[]>((resolve, reject) => {
        resolve(processes);
      });
    });
  }

  describeProcess(url: string, identifier: string): Promise<any> {
    let params = {
      service: 'WPS',
      request: 'DescribeProcess',
      identifier: identifier,
    };

    return this.getUnmodified(url, params).then((response: any) => {
      let parser = new DOMParser();

      let xmlDoc = parser.parseFromString(response.text(), 'text/xml');

      let abstracts = Array.from(xmlDoc.getElementsByTagNameNS(this.OWS_NS, 'Abstract')).map((item: any) => {
        return item.innerHTML.replace(/^\n+|\n+$/g, '');
      });

      let metadata = Array.from(xmlDoc.getElementsByTagNameNS(this.OWS_NS, 'Metadata')).map((item: any) => {
        let data = item.attributes.getNamedItemNS(this.XLINK_NS, 'title').value.split(':');

        return { name: data[0], value: data[1] };
      });

      return new Promise<any>((resolve, reject) => {
        let data = {
          abstract: abstracts[0] || '',
          metadata: Object.assign({}, metadata),
        };

        resolve(data);
      });
    });
  }

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
