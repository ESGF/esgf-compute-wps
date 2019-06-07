import { Injectable } from '@angular/core';
import { Http, URLSearchParams, RequestOptions, Headers } from '@angular/http';
import { Params } from '@angular/router';

import { Process } from '../configure/process';
import { Variable } from '../configure/variable';
import { AuthService } from '../core/auth.service';

export interface WPSResponse {
  status: string;
  error?: string;
  data?: any;
}

const XML_NS = 'http://www.w3.org/2000/xmlns/';
const WPS_NS = 'http://www.opengis.net/wps/1.0.0';
const OWS_NS = 'http://www.opengis.net/ows/1.1';
const XLINK_NS = 'http://www.w3.org/1999/xlink';
const XSI_NS = 'http://www.w3.org/2001/XMLSchema-instance';
const SCHEMA_LOCATION = 'http://www.opengis.net/wps/1.0.0/wpsExecute_request.xsd';

@Injectable()
export class WPSService {
  constructor(
    protected http: Http,
  ) { }

  getCapabilities(url: string): Promise<string[]> {
    let params = {
      service: 'WPS',
      request: 'GetCapabilities',
    };

    return this.getUnmodified(url, params).then((response: any) => {
      let parser = new DOMParser();

      let xmlDoc = parser.parseFromString(response.text(), 'text/xml');

      let processes = Array.from(xmlDoc.getElementsByTagNameNS(WPS_NS, 'Process')).map((item: any) => {
        let identifier = item.getElementsByTagNameNS(OWS_NS, 'Identifier');

        return identifier[0].innerHTML;
      });

      return processes.sort();
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

      let abstracts = Array.from(xmlDoc.getElementsByTagNameNS(OWS_NS, 'Abstract')).map((item: any) => {
        return item.innerHTML.replace(/^\n+|\n+$/g, '');
      });

      let metadata = {};

      try {
        metadata = Array.from(xmlDoc.getElementsByTagNameNS(OWS_NS, 'Metadata')).map((item: any) => {
          let data = item.attributes.getNamedItemNS(XLINK_NS, 'title').value.split(':');
          let result = {};
          let name = data[0];

          if (data[1] == '*') {
            result[name] = Infinity;
          } else {
            result[name] = data[1];
          }

          return result;
        }).reduce((a: any, b: any) => {
          return Object.assign(a, b); 
        });
      } catch (err) { }

      return {
        abstract: abstracts[0] || 'Not available',
        metadata: metadata,
      };
    });
  }

  execute(url: string, api_key: string, processes: Process[]): Promise<string> {
    let data = this.prepareDataInputsXML(processes);

    return this.postCSRFUnmodified(url, data, new Headers({'COMPUTE-TOKEN': api_key})).then((response: any) => {
      let parser = new DOMParser();

      let xmlDoc = parser.parseFromString(response.text(), 'text/xml');

      let exception = xmlDoc.getElementsByTagNameNS(OWS_NS, 'Exception');

      if (exception != null && exception.length > 0) {
        let exceptionCode = exception[0].attributes.getNamedItem('exceptionCode').value;

        let exceptionText = exception[0].getElementsByTagNameNS(OWS_NS, 'ExceptionText');

        let exceptionData = `${exceptionCode}`;

        if (exceptionText != null && exceptionText.length > 0) {
          exceptionData = `${exceptionData}: ${exceptionText[0].innerHTML}`;
        }

        return Promise.reject(exceptionData);
      }

      let status = xmlDoc.getElementsByTagNameNS(WPS_NS, 'Status');

      let creationTime = status[0].attributes.getNamedItem('creationTime');

      let state = status[0].children[0].localName;

      return Promise.resolve(`Process entered state "${state}" as of ${creationTime.value}`);
    });
  }

  dataInputJSON(dataInput: any) {
    return JSON.stringify(Object.keys(dataInput).map((key: string) => dataInput[key]));
  }

  prepareDataInputs(processes: Process[]): any {
    let variables = {};
    let domains = {};
    let operations = {};

    for (let process of processes) {
      operations[process.uid] = process.toJSON();

      if (process.domain != null && process.domain.isValid() && !(process.domain.uid in domains)) {
        domains[process.domain.uid] = process.domain.toJSON(); 
      }

      for (let input of process.inputs) {
        if (input instanceof Variable) {
          variables[input.uid] = input.toJSON();
        }
      }
    }

    return { 
      variable: this.dataInputJSON(variables),
      domain: this.dataInputJSON(domains),
      operation: this.dataInputJSON(operations),
    };
  }

  prepareDataInputsXML(processes: Process[]): string {
    let dataInputs = this.prepareDataInputs(processes);

    let dom = document.implementation.createDocument('', '', null);

    let root = dom.createElementNS(WPS_NS, 'wps:Execute');
    dom.appendChild(root);

    root.setAttributeNS(XML_NS, 'xmlns:ows', OWS_NS);
    root.setAttributeNS(XML_NS, 'xmlns:xlink', XLINK_NS);
    root.setAttributeNS(XML_NS, 'xmlns:xsi', XSI_NS);
    root.setAttributeNS(XSI_NS, 'xsi:schemaLocation', SCHEMA_LOCATION);

    root.setAttribute('service', 'WPS');
    root.setAttribute('version', '1.0.0');

    let identifierElem = dom.createElementNS(OWS_NS, 'ows:Identifier');

    identifierElem.innerHTML = processes[0].identifier;

    root.appendChild(identifierElem);

    let dataInputsElem = dom.createElementNS(WPS_NS, 'wps:DataInputs');
    root.appendChild(dataInputsElem);

    for (let key in dataInputs) {
      let inputElem = dom.createElementNS(WPS_NS, 'wps:Input');
      dataInputsElem.appendChild(inputElem);

      let inputIdElem = dom.createElementNS(OWS_NS, 'ows:Identifier');
      inputIdElem.innerHTML = key;
      inputElem.appendChild(inputIdElem);

      let inputTitleElem = dom.createElementNS(OWS_NS, 'ows:Title');
      inputTitleElem.innerHTML = key;
      inputElem.appendChild(inputTitleElem);

      let dataElem = dom.createElementNS(WPS_NS, 'wps:Data');
      inputElem.appendChild(dataElem);

      let complexDataElem = dom.createElementNS(WPS_NS, 'wps:ComplexData')
      complexDataElem.innerHTML = dataInputs[key];
      dataElem.appendChild(complexDataElem);
    }

    return new XMLSerializer().serializeToString(root);
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

  delete(url: string) {
    let headers = new Headers();

    headers.append('X-CSRFToken', this.getCookie('csrftoken'));

    return this.http.delete(url, {
      headers: headers,
      withCredentials: true
    })
      .toPromise();
  }

  getCSRF(url: string, params: URLSearchParams = null, headers: Headers = new Headers()) {
    headers.append('X-CSRFToken', this.getCookie('csrftoken'));

    return this.get(url, params, headers);
  }

  getUnmodified(url: string, params: URLSearchParams|Params = null, headers: Headers = new Headers()) {
    return this.http.get(url, {
      params: params,
      headers: headers,
      withCredentials: true
    })
      .toPromise();
  }

  get(url: string, params: URLSearchParams|Params = null, headers: Headers = new Headers()) {
    return this.getUnmodified(url, params, headers)
      .then(result => {
        let response = result.json() as WPSResponse;

        if (response.status === 'failed') {
          return Promise.reject(response.error);
        }
     
        return Promise.resolve(response);
      });
  }

  postCSRF(url: string, data: string = '', headers: Headers = new Headers()) {
    headers.append('X-CSRFToken', this.getCookie('csrftoken'));

    return this.post(url, data, headers);
  }

  postCSRFUnmodified(url: string, data: string = '', headers = new Headers(), params: URLSearchParams = new URLSearchParams()) {
    headers.append('X-CSRFToken', this.getCookie('csrftoken'));

    return this.postUnmodified(url, data, headers, params);
  }

  postUnmodified(url: string, data: string = '', headers = new Headers(), params = new URLSearchParams()) {
    headers.append('Content-Type', 'application/x-www-form-urlencoded');

    return this.http.post(url, data, {
      headers: headers,
      params: params,
      withCredentials: true
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
