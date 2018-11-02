import { Injectable } from '@angular/core';
import { Http, URLSearchParams, RequestOptionsArgs, Headers } from '@angular/http';
import { Params } from '@angular/router';

import { Process } from '../configure/process';
import { AuthService } from '../core/auth.service';

export interface WPSResponse {
  status: string;
  error?: string;
  data?: any;
}

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

      let metadata = Array.from(xmlDoc.getElementsByTagNameNS(OWS_NS, 'Metadata')).map((item: any) => {
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

      return {
        abstract: abstracts[0] || 'Not available',
        metadata: metadata,
      };
    });
  }

  execute(url: string, api_key: string, process: Process): Promise<string> {
    try {
      process.validate();
    } catch(err) {
      return Promise.reject(err);
    }

    let data = this.prepareDataInputsXML(process);

    let params = new URLSearchParams();

    params.append('api_key', api_key);

    return this.postCSRFUnmodified(url, data, params=params).then((response: any) => {
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

  createAttribute(doc: any, node: any, name: string, value: string) {
    let attribute = doc.createAttribute(name);

    attribute.value = value;

    node.setAttributeNode(attribute);
  }

  createAttributeNS(doc: any, node: any, ns: string, name: string, value: string) {
    let attribute = doc.createAttributeNS(ns, name);

    attribute.value = value;

    node.setAttributeNode(attribute);
  }

  createElementNS(doc: any, node: any, ns: string, name: string, value: string = null) {
    let newNode = doc.createElementNS(ns, name);

    if (value != null) {
      newNode.innerHTML = value;
    }

    node.appendChild(newNode);

    return newNode;
  }

  prepareDataInputs(process: Process): any {
    let data = {
      variable: JSON.stringify(process.getVariable()),
      domain: JSON.stringify(process.getDomain()),
      operation: JSON.stringify(process.getOperation()),
    };

    return data;
  }

  prepareDataInputsString(process: Process): string {
    let data = this.prepareDataInputs(process);

    return `[variable=${data.variable}|domain=${data.domain}|operation=${data.operation}]`;
  }

  prepareDataInputsXML(process: Process): string {
    let dataInputs = this.prepareDataInputs(process);

    let doc = document.implementation.createDocument(WPS_NS, 'wps:Execute', null);

    let root = doc.documentElement;

    this.createAttribute(doc, root, 'service', 'WPS');
    this.createAttribute(doc, root, 'version', '1.0.0');

    this.createElementNS(doc, root, OWS_NS, 'ows:Identifier', process.identifier);

    let dataInputsElement = this.createElementNS(doc, root, WPS_NS, 'wps:DataInputs');

    for (let key in dataInputs) {
      let inputElement = this.createElementNS(doc, dataInputsElement, WPS_NS, 'wps:Input');

      this.createElementNS(doc, inputElement, OWS_NS, 'ows:Identifier', key);
      
      this.createElementNS(doc, inputElement, OWS_NS, 'ows:Title', key);

      let dataElement = this.createElementNS(doc, inputElement, WPS_NS, 'wps:Data');

      this.createElementNS(doc, dataElement, WPS_NS, 'wps:LiteralData', dataInputs[key]);
    }

    return new XMLSerializer().serializeToString(doc.documentElement);
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
