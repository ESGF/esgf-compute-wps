import { Injectable } from '@angular/core';
import { Http } from '@angular/http';

import { WPSService, WPSResponse } from '../core/wps.service';
import { Job, Status, Message } from './job';

export class User {
  constructor(
    public username: string = '',
    public openid: string = '',
    public email: string = '',
    public api_key: string = '',
    public type: string = '',
    public admin?: boolean,
    public local_init?: boolean,
    public expires?: number,
    public password?: string
  ) { }

  toUrlEncoded(): string {
    let params = '';
    let fields = ['username', 'openid', 'email', 'password'];

    for (let k of fields) {
      if (this[k] != undefined) {
        params += `${k.toLowerCase()}=${this[k]}&`;
      }
    }

    return params;
  }
}

@Injectable()
export class UserService extends WPSService {
  constructor(
    http: Http,
  ) { 
    super(http); 
  }

  formatStatus(value: Status) {
    let p = new DOMParser();

    if (value.output !== null) {
      let xmlDoc = p.parseFromString(value.output, 'text/xml');

      let elements = xmlDoc.getElementsByTagName('ows:ComplexData');

      if (elements.length > 0) {
        let variable = JSON.parse(elements[0].innerHTML);

        value.output = variable.uri;
      }
    } else if (value.exception != null) {
      let xmlDoc = p.parseFromString(value.exception, 'text/xml');

      let elements = xmlDoc.getElementsByTagName('ows:ExceptionText');

      if (elements.length > 0) {
        value.exception = elements[0].innerHTML;
      }
    }
  }

  jobDetails(id: number): Promise<Status[]> {
    return this.get(`wps/jobs/${id}`)
      .then((response: WPSResponse) => {
        if (response.status === 'success') {
          let status = response.data as Status[];

          status.forEach((value: Status) => {
            this.formatStatus(value);
          });

          return status;
        }

        return null;
      });
  }

  jobs(offset: number, items: number): Promise<Job[]> {
    return this.get('wps/jobs')
      .then((response: WPSResponse) => {
        if (response.status === 'success') {
          return response.data.jobs as Job[];
        }

        return [];
      });
  }

  update(user: User): Promise<WPSResponse> {
    return this.postCSRF('auth/update', user.toUrlEncoded());
  }

  regenerateKey(): Promise<WPSResponse> {
    return this.getCSRF('auth/user/regenerate');
  }
}
