import { Injectable } from '@angular/core';
import { Http, URLSearchParams } from '@angular/http';

import { WPSService, WPSResponse } from '../core/wps.service';
import { Job, Status, Message } from './job';
import { ConfigService } from '../core/config.service';

export class User {
  constructor(
    public username: string = '',
    public openID: string = '',
    public email: string = '',
    public api_key: string = '',
    public type: string = '',
    public admin?: boolean,
    public local_init?: boolean,
    public expires?: number,
  ) { }

  toUrlEncoded(): string {
    let params = '';
    let fields = ['email'];

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
    private configService: ConfigService,
  ) { 
    super(http); 
  }

  formatStatus(value: Status) {
    let p = new DOMParser();

    if (value.output !== null) {
      value.output = JSON.parse(value.output);
      //let xmlDoc = p.parseFromString(value.output, 'text/xml');

      //let elements = xmlDoc.getElementsByTagName('ows:ComplexData');

      //if (elements.length > 0) {
      //  let variable = JSON.parse(elements[0].innerHTML);

      //  value.output = variable.uri;
      //}
    } else if (value.exception != null) {
      let xmlDoc = p.parseFromString(value.exception, 'text/xml');

      let elements = xmlDoc.getElementsByTagName('ows:ExceptionText');

      if (elements.length > 0) {
        value.exception = elements[0].innerHTML;
      }
    }
  }

  jobDetails(id: number, update: boolean = false): Promise<Status[]> {
    let params = new URLSearchParams();

    params.append('update', update.toString());

    return this.get(`${this.configService.jobsPath}${id}/`, params)
      .then((response: WPSResponse) => {
        let status = response.data as Status[];

        try {
          status.forEach((value: Status) => {
            this.formatStatus(value);
          });
        } catch(err) { }

        return status;
      });
  }

  jobs(): Promise<Job[]> {
    return this.get(this.configService.jobsPath)
      .then((response: WPSResponse) => {
        return response.data.map((data: any) => new Job(data));
      });
  }

  update(user: User): Promise<WPSResponse> {
    return this.postCSRF(this.configService.authUpdatePath, user.toUrlEncoded());
  }

  regenerateKey(): Promise<WPSResponse> {
    return this.getCSRF(this.configService.authUserRegenPath);
  }
}
