import { Injectable } from '@angular/core';
import { Http } from '@angular/http';

import { NotificationService } from './notification.service';

export interface WPSResponse {
  status: string;
  error: string;
  data: any;
}

export class Job {
  status: Status[];

  constructor(
    public id: number,
    public elapsed: string,
    public accepted: string[]
  ) { }

  reformatStatus(status: Status): Status {
    let p = new DOMParser();

    if (status.exception) {
      let xmlDoc = p.parseFromString(status.exception, 'text/xml');

      let elements = xmlDoc.getElementsByTagName('ows:ExceptionText');

      if (elements.length > 0) {
        status.exception = elements[0].innerHTML;
      }
    } else if (status.output) {
      let xmlDoc = p.parseFromString(status.output, 'text/xml');

      let elements = xmlDoc.getElementsByTagName('ows:ComplexData');

      if (elements.length > 0) {
        let variable = JSON.parse(elements[0].innerHTML);

        status.output = variable.uri;
      }
    }

    return status;
  }

  update(updates: Status[]): string {
    if (this.status === undefined) {
      this.status = new Array<Status>();
    }

    console.log(updates);

    updates.forEach((s: Status) => {
      let match = this.status.find((i: Status) => i.status === s.status);

      if (match !== undefined) {
        s.messages.forEach((m: Message) => match.messages.push(m));
      } else {
        s = this.reformatStatus(s);

        this.status.push(s);
      }
    });

    return this.status[this.status.length-1].status || undefined;
  }
}

export interface Status {
  created_date: string;
  status: string;
  output: string;
  exception: string;
  messages: Message[];
}

export interface Message {
  created_date: string;
  percent: number;
  message: string;
}

@Injectable()
export class WPSService {
  constructor(
    private notificationService: NotificationService,
    private http: Http
  ) { }

  status(jobID: number): Promise<Status[]> {
    return this.http.get(`/wps/jobs/${jobID}`)
      .toPromise()
      .then(response => response.json().data as Status[])
      .catch(this.handleError);
  }

  update(jobID: number): Promise<Status[]> {
    return this.http.get(`/wps/jobs/${jobID}?update=true`)
      .toPromise()
      .then(response => response.json().data as Status[])
      .catch(this.handleError);
  }

  jobs(): Promise<Job[]> {
    return this.http.get('/wps/jobs')
      .toPromise()
      .then(response => response.json().data.map((x: any) => new Job(x.id, x.elapsed, x.accepted)))
      .catch(this.handleError);
  }
  
  private handleError(error: any): Promise<any> {
    this.notificationService.error(error.message || error);

    return Promise.reject(error.message || error);
  }
}
