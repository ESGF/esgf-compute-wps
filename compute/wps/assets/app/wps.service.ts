import { Injectable } from '@angular/core';
import { Http } from '@angular/http';

import { NotificationService } from './notification.service';

export class Job {
  status: Status[];

  constructor(
    public id: number,
    public elapsed: string,
    public accepted: string[]
  ) { }

  update(updates: Status[]): string {
    if (this.status === undefined) {
      this.status = new Array<Status>();
    }

    updates.forEach((s: Status) => {
      let match = this.status.find((i: Status) => i.status === s.status);

      if (match !== undefined) {
        s.messages.forEach((m: Message) => match.messages.push(m));
      } else {
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
