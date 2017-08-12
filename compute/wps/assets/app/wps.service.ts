import { Injectable } from '@angular/core';
import { Http } from '@angular/http';

export class Job {
  id: number;
  elapsed: string;
  accepted: string[];
  status: any;
}

export class Status {
  created_date: string;
  status: string;
  output: string;
  exception: string;
  messages: Message[];
}

export class Message {
  created_date: string;
  percent: number;
  message: string;
}

@Injectable()
export class WPSService {
  constructor(private http: Http) { }

  status(jobID: number): Promise<Status[]> {
    return this.http.get(`/wps/jobs/${jobID}`)
      .toPromise()
      .then(response => response.json().data as Status[])
      .catch(this.handleError);
  }

  jobs(): Promise<Job[]> {
    return this.http.get('/wps/jobs')
      .toPromise()
      .then(response => response.json().data as Job[])
      .catch(this.handleError);
  }
  
  private handleError(error: any): Promise<any> {
    return Promise.reject(error.message || error);
  }
}
