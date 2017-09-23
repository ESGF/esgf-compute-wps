import { Injectable } from '@angular/core';
import { Http, URLSearchParams } from '@angular/http';

export interface FileStat {
  name: string;
  host: string;
  url: string;
  variable: string;
  requested: number;
  requested_date: any;
}

export interface ProcessStat {
  identifier: string;
  backend: string;
  requested: number;
  requested_date: any;
}

export interface Stats {
  files?: Array<FileStat>;
  processes?: Array<ProcessStat>;
}

@Injectable()
export class StatsService {
  constructor(
    private http: Http
  ) { }

  stats(): Promise<Stats> {
    return this.http.get('auth/user/stats')
      .toPromise()
      .then(response => response.json().data as Stats)
      .catch(this.handleError);
  }

  processes(): Promise<Stats> {
    let params = new URLSearchParams();

    params.set('stat', 'process');

    return this.http.get('auth/user/stats', {
      params: params
    })
      .toPromise()
      .then(response => response.json().data as Stats)
      .catch(this.handleError);
  }

  private handleError(error: any): Promise<any> {
    return Promise.reject(error.message || error);
  }
}
