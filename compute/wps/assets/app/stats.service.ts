import { Injectable } from '@angular/core';
import { Http } from '@angular/http';

export interface FileStat {
  name: string;
  host: string;
  url: string;
  variable: string;
  requested: number;
  requested_date: any;
}

export interface Stats {
  files: Array<FileStat>;
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

  private handleError(error: any): Promise<any> {
    return Promise.reject(error.message || error);
  }
}
