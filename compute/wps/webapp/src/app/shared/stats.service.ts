import { Injectable } from '@angular/core';
import { Http, URLSearchParams } from '@angular/http';

import { WPSService, WPSResponse } from '../core/wps.service';

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

@Injectable()
export class StatsService extends WPSService {
  constructor(
    http: Http
  ) {
    super(http);
  }

  userFiles(): Promise<FileStat[]> {
    return this.get('auth/user/stats')
      .then((response: WPSResponse) => {
        if (response.status === 'success') {
          return response.data.files as FileStat[];
        }

        return [];
      });
  }

  userProcesses(): Promise<ProcessStat[]> {
    let params = new URLSearchParams();

    params.set('stat', 'process');

    return this.get('auth/user/stats', params)
      .then((response: WPSResponse) => {
        if (response.status === 'success') {
          return response.data.processes as ProcessStat[];
        }

        return [];
      });
  }

  files(): Promise<FileStat[]> {
    let params = new URLSearchParams();

    params.set('type', 'files');

    return this.get('wps/admin/stats', params)
      .then((response: WPSResponse) => {
        if (response.status === 'success') {
          return response.data.files as FileStat[];
        }

        return [];
      });
  }

  processes(): Promise<ProcessStat[]> {
    return this.get('wps/admin/stats')
      .then((response: WPSResponse) => {
        if (response.status === 'success') {
          return response.data.processes as ProcessStat[];
        }

        return [];
      });
  }
}
