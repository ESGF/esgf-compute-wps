import { Injectable } from '@angular/core';
import { Http, URLSearchParams } from '@angular/http';

import { WPSService, WPSResponse } from '../core/wps.service';
import { ConfigService } from '../core/config.service';

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
    http: Http,
    private configService: ConfigService,
  ) {
    super(http);
  }

  userFiles(): Promise<FileStat[]> {
    return this.get(this.configService.authUserStatsPath)
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

    return this.get(this.configService.authUserStatsPath, params)
      .then((response: WPSResponse) => {
        if (response.status === 'success') {
          return response.data.processes as ProcessStat[];
        }

        return [];
      });
  }
}
