import { Component, OnInit } from '@angular/core';

import { Header } from './pagination.component';
import { WPSService } from './wps.service';

@Component({
  selector: 'stats-processes',
  styleUrls: ['./forms.css'],
  templateUrl: './stats-processes.component.html'
})
export class StatsProcessesComponent implements OnInit {
  processes: Promise<any[]>;
  headers = [
    new Header('Identifier', 'identifier'),
    new Header('Backend', 'backend'),
    new Header('Executed', 'executed'),
    new Header('Success', 'success'),
    new Header('Failed', 'failed'),
    new Header('Retry', 'retry')
  ];

  constructor(
    private wpsService: WPSService
  ) { }

  ngOnInit() {
    this.processes = this.wpsService.statsProcesses()
      .then(response => {
        return response.data.processes.map((value: any) => {
          for (let key in value) {
            if (value[key] === null) {
              value[key] = 'None';
            }
          }

          return value;
        });
      });
  }
}
