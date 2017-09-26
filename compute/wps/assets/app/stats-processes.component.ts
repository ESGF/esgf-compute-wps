import { Component, OnInit } from '@angular/core';

import { WPSService } from './wps.service';

@Component({
  selector: 'stats-processes',
  styleUrls: ['./forms.css'],
  templateUrl: './stats-processes.component.html'
})
export class StatsProcessesComponent implements OnInit {
  processes: Array<any> = null;

  constructor(
    private wpsService: WPSService
  ) { }

  ngOnInit() {
    this.wpsService.statsProcesses()
      .then(response => {
        this.processes = response.data.processes.map((value: any) => {
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
