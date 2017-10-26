import { Component, OnInit } from '@angular/core';

import { Header } from '../shared/pagination.component';
import { ProcessStat, StatsService } from '../shared/stats.service';

@Component({
  selector: 'stats-processes',
  styleUrls: ['../forms.css'],
  templateUrl: './admin-processes.component.html'
})
export class AdminProcessesComponent implements OnInit {
  headers = [
    new Header('Identifier', 'identifier'),
    new Header('Backend', 'backend'),
    new Header('Executed', 'executed'),
    new Header('Success', 'success'),
    new Header('Failed', 'failed'),
    new Header('Retry', 'retry')
  ];

  processes: Promise<ProcessStat[]>;

  constructor(
    private statsService: StatsService
  ) { }

  ngOnInit() {
    this.processes = this.statsService.processes();
  }
}
