import { Component, OnInit } from '@angular/core';

import { Header } from './pagination.component';
import { ProcessStat, Stats, StatsService } from './stats.service';

@Component({
  selector: 'user-processes',
  styleUrls: ['forms.css'],
  templateUrl: './user-processes.component.html',
})
export class UserProcessesComponent implements OnInit {
  stats: Promise<ProcessStat[]> = null;
  headers = [
    new Header('Identifier', 'identifier'),
    new Header('Backend', 'backend'),
    new Header('Requested', 'requested'),
    new Header('Last Requested', 'requested_date')
  ];

  constructor(
    private statsService: StatsService
  ) { }

  ngOnInit() {
    this.stats = this.statsService.processes()
      .then(stats => stats.processes);
  }
}
