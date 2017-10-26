import { Component, OnInit } from '@angular/core';

import { Header } from '../shared/pagination.component';
import { ProcessStat, StatsService } from '../shared/stats.service';

@Component({
  selector: 'user-processes',
  styleUrls: ['../forms.css'],
  templateUrl: './user-processes.component.html',
})
export class UserProcessesComponent implements OnInit {
  headers = [
    new Header('Identifier', 'identifier'),
    new Header('Backend', 'backend'),
    new Header('Requested', 'requested'),
    new Header('Last Requested', 'requested_date')
  ];

  processes: Promise<ProcessStat[]>;

  constructor(
    private statsService: StatsService
  ) { }

  ngOnInit() {
    this.processes = this.statsService.userProcesses();
  }
}
