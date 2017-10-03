import { Component, OnInit } from '@angular/core';

import { ProcessStat, Stats, StatsService } from './stats.service';

@Component({
  selector: 'user-processes',
  styleUrls: ['forms.css'],
  templateUrl: './user-processes.component.html',
})
export class UserProcessesComponent implements OnInit {
  stats: Promise<ProcessStat[]> = null;

  constructor(
    private statsService: StatsService
  ) { }

  ngOnInit() {
    this.stats = this.statsService.processes()
      .then(stats => stats.processes);
  }
}
