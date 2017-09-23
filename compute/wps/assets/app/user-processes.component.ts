import { Component, OnInit } from '@angular/core';

import { Stats, StatsService } from './stats.service';

@Component({
  selector: 'user-processes',
  styleUrls: ['forms.css'],
  templateUrl: './user-processes.component.html',
})
export class UserProcessesComponent implements OnInit {
  stats: Stats = null; 

  constructor(
    private statsService: StatsService
  ) { }

  ngOnInit() {
    this.statsService.processes()
      .then(stats => this.stats = stats)
      .catch(error => console.log(error));
  }
}
