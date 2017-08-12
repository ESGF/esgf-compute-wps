import { Component, OnInit } from '@angular/core';

import { Job, Status, WPSService } from './wps.service';

@Component({
  templateUrl: './jobs.component.html',
  styles: [`
    .selected {
      background-color: #CFD8DC !important;
      color: white;
    }
    .jobs li {
      cursor: pointer;
      background-color: #EEE;
      margin: .5em;
      padding: .3em;
      border-radius: 4px;
    }
    .jobs li:hover {
      left: .1em;
      background-color: #DDD;
    }
    .jobs li.selected:hover {
      background-color: #BBD8DC !important;
      color: white;
    }
    .message {
      margin: 0px 4px;
    }
  `],
  providers: [WPSService]
})
export class JobsComponent implements OnInit { 
  selectedJob: Job;
  jobs: Job[] = new Array<Job>();

  constructor(private wps: WPSService) { }

  ngOnInit() {
    this.wps.jobs()
      .then(response => this.jobs = response);
  }

  onClick(job: Job) {
    this.selectedJob = job;

    if (this.selectedJob.status === undefined) {
      this.wps.status(job.id)
        .then(response => this.selectedJob.status = response);
    }
  }
}
