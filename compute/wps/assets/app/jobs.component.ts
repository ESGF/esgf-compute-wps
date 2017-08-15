import { Component, OnInit, OnDestroy } from '@angular/core';

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
  updateTimer: any;

  constructor(private wps: WPSService) { }

  ngOnInit() {
    this.wps.jobs()
      .then(response => {
        this.jobs = response;

        if (this.jobs.length > 0) this.setJob(this.jobs[0]);
      });
  }

  ngOnDestroy() {
    clearInterval(this.updateTimer);
  }

  startJobMonitor() {
    this.updateTimer = setInterval(() => this.updateJob(), 4000);
  }

  updateJob() {
    if (this.selectedJob !== undefined) {
      this.wps.update(this.selectedJob.id)
        .then(response => this.handleStatusUpdate(response));
    }
  }

  onClick(job: Job) {
    this.setJob(job);
  }

  setJob(job: Job) {
    this.selectedJob = job;

    if (this.selectedJob.status === undefined) {
      this.wps.status(job.id)
        .then(response => this.handleStatus(response));
    }
  }

  handleStatus(status: Status[]) {
    let latest = this.selectedJob.update(status);

    if (latest !== 'ProcessSucceeded' && latest !== 'ProcessFailed') {
      this.startJobMonitor();
    }
  }

  handleStatusUpdate(updates: Status[]) {
    let latest = this.selectedJob.update(updates);

    if (latest === 'ProcessSucceeded' || latest === 'ProcessFailed') {
      clearInterval(this.updateTimer);
    }
  }
}
