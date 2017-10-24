import { Component, OnInit, OnDestroy } from '@angular/core';

import { Header } from './pagination.component';
import { Job, Status, WPSService } from './wps.service';
import { NotificationService } from './notification.service';

@Component({
  templateUrl: './jobs.component.html',
  styleUrls: ['./forms.css']
})
export class JobsComponent implements OnInit, OnDestroy { 
  selectedJob: Job;
  jobs: Promise<Job[]>;
  updateTimer: any;
  headers = [
    new Header('Created', 'created'),
    new Header('Elapsed', 'elapsed')
  ];

  constructor(
    private wps: WPSService,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.jobs = this.wps.jobs(0, null)
      .then(response => {
        if (response.status === 'success') {
          if (response.jobs.length > 0) this.selectJob(response.jobs[0]);

          return response.jobs;
        } else {
          this.notificationService.error(`Failed to retrieve jobs: "${response.erorr}"`);

          return [];
        }
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

  selectJob(job: Job) {
    this.selectedJob = job;

    if (this.selectedJob.status === undefined) {
      this.wps.status(job.id)
        .then(response => this.handleStatus(response));
    } else {
      if (this.updateTimer) {
        let latest = this.selectedJob.latest()

        if (latest === 'ProcessSucceeded' || latest === 'ProcessFailed') {
          clearInterval(this.updateTimer);
        }
      }
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
