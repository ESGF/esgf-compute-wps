import { Component, OnInit, OnDestroy } from '@angular/core';

import { Job, Status, WPSService } from './wps.service';
import { NotificationService } from './notification.service';

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
      margin: .2em;
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
export class JobsComponent implements OnInit, OnDestroy { 
  PAGES: number = 3;
  ITEMS_PER_PAGE: number = 10;

  page: number = 0;
  pageIndex: number = 0;
  pageNumbers: Array<number>;
  itemCount: number;

  selectedJob: Job;
  jobs: Job[] = new Array<Job>();
  updateTimer: any;

  constructor(
    private wps: WPSService,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.wps.jobs(0, null)
      .then(response => {
        if (response.status === 'success') {
          this.itemCount = response.count;

          let pageCount = Math.ceil(this.itemCount / this.ITEMS_PER_PAGE);

          this.pageNumbers = new Array(pageCount).fill(0).map((x: number, i: number) => i + 1);

          this.jobs = response.jobs;

          if (this.jobs.length > 0) this.setJob(this.jobs[0]);
        } else {
          this.notificationService.error(`Failed to retrieve jobs: "${response.erorr}"`);
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

  onClick(job: Job) {
    this.setJob(job);
  }

  setPage(page: number) {
    this.page = page;
  }

  onChangePage(direction: number) {
    if (direction > 0) {
      if (this.pageIndex + this.PAGES < this.pageNumbers.length) {
        this.pageIndex += direction;
      }
    } else {
      if (this.pageIndex > 0) {
        this.pageIndex += direction;
      }
    }

    this.page += direction;

    if (this.page < 0) {
      this.page = 0;
    } else if (this.page > this.pageNumbers.length - 1) {
      this.page = this.pageNumbers.length - 1;
    }
  }

  onRemoveAll() {
    this.wps.removeAll()
      .then(response => this.handleRemoveAll(response));
  }

  setJob(job: Job) {
    this.selectedJob = job;

    if (this.selectedJob.status === undefined) {
      this.wps.status(job.id)
        .then(response => this.handleStatus(response));
    }
  }

  onRemoveJob(jobID: number) {
    this.wps.remove(this.selectedJob.id)
      .then(response => this.handleRemoveJob(response, this.selectedJob.id));
  }

  handleRemoveAll(response: any) {
    if (response.status === 'success') {
      this.selectedJob = null;

      this.jobs = new Array<Job>();

      this.notificationService.message('Successfully removed all jobs');
    } else {
      this.notificationService.error(`Failed to remove all jobs: "${response.error}"`); 
    }
  }

  handleRemoveJob(response: any, jobID: number) {
    if (response.status === 'success') {
      for (let i = 0; i < this.jobs.length; i++) {
        if (this.jobs[0].id === jobID) {
          this.jobs.splice(i, 1);

          break;
        }
      }
      
      this.notificationService.message('Successfully remove job');
    } else {
      this.notificationService.error(`Error removing job: ${response.error}`);
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
