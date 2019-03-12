import { Component, OnInit, OnDestroy } from '@angular/core';

import { UserService } from './user.service';
import { NotificationService } from '../core/notification.service';
import { Job, Status, Message } from './job';

@Component({
  templateUrl: './jobs.component.html',
  styleUrls: ['../forms.css']
})
export class JobsComponent implements OnInit, OnDestroy { 
  selectedJob: Job;
  jobs: Job[];
  sortKey = 'accepted_on';
  sortDir = 1;

  constructor(
    private userService: UserService,
    private notificationService: NotificationService
  ) { 
    this.selectedJob = null; 
  }

  ngOnInit() {
    this.userService.jobs()
      .then((item: Job[]) => this.setJobs(item))
      .catch(error => {
        this.notificationService.error(error);

        this.jobs = [];
      });
  }

  ngOnDestroy() { }

  setJobs(jobs: Job[]) {
    if (jobs.length > 0) {
      this.selectJob(jobs[0]);
    }

    this.jobs = jobs;
  }

  nextPage() {
    this.userService.nextJobs()
      .then((item: Job[]) => this.setJobs(item))
      .catch(error => {
        this.notificationService.error(error);
      });
  }

  previousPage() {
    this.userService.previousJobs()
      .then((item: Job[]) => this.setJobs(item))
      .catch(error => {
        this.notificationService.error(error);
      });
  }

  sort(key: string) {
    this.sortKey = key;

    this.jobs = this.jobs.sort((a: any, b: any) => {
      if (a[key] > b[key]) {
        return 1;
      } else if (a[key] < b[key]) {
        return -1;
      } else {
        return 0;
      }
    });

    if (this.sortDir == 1) {
      this.sortDir = -1;
    } else {
      this.sortDir = 1;

      this.jobs.reverse();
    }
  }

  selectJob(value: Job) {
    this.selectedJob = value;

    this.userService.retrieveJobStatus(this.selectedJob);
  }
}
