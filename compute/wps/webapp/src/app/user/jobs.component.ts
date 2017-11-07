import { Component, OnInit, OnDestroy } from '@angular/core';

import { Header } from '../shared/pagination.component';
import { UserService } from './user.service';
import { NotificationService } from '../core/notification.service';
import { Job, Status, Message } from './job';

@Component({
  templateUrl: './jobs.component.html',
  styleUrls: ['../forms.css']
})
export class JobsComponent implements OnInit, OnDestroy { 
  UPDATE_TIMEOUT: number = 30 * 1000;

  headers = [
    new Header('Created', 'created_date'),
    new Header('Elapsed', 'elapsed')
  ];

  updateTimer: any;
  selectedJob: Job;
  jobs: Promise<Job[]>;

  constructor(
    private userService: UserService,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.jobs = this.userService.jobs()
      .then((jobs: Job[]) => {
        if (jobs.length > 0) {
          this.selectJob(jobs[0]);
        }

        return jobs;
      })
      .catch(error => {
        this.notificationService.error(error);

        return [];
      });
  }

  ngOnDestroy() { 
    if (this.updateTimer !== null) {
      clearInterval(this.updateTimer);
    }
  }

  startUpdateTimer() {
    if (this.updateTimer !== null) {
      clearInterval(this.updateTimer);
    }

    setInterval(this.updateJob, this.UPDATE_TIMEOUT);
  }

  updateJob() {
    console.log('test');
  }

  selectJob(value: Job) {
    this.selectedJob = value;

    if (this.selectedJob.status !== undefined) {
      if (!this.selectedJob.completed) {
        this.startUpdateTimer();
      } else {
        if (this.updateTimer !== null) {
          clearInterval(this.updateTimer);
        }
      }

      return;
    }

    this.userService.jobDetails(this.selectedJob.id)
      .then(details => {
        this.selectedJob.status = details;

        if (!this.selectedJob.completed) {
          this.startUpdateTimer();
        }
      })
      .catch(error => {
        this.notificationService.error(`Failed to retrieve job details: ${error}`); 
      });
  }
}
