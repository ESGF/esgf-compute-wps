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
  MAX_EMPTY_RESPONSES = 4;
  UPDATE_TIMEOUT: number = 2 * 1000;

  headers = [
    new Header('Created', 'created_date'),
    new Header('Elapsed', 'elapsed')
  ];

  emptyUpdates: number = 0;
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
    this.stopUpdateTimer();
  }

  startUpdateTimer() {
    if (this.updateTimer !== null) {
      clearInterval(this.updateTimer);
    }

    this.updateTimer = setInterval(() => { this.updateJob(); }, this.UPDATE_TIMEOUT);
  }

  stopUpdateTimer() {
    if (this.updateTimer !== null) {
      this.emptyUpdates = 0;

      clearInterval(this.updateTimer);
    }
  }

  updateJob() {
    this.userService.jobDetails(this.selectedJob.id, true)
      .then(details => {
        if (this.emptyUpdates >= this.MAX_EMPTY_RESPONSES) {
          this.emptyUpdates = 0;

          this.stopUpdateTimer();

          this.notificationService.message(`Ending status update, received ${this.MAX_EMPTY_RESPONSES} empty updates`);

          return;
        }

        if (details.length === 0) {
          this.emptyUpdates += 1;

          return;
        }
        
        if (this.selectedJob.status == null) {
          this.selectedJob.status = details;
        } else {
          this.selectedJob.updateStatus(details);
        }
      });
  }

  selectJob(value: Job) {
    this.selectedJob = value;

    if (this.selectedJob.status != null) {
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
