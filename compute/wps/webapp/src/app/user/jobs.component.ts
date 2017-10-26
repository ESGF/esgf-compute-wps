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
  headers = [
    new Header('Created', 'created'),
    new Header('Elapsed', 'elapsed')
  ];

  selectedJob: Job;
  jobs: Promise<Job[]>;

  constructor(
    private userService: UserService,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.jobs = this.userService.jobs(0, null)
      .then((jobs: Job[]) => {
        if (jobs.length > 0) {
          this.selectJob(jobs[0]);
        }

        return jobs;
      });
  }

  ngOnDestroy() { }

  selectJob(value: Job) {
    this.selectedJob = value;

    this.userService.jobDetails(this.selectedJob.id)
      .then(details => {
        this.selectedJob.status = details;
      });
  }
}
