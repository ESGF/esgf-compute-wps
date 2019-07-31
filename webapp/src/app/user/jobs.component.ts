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
  sortDir = -1;
  itemsPP = 10;
  itemsPPChoices = [10, 25, 50, 100];
  defaultSort = 'accepted';

  constructor(
    private userService: UserService,
    private notificationService: NotificationService
  ) { 
    this.selectedJob = null; 
  }

  ngOnInit() {
    this.userService.jobs(`-${this.defaultSort}`, this.itemsPP)
      .then((item: Job[]) => {
        this.setJobs(item);
      })
      .catch(error => {
        this.notificationService.error(error);
      });
  }

  ngOnDestroy() { }

  setJobs(jobs: Job[]) {
    if (jobs.length > 0) {
      this.selectJob(jobs[0]);
    }

    this.jobs = jobs;
  }

  removeAll() {
    this.userService.removeAll()
      .then(() => {
        this.selectedJob = null;

        this.setJobs([]);
      });
  }

  updateLimit(limit: number) {
    this.userService.jobs(this.defaultSort, limit)
      .then((item: Job[]) => {
        this.setJobs(item);

        this.itemsPP = limit;
      })
      .catch(error => {
        this.notificationService.error(error);
      });
  }

  nextPage() {
    if (this.userService.canNextJobs()) {
      this.userService.nextJobs()
        .then((item: Job[]) => {
          this.setJobs(item);
        })
        .catch(error => {
          this.notificationService.error(error);
        });
    }
  }

  previousPage() {
    if (this.userService.canPreviousJobs()) {
      this.userService.previousJobs()
        .then((item: Job[]) => {
          this.setJobs(item);
        })
        .catch(error => {
          this.notificationService.error(error);
        });
    }
  }

  sort(key: string) {
    if (this.sortDir === -1) {
      key = `-${key}`;
    }

    this.userService.jobs(key, this.itemsPP)
      .then((item: Job[]) => {
        this.setJobs(item);

        // only swap direction on success
        this.sortDir *= -1;
      })
      .catch(error => {
        this.notificationService.error(error);
      });
  }

  selectJob(value: Job) {
    event.stopPropagation();

    this.selectedJob = value;

    this.userService.retrieveJobStatus(this.selectedJob);
  }

  deleteJob(value: Job) {
    event.stopPropagation();

    this.selectedJob = null; 

    this.userService.deleteJob(value)
      .then(() => {
        this.jobs = this.jobs.filter((item: Job) => {
          return value.id !== item.id;
        });

        if (this.jobs.length > 0) {
          this.selectJob(this.jobs[0]);
        }
      });
  }
}
