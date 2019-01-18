import { Component, OnInit, OnDestroy } from '@angular/core';

import { Subscription } from 'rxjs/Subscription';

import { NotificationService, NotificationType } from './notification.service';

@Component({
  selector: 'notification',
  template: `
  <div *ngIf="message" class="alert alert-dismissible alert-success" role="alert">
    <button type="button" class="close" (click)="message = ''"><span aria-hidden="true">&times;</span></button>
    <a *ngIf="messageLink !== null; else noLink" routerLink="{{messageLink}}" class="alert-link">{{message}}</a>
    <ng-template #noLink>{{message}}</ng-template>
  </div>
  <div *ngIf="warn" class="alert alert-dismissible alert-warning" role="alert">
    <button type="button" class="close" (click)="warn = ''"><span aria-hidden="true">&times;</span></button>
    {{warn}}
  </div>
  <div *ngIf="error" class="alert alert-dismissible alert-danger" role="alert">
    <button type="button" class="close" (click)="error = ''"><span aria-hidden="true">&times;</span></button>
    {{error}}
  </div>
  `,
})
export class NotificationComponent implements OnInit, OnDestroy {
  TIMEOUT = 4*1000;

  notificationSub: Subscription;  

  message: string;
  messageLink: string;
  warn: string;
  error: string;

  messageTimer: any;
  warnTimer: any;
  errorTimer: any;

  constructor(
    private notificationService: NotificationService,
  ) { }

  ngOnInit() { }

  startMessageTimer() {
    if (this.messageTimer) {
      clearInterval(this.messageTimer);
    }

    this.messageTimer = setInterval(() => {
      this.message = '';

      clearInterval(this.messageTimer);

      this.messageTimer = null;
    }, this.TIMEOUT);
  }

  startWarnTimer() {
    if (this.warnTimer) {
      clearInterval(this.warnTimer);
    }

    this.warnTimer = setInterval(() => {
      this.warn = '';

      clearInterval(this.warnTimer);

      this.warnTimer = null;
    }, this.TIMEOUT);
  }

  startErrorTimer() {
    if (this.errorTimer) {
      clearInterval(this.errorTimer);
    }

    this.errorTimer = setInterval(() => {
      this.error = '';

      clearInterval(this.errorTimer);

      this.errorTimer = null;
    }, this.TIMEOUT);
  }

  unsubscribe() {
    this.notificationService.unsubscribe();
  }

  subscribe() {
    this.notificationSub = this.notificationService.subscribe((value: any) => {
      if (value ===  null) return;

      switch(value.type) {
      case NotificationType.Message:
          this.message = value.text;
          this.messageLink = value.link;

          this.startMessageTimer();

          break;
      case NotificationType.Warn:
          this.warn = value.text;

          this.startWarnTimer();

          break;
      case NotificationType.Error:
          this.error = value.text;

          this.startErrorTimer();

          break;
      }
    });
  }

  ngOnDestroy() {
    if (this.messageTimer) {
      clearInterval(this.messageTimer);
    }

    if (this.warnTimer) {
      clearInterval(this.warnTimer);
    }

    if (this.errorTimer) {
      clearInterval(this.errorTimer);
    }

    if (this.notificationSub) {
      this.notificationSub.unsubscribe();
    }
  }
}
