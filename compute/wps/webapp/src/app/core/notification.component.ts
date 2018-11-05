import { Component, OnInit, OnDestroy } from '@angular/core';

import { Subscription } from 'rxjs/Subscription';

import { NotificationService, NotificationType } from './notification.service';

@Component({
  selector: 'notification',
  template: `
  <div *ngIf="message" class="alert alert-dismissible alert-success" role="alert">
    <button type="button" class="close" (click)="hideMessage();"><span aria-hidden="true">&times;</span></button>
    <a *ngIf="messageLink !== null; else noLink" routerLink="{{messageLink}}" class="alert-link">{{message}}</a>
    <ng-template #noLink>{{message}}</ng-template>
  </div>
  <div *ngIf="warn" class="alert alert-dismissible alert-warning" role="alert">
    <button type="button" class="close" (click)="hideWarning();"><span aria-hidden="true">&times;</span></button>
    {{warn}}
  </div>
  <div *ngIf="error" class="alert alert-dismissible alert-danger" role="alert">
    <button type="button" class="close" (click)="hideError();"><span aria-hidden="true">&times;</span></button>
    {{error}}
  </div>
  `,
})
export class NotificationComponent implements OnInit, OnDestroy {
  notificationSub: Subscription;  

  message: string;
  messageLink: string;
  warn: string;
  error: string;

  constructor(
    private notificationService: NotificationService,
  ) { }

  ngOnInit() {
    this.notificationSub = this.notificationService.notification$.subscribe((value: any) => {
      if (value ===  null) return;

      switch(value.type) {
      case NotificationType.Message:
          this.message = value.text;
          this.messageLink = value.link;

          break;
      case NotificationType.Warn:
          this.warn = value.text;

          break;
      case NotificationType.Error:
          this.error = value.text;

          break;
      }
    });
  }

  ngOnDestroy() {
    if (this.notificationSub) {
      this.notificationSub.unsubscribe();
    }
  }
}
