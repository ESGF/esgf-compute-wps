import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';

export enum NotificationType {
  Message,
  Warn,
  Error
}

@Injectable()
export class NotificationService {
  private notificationSource = new Subject<any>();

  notification$ = this.notificationSource.asObservable();

  message(text: string) {
    this.notificationSource.next({type: NotificationType.Message, text: text});
  }

  warn(text: string) {
    this.notificationSource.next({type: NotificationType.Warn, text: text});
  }

  error(text: string) {
    this.notificationSource.next({type: NotificationType.Error, text: text});
  }
}
