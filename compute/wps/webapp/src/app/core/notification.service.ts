import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';

export enum NotificationType {
  Message,
  Warn,
  Error
}

@Injectable()
export class NotificationService {
  notification$ = new BehaviorSubject<any>(null);

  message(text: string) {
    this.notification$.next({type: NotificationType.Message, text: text});
  }

  warn(text: string) {
    this.notification$.next({type: NotificationType.Warn, text: text});
  }

  error(text: string) {
    this.notification$.next({type: NotificationType.Error, text: text});
  }
}
