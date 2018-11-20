import { Injectable } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import { Observable } from 'rxjs/Observable';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import { Subject } from 'rxjs/Subject';
import { switchMap } from 'rxjs/operators';
import { NEVER } from 'rxjs';

export enum NotificationType {
  Message,
  Warn,
  Error
}

@Injectable()
export class NotificationService {
  notification$ = new Subject<any>();

  toggles: {stream: any, toggle: Subject<boolean>}[] = [];

  subscribe(fn: any): any {
    /**
     * The goal is to have the latest subscriber recieving notifications.
     * For the moment it's assumed that the latest subscriber will call
     * unsubscribe. It's possible for another component to unsunscribe
     * for another component. 
     */
    let toggle = new Subject<boolean>();

    let stream = toggle
      .pipe(switchMap((value: boolean) => value ? this.notification$ : NEVER));

    let streamSub = stream.subscribe(fn);

    if (this.toggles.length > 0) {
      this.toggles[this.toggles.length-1].toggle.next(false);
    }

    this.toggles.push({stream: stream, toggle: toggle});

    toggle.next(true);

    return streamSub;
  }

  unsubscribe() {
    let last = this.toggles.pop();

    last.toggle.next(false);

    if (this.toggles.length > 0) {
      this.toggles[this.toggles.length-1].toggle.next(true);
    }
  }

  message(text: string, link: string = null) {
    this.notification$.next({type: NotificationType.Message, text: text, link: link});
  }

  warn(text: string) {
    this.notification$.next({type: NotificationType.Warn, text: text});
  }

  error(text: string) {
    this.notification$.next({type: NotificationType.Error, text: text});
  }
}
