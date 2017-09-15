import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';

@Injectable()
export class NotificationService {
  // default timeout 10 seconds
  TIMEOUT = 10 * 1000;

  private errorSource = new Subject<string>();
  private messageSource = new Subject<string>();
  private clearSource = new Subject<null>();

  error$ = this.errorSource.asObservable();
  message$ = this.messageSource.asObservable();
  clear$ = this.clearSource.asObservable();

  clearTimer: any = null;

  startTimer() {
    // Check if timer is already running and reset
    if (this.clearTimer) {
      this.stopTimer();
    }

    this.clearTimer = setInterval(() => this.clear(), this.TIMEOUT); 
  }

  stopTimer() {
    if (this.clearTimer) {
      clearInterval(this.clearTimer);

      this.clearTimer = null;
    }
  }

  clear() {
    this.clearSource.next();
  }

  error(text: string) {
    this.errorSource.next(text);

    this.startTimer();
  }

  message(text: string) {
    this.messageSource.next(text);

    this.startTimer();
  }
}
