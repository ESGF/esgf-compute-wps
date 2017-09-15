import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';

@Injectable()
export class NotificationService {
  private errorSource = new Subject<string>();
  private messageSource = new Subject<string>();
  private clearSource = new Subject<null>();

  error$ = this.errorSource.asObservable();
  message$ = this.messageSource.asObservable();
  clear$ = this.clearSource.asObservable();

  clear() {
    this.clearSource.next();
  }

  error(text: string) {
    this.errorSource.next(text);
  }

  message(text: string) {
    this.messageSource.next(text);
  }
}
