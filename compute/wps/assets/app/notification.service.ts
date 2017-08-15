import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';

@Injectable()
export class NotificationService {
  private errorSource = new Subject<string>();

  error$ = this.errorSource.asObservable();

  error(text: string) {
    this.errorSource.next(text);
  }
}
