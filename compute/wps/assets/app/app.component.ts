import { Component, OnDestroy } from '@angular/core';

import { Subscription } from 'rxjs/Subscription';

import { AuthService } from './auth.service';
import { NotificationService } from './notification.service';

@Component({
  selector: 'my-app',
  templateUrl: './app.component.html',
  providers: [
    AuthService,
  ]
})

export class AppComponent implements OnDestroy { 
  logged: boolean = false;
  loggedSub: Subscription;

  error: boolean = false;
  errorMessage: string = undefined;
  errorSub: Subscription;

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService
  ) { 
    this.loggedSub = this.authService.logged$.subscribe(
      logged => {
        this.logged = logged;
    });

    this.errorSub = this.notificationService.error$.subscribe(
      text => {
        this.error = true;

        this.errorMessage = text;
      });
  }

  ngOnDestroy() {
    this.loggedSub.unsubscribe();

    this.errorSub.unsubscribe();
  }

  onHideError(): void {
    if (this.error) this.error = false;
  }
}
