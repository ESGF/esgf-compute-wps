import { Component, OnInit, OnDestroy } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';

import { Subscription } from 'rxjs/Subscription';

import 'rxjs/add/operator/filter';

import { AuthService } from './auth.service';
import { NotificationType, NotificationService } from './notification.service';

@Component({
  selector: 'my-app',
  templateUrl: './app.component.html',
  providers: [
    AuthService,
  ]
})

export class AppComponent implements OnInit, OnDestroy { 
  MESSAGE_TIMEOUT: number = 10 * 1000;

  logged: boolean = false;
  loggedSub: Subscription;

  notificationSub: Subscription;

  message: string = '';
  warn: string = '';
  error: string = '';

  clearTimer: any;

  constructor(
    private router: Router,
    private authService: AuthService,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.router.events.filter((event) => event instanceof NavigationEnd)
      .subscribe((event) => {
        this.clear();
      });

    this.loggedSub = this.authService.logged$.subscribe((logged) => {
      this.logged = logged;
    });

    this.notificationSub = this.notificationService.notification$.subscribe((value: any) => {
      switch(value.type) {
      case NotificationType.Message:
          this.message = value.text;

          this.startTimer();

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
    this.stopTimer();

    this.notificationSub.unsubscribe();
  }

  startTimer() {
    if (this.clearTimer) {
      this.stopTimer();
    }

    this.clearTimer = setInterval(() => {
      this.message = '';

      this.stopTimer();
    }, this.MESSAGE_TIMEOUT);
  }

  stopTimer() {
    if (this.clearTimer) {
      clearInterval(this.clearTimer);

      this.clearTimer = null;
    }
  }

  clear() { 
    this.hideMessage();

    this.hideWarning();

    this.hideError();
  }

  hideMessage() {
    this.message = '';
  }

  hideWarning() {
    this.warn = '';
  }

  hideError() {
    this.error = '';
  }
}
