import { Component, OnInit, OnDestroy } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';

import { Subscription } from 'rxjs/Subscription';

import 'rxjs/add/operator/filter';

import { AuthService } from './auth.service';
import { NotificationType, NotificationService } from './notification.service';
import { WPSService, WPSResponse } from './wps.service';

@Component({
  selector: 'my-app',
  templateUrl: './app.component.html',
  providers: [
    AuthService,
    WPSService
  ]
})

export class AppComponent implements OnInit, OnDestroy { 
  MESSAGE_TIMEOUT: number = 10 * 1000;
  NOTIFICATION_TIMEOUT: number = 10 * 1000;

  admin: boolean = false;
  logged: boolean = false;

  loggedSub: Subscription;

  notificationSub: Subscription;

  notification: string = '';
  message: string = '';
  warn: string = '';
  error: string = '';

  clearTimer: any;
  notificationTimer: any;

  constructor(
    private router: Router,
    private authService: AuthService,
    private notificationService: NotificationService,
    private wpsService: WPSService
  ) { }

  ngOnInit() {
    this.router.events.filter((event) => event instanceof NavigationEnd)
      .subscribe((event) => {
        this.clear();
      });

    this.loggedSub = this.authService.logged.subscribe((user) => {
      if (user !== null) {
        this.logged = true;

        this.admin = user.admin;
      } else {
        this.logged = false;

        this.admin = false;
      }
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

    this.startNotificationTimer();
  }

  ngOnDestroy() {
    this.stopNotificationTimer();

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

  startNotificationTimer() {
    this.getNotification();

    this.notificationTimer = setInterval(() => this.getNotification(), this.NOTIFICATION_TIMEOUT);
  }

  stopNotificationTimer() {
    if (this.notificationTimer) {
      clearInterval(this.notificationTimer);

      this.notificationTimer = null;
    }
  }

  getNotification() {
    this.wpsService.notification()
      .then(response => {
        if (response.status === 'success') {
          this.notification = response.data.notification || '';
        } else {
          this.notification = '';
        }
      });
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
