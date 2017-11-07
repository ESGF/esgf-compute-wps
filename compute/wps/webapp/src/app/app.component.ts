import { Component, OnInit, OnDestroy } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';

import { Subscription } from 'rxjs/Subscription';

import 'rxjs/add/operator/filter';

import { AuthService } from './core/auth.service';
import { NotificationType, NotificationService } from './core/notification.service';
import { User } from './user/user.service';
import { WPSService, WPSResponse } from './core/wps.service';

@Component({
  selector: 'my-app',
  templateUrl: './app.component.html',
})

export class AppComponent implements OnInit, OnDestroy { 
  MESSAGE_TIMEOUT: number = 10 * 1000;
  NOTIFICATION_TIMEOUT: number = 60 * 1000;

  admin: boolean = false;
  logged: boolean = false;

  loggedSub: Subscription;

  notificationSub: Subscription;

  notification: string = '';
  message: string = '';
  messageLink: string = null;
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
    if (this.router.events) {
      this.router.events.filter((event) => event instanceof NavigationEnd)
        .subscribe((event) => {
          this.clear();
        });
    }

    this.authService.user$.subscribe((user: User) => {
      if (user !== undefined) {
        this.admin = user.admin;
      }
    });

    this.loggedSub = this.authService.isLoggedIn$.subscribe((value: boolean) => {
      this.logged = value;
    });

    this.notificationSub = this.notificationService.notification$.subscribe((value: any) => {
      if (value ===  null) return;

      switch(value.type) {
      case NotificationType.Message:
          this.message = value.text;
          this.messageLink = value.link;

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

    if (this.notificationSub) {
      this.notificationSub.unsubscribe();
    }
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
