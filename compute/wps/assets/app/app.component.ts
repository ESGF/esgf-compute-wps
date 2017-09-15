import { Component, OnInit, OnDestroy } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';

import { Subscription } from 'rxjs/Subscription';

import 'rxjs/add/operator/filter';

import { AuthService } from './auth.service';
import { NotificationService } from './notification.service';

@Component({
  selector: 'my-app',
  templateUrl: './app.component.html',
  providers: [
    AuthService,
  ]
})

export class AppComponent implements OnInit, OnDestroy { 
  logged: boolean = false;
  loggedSub: Subscription;

  text: string = '';
  notification: boolean = false;

  error: boolean = false;
  errorSub: Subscription;

  message: boolean = false;
  messageSub: Subscription;

  clearSub: Subscription;

  constructor(
    private router: Router,
    private authService: AuthService,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.router.events.filter((event) => event instanceof NavigationEnd)
      .subscribe((event) => {
        this.notificationService.clear();
      });

    this.loggedSub = this.authService.logged$.subscribe((logged) => {
      this.logged = logged;
    });

    this.errorSub = this.notificationService.error$.subscribe((text) => {
      this.clear();
    
      this.setText(text);

      this.error = true;

      console.log(`Error notification ${text}`);
    });

    this.messageSub = this.notificationService.message$.subscribe((text) => {
      this.clear();

      this.setText(text);

      this.message = true;

      console.log(`Message notification ${text}`);
    });

    this.clearSub = this.notificationService.clear$.subscribe(() => {
      this.notification = false;
    });
  }

  ngOnDestroy() {
    this.loggedSub.unsubscribe();

    this.errorSub.unsubscribe();

    this.messageSub.unsubscribe();
  }

  setText(text: string) {
    this.notification = true;

    this.text = text;
  }

  clear() {
    this.error = false;

    this.message = false;
  }

  onHide() {
    this.notification = false;
  }
}
