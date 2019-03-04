import { Component, OnInit, ViewChild } from '@angular/core';

import { Subscription } from 'rxjs/Subscription';

import 'rxjs/add/operator/filter';

import { AuthService } from './core/auth.service';
import { ConfigService } from './core/config.service';
import { User } from './user/user.service';
import { WPSService, WPSResponse } from './core/wps.service';
import { NotificationComponent } from './core/notification.component';

@Component({
  selector: 'my-app',
  templateUrl: './app.component.html',
})

export class AppComponent implements OnInit { 
  @ViewChild(NotificationComponent)
  private notificationComponent: NotificationComponent;

  logged: boolean = false;

  loggedSub: Subscription;

  constructor(
    private authService: AuthService,
    private configService: ConfigService,
    private wpsService: WPSService,
  ) { 
    this.authService
      .userDetails();
  }

  ngOnInit() {
    this.notificationComponent.subscribe();

    this.loggedSub = this.authService.isLoggedIn$.subscribe((value: boolean) => {
      this.logged = value;
    });
  }
}
