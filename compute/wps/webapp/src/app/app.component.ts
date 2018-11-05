import { Component, OnInit } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';

import { Subscription } from 'rxjs/Subscription';

import 'rxjs/add/operator/filter';

import { AuthService } from './core/auth.service';
import { ConfigService } from './core/config.service';
import { User } from './user/user.service';
import { WPSService, WPSResponse } from './core/wps.service';

@Component({
  selector: 'my-app',
  templateUrl: './app.component.html',
})

export class AppComponent implements OnInit { 
  admin: boolean = false;
  logged: boolean = false;

  loggedSub: Subscription;

  clearTimer: any;

  constructor(
    private router: Router,
    private authService: AuthService,
    private configService: ConfigService,
    private wpsService: WPSService
  ) { }

  ngOnInit() {
    this.authService.user$.subscribe((user: User) => {
      if (user != null) {
        this.admin = user.admin;
      }
    });

    this.loggedSub = this.authService.isLoggedIn$.subscribe((value: boolean) => {
      this.logged = value;
    });
  }
}
