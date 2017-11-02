import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router, NavigationExtras } from '@angular/router';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';

@Component({ 
  template: '',
})
export class LoginCallbackComponent implements OnInit {
  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
    private router: Router
  ) { }

  ngOnInit() {
    this.authService.isLoggedIn$.subscribe((value: boolean) => {
      if (value) {
        this.router.navigate(['/wps/home/user/profile']);

        this.notificationService.message('Successfully authenticated to ESGF OpenID');
      }
    });
  }
}

