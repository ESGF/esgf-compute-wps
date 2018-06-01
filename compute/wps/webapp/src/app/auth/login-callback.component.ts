import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router, NavigationExtras } from '@angular/router';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';
import { ConfigService } from '../core/config.service';

@Component({ 
  template: '',
})
export class LoginCallbackComponent implements OnInit {
  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
    private configService: ConfigService,
    private activated: ActivatedRoute,
    private router: Router,
  ) { }

  ngOnInit() {
    setTimeout(() => {
      this.activated.queryParams.subscribe((v: any) => {
        if ('expires' in v) {
          this.authService.setExpires(v['expires']);

          this.router.navigate([this.configService.profilePath]);

          this.notificationService.message('Successfully authenticated to ESGF OpenID');
        }
      });
    });
  }
}
