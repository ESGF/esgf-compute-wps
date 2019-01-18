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
    this.activated.queryParams.subscribe((v: any) => {
      if ('expires' in v) {
        this.authService.setExpires(v['expires']);
      }

      this.authService.userDetails()
        .then(() => {
          if ('next' in v && v['next'] != 'null') {
            window.location.href = v['next'];
          } else {
            this.router.navigate([this.configService.profilePath]);

            this.notificationService.message('Successfully authenticated to ESGF OpenID');
          }
        });
    });
  }
}
