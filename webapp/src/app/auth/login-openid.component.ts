import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';

interface Provider {
  name: string;
  url: string;
}

@Component({
  templateUrl: './login-openid.component.html',
  styleUrls: ['../forms.css']
})
export class LoginOpenIDComponent implements OnInit {
  providers: Provider[] = [];

  model: any = {};

  next: string;
  error: string;

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
    private route: ActivatedRoute,
    private router: Router,
  ) { 
  }

  ngOnInit() {
    this.route.queryParams.subscribe((item: any) => {
      this.next = item['next'] || null;

      this.error = item['error'] || null;

      this.router.navigate(['.'], { relativeTo: this.route });
    });

    if (this.error !== null) {
      this.notificationService.error(this.error);
    }

    this.authService.providers()
      .then((result: {data: Provider[]}) => {
        this.providers = result.data;

        this.model.idp = this.providers[0];
      });
  }

  onSubmit() {
    this.authService.loginOpenID(this.model.idp.url, this.next)
      .then(response => {
        this.redirect(response.data.redirect);
      })
      .catch(error => {
        this.notificationService.error(`Error connecting to OpenID server`);
      });
  }

  redirect(url: string) {
    window.location.replace(url);
  }
}
