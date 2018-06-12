import { Component, OnInit } from '@angular/core';
import { Router, NavigationExtras } from '@angular/router';

import { User } from '../user/user.service';
import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';
import { ConfigService } from '../core/config.service';

@Component({
  templateUrl: './login.component.html',
  styleUrls: ['../forms.css']
})
export class LoginComponent implements OnInit {
  model: any = { username: '', password: '' };
  next: string;

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
    private configService: ConfigService,
    private router: Router
  ) { }

  ngOnInit() { 
    this.authService.isLoggedIn$.subscribe((value: boolean) => {
      if (value) {
        let redirect = this.authService.redirectUrl || this.configService.profilePath;

        this.router.navigateByUrl(redirect);
      }
    });
  }

  onSubmit() {
    this.authService.login(this.model.username, this.model.password)
      .catch(error => {
        this.notificationService.error(error); 
      });
  }
}
