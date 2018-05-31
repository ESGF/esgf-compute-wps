import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { User } from '../user/user.service';
import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';
import { ConfigService } from '../core/config.service';

@Component({
  templateUrl: './create-user.component.html',
  styleUrls: ['../forms.css']
})

export class CreateUserComponent {
  model: User = new User();

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
    private configService: ConfigService,
    private router: Router
  ) { }

  onSubmit(): void {
    this.authService.create(this.model)
      .then(response => {
        this.router.navigate([this.configService.loginPath]);
      })
      .catch(error => {
        this.notificationService.error(`Failed creating account: "${error}"`);
      });
  }
}
