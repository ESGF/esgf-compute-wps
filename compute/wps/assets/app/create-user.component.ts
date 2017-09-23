import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { AuthService, User } from './auth.service';
import { NotificationService } from './notification.service';

@Component({
  templateUrl: './create-user.component.html',
  styleUrls: ['./forms.css']
})

export class CreateUserComponent {
  model: User = new User();

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
    private router: Router
  ) { }

  onSubmit(): void {
    this.authService.create(this.model)
      .then(response => this.handleResponse(response));
  }

  handleResponse(response: any): void {
    if (response.status === 'success') {
      this.router.navigate(['/wps/home/login']);
    } else {
      this.notificationService.error(`Failed creating account: "${response.error}"`);
    }
  }
}
