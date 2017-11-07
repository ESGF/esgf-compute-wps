import { Component } from '@angular/core';

import { User } from '../user/user.service';
import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';

@Component({
  template: `
    <div class="container">
      <form  (ngSubmit)="onSubmit()" #recoverForm="ngForm">
        <div class="form-group">
          <label for="email">Email</label>
          <input type="email" class="form-control" id="email" required [(ngModel)]="model.email" name="email">
        </div>
        <button type="submit" class="btn btn-success">Recover</button>
      </form>
    </div>
  `
})
export class ForgotUsernameComponent {
  model: User = new User();

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService
  ) { }

  onSubmit() {
    this.authService.forgotUsername(this.model.email)
      .then(response => {
        this.notificationService.message(`Email with username sent to "${this.model.email}"`);

        this.redirect(response.data.redirect);
      })
      .catch(error => {
        this.notificationService.error(error); 
      });
  }

  redirect(url: string) {
    window.location.replace(url);
  }
}
