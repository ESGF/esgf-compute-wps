import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Params } from '@angular/router';

import { User } from '../user/user.service';
import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';

@Component({
  template: `
    <div class="container">
      <form (ngSubmit)="onSubmit()" #recoverForm="ngForm">
        <div class="form-group">
          <label for="username">Username</label>
          <input type="text" class="form-control" id="username" required [(ngModel)]="model.username" name="username">
        </div>
        <button type="submit" class="btn btn-success">Retrieve Password</button>        
      </form>
    </div>
  `
})
export class ForgotPasswordComponent {
  model: User = new User();

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService
  ) { }

  onSubmit() {
    this.authService.forgotPassword(this.model.username)
      .then(response => {
        this.notificationService.message('Sent reset password email');
      })
      .catch(error => {
        this.notificationService.error(error);
      });
  }
}
