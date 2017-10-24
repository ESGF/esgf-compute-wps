import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Params } from '@angular/router';

import { AuthService } from './auth.service';
import { NotificationService } from './notification.service';

@Component({
  template: `
    <div class="container">
      <form (ngSubmit)="onSubmit()" #recoverForm="ngForm">
        <div class="form-group">
          <label for="password">Password</label>
          <input type="password" class="form-control" id="password" required [(ngModel)]="model.password" name="password">
        </div>
        <div class="form-group">
          <label for="password2">Repeat Password</label>
          <input type="password" class="form-control" id="password2" required [(ngModel)]="model.password_repeat" name="password_repeat">
        </div>
        <button type="submit" class="btn btn-success">Reset Password</button>        
      </form>
    </div>
  `
})
export class ResetPasswordComponent implements OnInit {
  model: any = {};

  constructor(
    private route: ActivatedRoute,
    private authService: AuthService,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.route.queryParams.subscribe((params: Params) => {
      this.model.token = params['token']; 

      this.model.username = params['username'];
    });
  }

  onSubmit() {
    if (this.model.password !== this.model.password_repeat) {
      this.notificationService.error('Passwords do not match');
    }

    if (this.model.token === undefined) {
      this.notificationService.error('Resetting a password requires a token')
    }

    this.authService.resetPassword(this.model)
      .then(response => {
        if (response.status === 'success') {
          this.notificationService.message('Successfully reset password');

          window.location.replace(response.data.redirect);
        } else {
          this.notificationService.error(response.error);
        }
      });
  }
}

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
  model: any = {};

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService
  ) { }

  onSubmit() {
    this.authService.forgotPassword(this.model.username)
      .then(response => {
        if (response.status === 'success') {
          this.notificationService.message('Sent reset password email');
        } else {
          this.notificationService.error(response.error);
        }
      });
  }
}
