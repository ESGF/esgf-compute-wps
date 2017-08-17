import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { User } from './user';
import { WPSResponse } from './wps.service';
import { AuthService } from './auth.service';
import { NotificationService } from './notification.service';

@Component({
  templateUrl: './update-user-form.component.html',
  styleUrls: ['./forms.css']
})

export class UpdateUserFormComponent { 
  model: User = new User();
  mpc: User = new User();
  error: boolean = false;
  errorMessage: string;

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
    private router: Router
  ) {
    this.authService.user()
      .then(response => {
        if (response.status === 'failed') {
          this.notificationService.error('Failed to retrieve account details');
        } else {
          this.model = response;
        }
      });
  }

  onSubmit(form: any) {
    let user = new User();

    for (let control in form.controls) {
      if (form.controls[control].dirty) {
        user[control] = form.controls[control].value;
      }
    }

    this.authService.update(user)
      .then(response => {
        if (response.status === 'failed') {
          this.notificationService.error('Failed to update account details');
        }
      });
  }

  onRegenerateKey() {
    this.authService.regenerateKey(this.model)
      .then(response => this.handleRegenerateKey(response));
  }

  onOAuth2() {
    this.authService.oauth2(this.model.openID)
      .then(response => this.handleOAuth2(response));
  }

  onMPCSubmit() {
    this.authService.myproxyclient(this.mpc)
      .then(response => this.handleMPC(response));
  }

  handleRegenerateKey(response: WPSResponse) {
    if (response.status === 'failed') {
      this.notificationService.error('Failed to generate a new API key'); 
    } else {
      this.model.api_key = response.api_key;
    }
  }

  handleOAuth2(response: WPSResponse) {
    if (response.status === 'failed') {
      this.notificationService.error('Failed to authenticate');
    } else {
      this.router.navigateByUrl(response.redirect);
    }
  }

  handleMPC(response: WPSResponse) {
    if (response.status === 'failed') {
      this.error = true;

      this.errorMessage = response.errors;
    } else {
      window.location.reload();
    }
  }
}
