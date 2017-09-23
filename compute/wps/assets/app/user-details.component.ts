import { Component, OnInit } from '@angular/core';

import { WPSResponse } from './wps.service';
import { AuthService, User } from './auth.service';
import { NotificationService } from './notification.service';

declare var jQuery: any;

@Component({
  selector: 'user-details',
  templateUrl: './user-details.component.html',
  styleUrls: ['./forms.css']
})
export class UserDetailsComponent { 
  model: User = new User();
  mpc: User = new User();
  error: boolean = false;
  errorMessage: string;

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.authService.user()
      .then(response => {
        if (response.status === 'success') {
          this.model = response.data as User;

          if (!this.model.local_init) {
            this.notificationService.warn('Please set a password if you would like to login locally');
          }
        } else {
          this.notificationService.error('Failed to retrieve account details');
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
        if (response.status === 'success') {
          this.model = response.data as User;

          this.notificationService.message('Successfully updated user details');
        } else {
          this.notificationService.error('Failed to update account details');
        }
      });
  }

  onRegenerateKey() {
    this.authService.regenerateKey(this.model)
      .then(response => {
        if (response.status === 'success') {
          this.model.api_key = response.data.api_key;

          this.notificationService.message('Successfully generated a new API key');
        } else {
          this.notificationService.error('Failed to generate a new API key'); 
        }
      });
  }

  onOAuth2() {
    this.authService.oauth2(this.model.openID)
      .then(response => {
        if (response.status === 'success') {
          window.location.replace(response.data.redirect);
        } else {
          this.notificationService.error(`OAuth2 failed with server error "${response.error}"`);
        }
      });
  }

  onMPCSubmit() {
    this.authService.myproxyclient(this.mpc)
      .then(response => {
        if (response.status === 'success') {
          this.model.type = response.data.type;

          this.model.api_key = response.data.api_key;

          jQuery('#myproxyclient').modal('hide');

          this.notificationService.message('Successfully authenticated using ESGF MyProxyClient');
        } else {
          this.notificationService.error(`MyProxyClient failed with server error "${response.error}"`);
        }
      });
  }
}
