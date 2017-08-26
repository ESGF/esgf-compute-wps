import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { User } from './user';
import { WPSResponse } from './wps.service';
import { AuthService } from './auth.service';
import { NotificationService } from './notification.service';

declare var jQuery: any;

@Component({
  templateUrl: './update-user-form.component.html',
  styleUrls: ['./forms.css']
})

export class UpdateUserFormComponent implements OnInit { 
  model: User = new User();
  mpc: User = new User();
  error: boolean = false;
  errorMessage: string;

  constructor(
    private authService: AuthService,
    private notificationService: NotificationService,
    private router: Router
  ) { }

  ngOnInit() {
    this.authService.user()
      .then(response => {
        if (response.status === 'success') {
          this.model = response.data as User;
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
        } else {
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
    if (response.status === 'success') {
      this.model.api_key = response.data.api_key;
    } else {
      this.notificationService.error('Failed to generate a new API key'); 
    }
  }

  handleOAuth2(response: WPSResponse) {
    if (response.status === 'success') {
      this.router.navigateByUrl(response.data.redirect);
    } else {
      this.notificationService.error('Failed to authenticate');
    }
  }

  handleMPC(response: WPSResponse) {
    if (response.status === 'success') {
      this.authService.user()
        .then(response => {
          if (response.status === 'success') {
            this.model = response.data as User;
          } else {
            this.notificationService.error('Failed to retrieve account details');
          }
        });

      jQuery('#myproxyclient').modal('hide');
    } else {
      this.error = true;

      this.errorMessage = response.error;
    }
  }
}
