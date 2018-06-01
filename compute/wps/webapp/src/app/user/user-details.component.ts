import { Component, OnInit } from '@angular/core';

import { WPSResponse } from '../core/wps.service';
import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';
import { UserService, User } from './user.service';
import { ConfigService } from '../core/config.service';

declare var jQuery: any;

@Component({
  selector: 'user-details',
  templateUrl: './user-details.component.html',
  styleUrls: ['../forms.css']
})
export class UserDetailsComponent { 
  model: User = new User();
  mpc: any = { username: '', password: '' };

  constructor(
    private authService: AuthService,
    private userService: UserService,
    private configService: ConfigService,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.authService.user$.subscribe((value: User) => {
      this.model = value;
    });
  }

  onSubmit(form: any) {
    let user = new User();

    for (let control in form.controls) {
      if (form.controls[control].dirty) {
        user[control] = form.controls[control].value;
      }
    }

    this.userService.update(user)
      .then(response => {
        this.model = response.data as User;

        this.notificationService.message('Successfully updated user details');
      })
      .catch(error => {
        this.notificationService.error('Failed to update account details');
      });
  }

  onRegenerateKey() {
    this.userService.regenerateKey()
      .then(response => {
        jQuery('#regenerateWarning').modal('hide');

        this.model.api_key = response.data.api_key;

        this.notificationService.message('Successfully generated a new API key');
      })
      .catch(error => {
        this.notificationService.error('Failed to generate a new API key'); 
      });
  }

  onOAuth2() {
    this.authService.oauth2(this.model.openID)
      .then(response => {
        window.location.replace(response.data.redirect);
      })
      .catch(error => {
        this.notificationService.error(`OAuth2 failed with server error "${error}"`);
      });
  }

  onMPCSubmit() {
    this.authService.myproxyclient(this.mpc.username, this.mpc.password)
      .then(response => {
        jQuery('#myproxyclient').modal('hide');

        this.model.type = response.data.type;

        this.model.api_key = response.data.api_key;

        this.notificationService.message('Successfully authenticated using ESGF MyProxyClient');
      })
      .catch(error => {
        this.notificationService.error(`MyProxyClient failed with server error "${error}"`);
      });
  }
}
