import { Component } from '@angular/core';

import { AuthService } from './auth.service';

@Component({
  templateUrl: './login-openid.component.html',
  styleUrls: ['./forms.css']
})
export class LoginOpenIDComponent {
  model: any = {};

  constructor(
    private authService: AuthService
  ) { }

  onLogin() {
    this.authService.loginOpenID(this.model.openidURL)
      .then(response => this.handleLogin(response))
      .catch(error => console.log(error));
  }

  handleLogin(response: any) {
    if (response.status === 'success') {
      window.location.replace(response.data.redirect);
    }
  }
}
