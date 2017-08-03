import { Component } from '@angular/core';
import { Location } from '@angular/common';

import { AuthService } from './auth.service';

@Component({
  template: ''
})

export class LogoutComponent {
  constructor(
    private authService: AuthService,
    private location: Location
  ) {
    this.authService.logout()
      .then(response => this.location.go('/wps/home'));
  }
}
