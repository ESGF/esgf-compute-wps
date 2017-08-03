import { Component } from '@angular/core';

import { AuthService } from './auth.service';

@Component({
  template: ''
})

export class LogoutComponent {
  constructor(private authService: AuthService) {
    this.authService.logout();
  }
}
