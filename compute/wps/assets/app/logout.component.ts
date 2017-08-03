import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { AuthService } from './auth.service';

@Component({
  template: ''
})

export class LogoutComponent {
  constructor(
    private authService: AuthService,
    private router: Router,
  ) {
    this.authService.logout()
      .then(response => this.router.navigate(['/wps/home']));
  }
}
