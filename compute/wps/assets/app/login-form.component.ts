import { Component } from '@angular/core';
import { Location } from '@angular/common';

import { User } from './user';
import { AuthService } from './auth.service';

@Component({
  templateUrl: './login-form.component.html',
  styleUrls: ['./forms.css']
})

export class LoginFormComponent {
  model = new User();

  constructor(
    private authService: AuthService,
    private location: Location
  ) { }

  onSubmit(): void {
    this.authService.login(this.model);
  }
}
