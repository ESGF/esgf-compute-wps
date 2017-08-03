import { Component } from '@angular/core';
import { Router } from '@angular/router';

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
    private router: Router
  ) { }

  onSubmit(): void {
    this.authService.login(this.model)
      .then(response => this.router.navigate(['/wps/home/profile']));
  }
}
