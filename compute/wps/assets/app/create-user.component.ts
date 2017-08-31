import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { User } from './user';
import { AuthService } from './auth.service';

@Component({
  templateUrl: './create-user.component.html',
  styleUrls: ['./forms.css']
})

export class CreateUserComponent {
  model: User = new User();

  constructor(
    private authService: AuthService,
    private router: Router
  ) { }

  onSubmit(): void {
    this.authService.create(this.model)
      .then(response => this.handleResponse(response));
  }

  handleResponse(response: any): void {
    if (response.status && response.status === 'success') {
      this.router.navigate(['/wps/home/login']);
    }
  }
}
