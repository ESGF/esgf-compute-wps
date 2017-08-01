import { Component } from '@angular/core';
import { Location } from '@angular/common';

import { User } from './user';
import { AuthService } from './auth.service';

@Component({
  templateUrl: './create-user-form.component.html',
  styles: [
    `
      .ng-valid[required], .ng-valid.required {
        border-left: 5px solid #42a948;
      }

      .ng-invalid:not(form) {
        border-left: 5px solid #a94442;
      }
    `
  ],
  providers: [AuthService]
})

export class CreateUserFormComponent {
  model: User = new User();

  constructor(
    private authService: AuthService,
    private location: Location
  ) { }

  onSubmit(): void {
    this.authService.create(this.model)
      .then(response => this.handleResponse(response));
  }

  handleResponse(response: string): void {
    this.location.go('wps/home');
  }
}
