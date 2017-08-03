import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { User } from './user';
import { AuthService } from './auth.service';

@Component({
  templateUrl: './update-user-form.component.html'
})

export class UpdateUserFormComponent { 
  model: User = new User();

  constructor(
    private authService: AuthService,
    private router: Router
  ) {
    this.authService.user()
      .then(user => this.model = user);
  }

  onSubmit(form: any): void {
    let user = new User();

    for (let control in form.controls) {
      if (form.controls[control].dirty) {
        user[control] = form.controls[control].value;
      }
    }

    this.authService.update(user);
  }
}
