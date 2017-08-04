import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { User } from './user';
import { AuthService } from './auth.service';

@Component({
  templateUrl: './update-user-form.component.html',
  styleUrls: ['./forms.css']
})

export class UpdateUserFormComponent { 
  model: User = new User();
  mpc: User = new User();

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

  onRegenerateKey(): void {
    this.authService.regenerateKey(this.model)
      .then(key => this.model.api_key = key);
  }

  onOAuth2(): void {
    this.authService.oauth2(this.model.openID)
      .then(response => this.handleOAuth2(response));
  }

  onMPCSubmit(): void {
    this.authService.myproxyclient(this.mpc)
      .then(response => this.handleMPC(response));
  }

  handleOAuth2(response: any): void {
    if (response.status && response.status === 'success') {
      this.router.navigateByUrl(response.redirect);
    }
  }

  handleMPC(response: any): void {
    if (response.status && response.status === 'success') {
      window.location.reload();
    }
  }
}
