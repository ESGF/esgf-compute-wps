import { Component } from '@angular/core';

import { AuthService } from './auth.service';

@Component({
  selector: 'my-app',
  templateUrl: './app.component.html',
  providers: [AuthService]
})

export class AppComponent { 
  logged: boolean = false;

  constructor(private authService: AuthService) { 
    this.authService.logged$.subscribe((data: boolean) => this.logged = data);
  }
}
