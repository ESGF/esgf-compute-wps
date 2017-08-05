import { Component, OnInit } from '@angular/core';

import { AuthService } from './auth.service';
import { ConfigureService } from './configure.service';

@Component({
  selector: 'my-app',
  templateUrl: './app.component.html',
  providers: [
    AuthService,
    ConfigureService,
  ]
})

export class AppComponent implements OnInit { 
  logged: boolean = false;
  error: boolean = false;
  message: string = '';

  constructor(
    private authService: AuthService,
    private configService: ConfigureService
  ) { }

  ngOnInit(): void {
    this.authService.logged$.subscribe((data: boolean) => this.logged = data);

    this.configService.error.subscribe((data: string) => {
      this.error = true;

      this.message = data;
    });
  }

  onHideError(): void {
    if (this.error) this.error = false;
  }
}
