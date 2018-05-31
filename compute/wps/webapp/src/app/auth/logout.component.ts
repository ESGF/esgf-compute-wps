import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { AuthService } from '../core/auth.service';
import { ConfigService } from '../core/config.service';

@Component({
  template: ''
})

export class LogoutComponent implements OnInit {
  constructor(
    private authService: AuthService,
    private configService: ConfigService,
    private router: Router,
  ) { }

  ngOnInit() {
    this.authService.logout()
    
    this.router.navigate([this.configService.basePath]);
  }
}
