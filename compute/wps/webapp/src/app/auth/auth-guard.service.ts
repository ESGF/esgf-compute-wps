import { Injectable } from '@angular/core';
import {
  CanActivate, CanActivateChild,
  Router,
  ActivatedRouteSnapshot,
  RouterStateSnapshot
} from '@angular/router';

import { AuthService } from '../core/auth.service';
import { ConfigService } from '../core/config.service';

@Injectable()
export class AuthGuard implements CanActivate, CanActivateChild {
  constructor(
    private authService: AuthService,
    private configService: ConfigService,
    private router: Router
  ) { }

  checkLogin(url: string): boolean {
    if (this.authService.isLoggedIn) { return true; }

    this.authService.redirectUrl = url;

    this.router.navigate([this.configService.loginPath]);

    return false;
  }

  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot) : boolean {
    let url = state.url;

    return this.checkLogin(url);
  }

  canActivateChild(route: ActivatedRouteSnapshot, state: RouterStateSnapshot) : boolean {
    return this.canActivate(route, state);
  }
}
