import { Injectable } from '@angular/core';
import {
  CanActivate, CanActivateChild,
  Router,
  ActivatedRouteSnapshot,
  RouterStateSnapshot
} from '@angular/router';

import { AuthService } from '../core/auth.service';

@Injectable()
export class AuthGuard implements CanActivate, CanActivateChild {
  constructor(
    private authService: AuthService,
    private router: Router
  ) { }

  checkLogin(url: string): boolean {
    if (this.authService.isLoggedIn) { return true; }

    this.authService.redirectUrl = url;

    this.router.navigate(['/wps/home/auth/login']);

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
