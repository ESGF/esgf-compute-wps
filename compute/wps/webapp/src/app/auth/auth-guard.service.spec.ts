import { TestBed } from '@angular/core/testing';
import {
  Router,
  ActivatedRouteSnapshot,
  RouterStateSnapshot
} from '@angular/router';

import { AuthGuard } from '../auth/auth-guard.service';
import { AuthService } from '../core/auth.service';

describe('AuthGuard', () => {
  let router: any;
  let state: any;
  let guard: any;
  let auth: any;

  beforeEach(() => {
    router = jasmine.createSpyObj('router', ['navigate']);

    state = { url: '' };

    class AuthServiceMock { 
      isLoggedIn: true;
      redirectUrl: ''
    }

    TestBed.configureTestingModule({
      providers: [
        AuthGuard,
        {provide: AuthService, useClass: AuthServiceMock},
        {provide: Router, useValue: router},
      ]
    });

    guard = TestBed.get(AuthGuard);

    auth = TestBed.get(AuthService);
  });

  describe('canActivateChild', () => {

    it('should allow', () => {
      auth.isLoggedIn = true;

      expect(guard.canActivateChild(new ActivatedRouteSnapshot(), <RouterStateSnapshot>state)).toBe(true);
    });

    it('should block, store url and redirect', () => {
      let redirectUrl = 'https://doesnotexist.com/doesnotexist';

      state.url = redirectUrl;

      expect(guard.canActivateChild(new ActivatedRouteSnapshot(), <RouterStateSnapshot>state)).toBe(false);

      expect(router.navigate).toHaveBeenCalledWith(['/wps/home/auth/login']);

      expect(auth.redirectUrl).toBe(redirectUrl);
    });
  });

  describe('canActivate', () => {

    it('should allow', () => {
      auth.isLoggedIn = true;

      expect(guard.canActivate(new ActivatedRouteSnapshot(), <RouterStateSnapshot>state)).toBe(true);
    });

    it('should block, store url and redirect', () => {
      let redirectUrl = 'https://doesnotexist.com/doesnotexist';

      state.url = redirectUrl;

      expect(guard.canActivate(new ActivatedRouteSnapshot(), <RouterStateSnapshot>state)).toBe(false);

      expect(router.navigate).toHaveBeenCalledWith(['/wps/home/auth/login']);

      expect(auth.redirectUrl).toBe(redirectUrl);
    });
  });
});
