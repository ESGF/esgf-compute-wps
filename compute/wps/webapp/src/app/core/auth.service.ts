import { Injectable } from '@angular/core';
import { Http, Headers, URLSearchParams } from '@angular/http';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import { Subject } from 'rxjs/Subject';

import { WPSService, WPSResponse } from './wps.service';
import { User } from '../user/user.service';

@Injectable()
export class AuthService extends WPSService {
  isLoggedIn: boolean;
  isLoggedIn$ = new BehaviorSubject<boolean>(this.isLoggedIn);

  user: User;
  user$ = new BehaviorSubject<User>(this.user);

  redirectUrl: string;

  constructor(
    http: Http,
  ) { 
    super(http);

    if (this.authenticated) {
      this.setLoggedIn(true);

      this.userDetails();
    }
  }

  get authenticated() {
    let expires = localStorage.getItem('expires');

    if (expires !== null) {
      let expiresDate = Date.parse(expires);

      return Date.now() <= expiresDate;
    }

    return false; 
  }

  setLoggedIn(value: boolean) {
    this.isLoggedIn$.next(value);

    this.isLoggedIn = value;
  }

  setUser(value: User) {
    this.user$.next(value);

    this.user = value;
  }

  userDetails() {
    this.getCSRF('auth/user/')
      .then(response => {
        if (response.status === 'success') {
          this.setUser(response.data as User);
        }
      });
  }

  resetPassword(data: any): Promise<WPSResponse> {
    let params = new URLSearchParams();

    params.set('username', data.username);
    params.set('token', data.token);
    params.set('password', data.password);

    return this.getCSRF('auth/reset', params);
  }

  forgotPassword(username: string): Promise<WPSResponse> {
    let params = new URLSearchParams();

    params.set('username', username);

    return this.getCSRF('auth/forgot/password', params);
  }

  forgotUsername(email: string): Promise<WPSResponse> {
    let params = new URLSearchParams();

    params.set('email', email);

    return this.getCSRF('auth/forgot/username', params);
  }

  create(user: User): Promise<WPSResponse> {
    return this.postCSRF('auth/create/', user.toUrlEncoded());
  }

  login(user: User) {
    this.postCSRF('auth/login/', user.toUrlEncoded())
      .then(response => {
        if (response.status === 'success') {
          this.user = response.data as User;

          localStorage.setItem('expires', this.user.expires.toString());

          this.setLoggedIn(true);

        } else {
          this.setLoggedIn(false);
        }
      });
  }

  loginOpenID(openidURL: string): Promise<WPSResponse> {
    return this.postCSRF('auth/login/openid/', `openid_url=${openidURL}`);
  }

  logout() {
    this.getCSRF('auth/logout/')
      .then(response => {
        this.setLoggedIn(false);

        localStorage.removeItem('expires');
      });
  }

  oauth2(openid: string): Promise<WPSResponse> {
    return this.postCSRF('auth/login/oauth2/', `openid=${openid}`);
  }

  myproxyclient(user: User): Promise<WPSResponse> {
    return this.postCSRF('auth/login/mpc/',
      `username=${user.username}&password=${user.password}`);
  }
}
