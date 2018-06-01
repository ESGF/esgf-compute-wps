import { Injectable } from '@angular/core';
import { Http, Headers, URLSearchParams } from '@angular/http';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import { Subject } from 'rxjs/Subject';

import { WPSService, WPSResponse } from './wps.service';
import { User } from '../user/user.service';
import { ConfigService } from './config.service';

@Injectable()
export class AuthService extends WPSService {
  isLoggedIn: boolean = false;
  isLoggedIn$ = new BehaviorSubject<boolean>(this.isLoggedIn);

  user: User = null;
  user$ = new BehaviorSubject<User>(this.user);

  redirectUrl: string;

  constructor(
    http: Http,
    private configService: ConfigService,
  ) { 
    super(http);

    if (this.authenticated) {
      this.setLoggedIn(true);
    }

    this.userDetails()
      .then(response => {
        this.setLoggedIn(true);
      })
      .catch(error => { });
  }

  get authenticated() {
    let expires = localStorage.getItem('expires');

    if (expires !== null) {
      let expiresDate = Date.parse(expires);

      return Date.now() <= expiresDate;
    }

    return false; 
  }

  setExpires(expires: any) {
    localStorage.setItem('expires', expires.toString());
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
    return this.getCSRF(this.configService.authUserPath)
      .then(response => {
        this.setUser(response.data as User);
      })
      .catch(error => {
        //this.setUser(null);

        return Promise.reject(error);
      });
  }

  resetPassword(data: any): Promise<WPSResponse> {
    let params = new URLSearchParams();

    params.set('username', data.username);
    params.set('token', data.token);
    params.set('password', data.password);

    return this.getCSRF(this.configService.authResetPath, params);
  }

  forgotPassword(username: string): Promise<WPSResponse> {
    let params = new URLSearchParams();

    params.set('username', username);

    return this.getCSRF(this.configService.authForgotPasswordPath, params);
  }

  forgotUsername(email: string): Promise<WPSResponse> {
    let params = new URLSearchParams();

    params.set('email', email);

    return this.getCSRF(this.configService.authForgotUsernamePath, params);
  }

  create(user: User): Promise<WPSResponse> {
    return this.postCSRF(this.configService.authCreatePath, user.toUrlEncoded());
  }

  login(username: string, password: string): Promise<WPSResponse> {
    let data = `username=${username}&password=${password}`;

    return this.postCSRF(this.configService.authLoginPath, data)
      .then(response => {
        this.setUser(response.data as User);

        this.setExpires(this.user.expires);

        this.setLoggedIn(true);

        return response;
      })
      .catch(error => {
        this.setLoggedIn(false);

        localStorage.removeItem('expires');

        throw error;
      });
  }

  loginOpenID(openidURL: string): Promise<WPSResponse> {
    return this.postCSRF(this.configService.authLoginOpenIDPath, `openid_url=${openidURL}`);
  }

  logout() {
    this.getCSRF(this.configService.authLogoutPath)
      .then(response => {
        this.setLoggedIn(false);

        localStorage.removeItem('expires');
      })
      .catch(error => {
        this.setLoggedIn(false);

        localStorage.removeItem('expires');
      });
  }

  oauth2(openid: string): Promise<WPSResponse> {
    return this.postCSRF(this.configService.authLoginOAuth2Path, `openid=${openid}`);
  }

  myproxyclient(username: string, password: string): Promise<WPSResponse> {
    return this.postCSRF(this.configService.authLoginMPCPath,
      `username=${username}&password=${password}`);
  }
}
