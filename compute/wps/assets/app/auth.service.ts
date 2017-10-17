import { Inject, Injectable } from '@angular/core';
import { Http, Headers, URLSearchParams } from '@angular/http';
import { DOCUMENT } from '@angular/platform-browser';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';
import { Subject } from 'rxjs/Subject';

import { WPSResponse } from './wps.service';

export interface User {
  username: string;
  openid: string;
  email: string;
  api_key: string;
  type: string;
  admin: boolean;
  local_init: boolean;
  expires?: number;
  password?: string;
}

interface Options {
  params: URLSearchParams,
  headers: Headers
}

@Injectable()
export class AuthService {
  user: User;

  logged$ = new BehaviorSubject<User>({} as User);

  logged = this.logged$.asObservable();

  constructor(
    @Inject(DOCUMENT) private doc: any,
    private http: Http
  ) { 
    this.getUserDetails();
  }

  getUserDetails() {
    this.userDetails()
      .then(data => {
        let response = data as WPSResponse;

        if (response.status === 'success') {
          this.user = response.data as User;

          localStorage.setItem('expires', this.user.expires.toString());

          this.logged$.next(this.user);
        } else {
          localStorage.removeItem('expires');

          this.logged$.next(null);
        }
      });
  }

  isLogged(): boolean {
    if (this.user || this.sessionValid()) {
      return true;
    }

    return false;
  }

  sessionValid(): boolean {
    if (this.user) {
      return Date.now() < this.user.expires;
    } else {
      let expires = localStorage.getItem('expires');

      if (Date.now() < Date.parse(expires)) {
        return true;
      }
    }
  
    return false;
  }

  getCookie(name: string): string {
    let cookieValue: string = null;

    if (this.doc.cookie && this.doc.cookie !== '') {
      let cookies: string[] = this.doc.cookie.split(';');

      for (let cookie of cookies) {
        if (cookie.trim().substring(0, name.length + 1) === (name + '=')) {
          cookieValue = decodeURIComponent(cookie.trim().substring(name.length + 1));

          break;
        }
      }
    }

    return cookieValue;
  }

  userToUrlEncoded(user: User): string {
    let params: string = '';

    for (let k in user) {
      params += `${k.toLowerCase()}=${user[k]}&`;
    }

    return params;
  }

  methodGet(url: string, params: URLSearchParams = null, headers: any = {}) {
    headers['X-CSRFToken'] = this.getCookie('csrftoken');

    let options: Options = {
      params: params,
      headers: new Headers(headers)
    };

    options.headers = headers;

    if (params !== null) {
      options.params = params;
    }

    return this.http.get(url, options)
      .toPromise()
      .then(response => response.json() as WPSResponse)
      .catch(this.handleError);
  }

  methodPost(url: string, data: string = '', headers: any = {}) {
    headers['X-CSRFToken'] = this.getCookie('csrftoken');

    headers['Content-Type'] = 'application/x-www-form-urlencoded'; 

    let options: Options = {
      params: null,
      headers: new Headers(headers)
    };

    return this.http.post(url, data, options)
      .toPromise()
      .then(response => response.json() as WPSResponse)
      .catch(this.handleError);
  }

  resetPassword(data: any): Promise<WPSResponse> {
    let params = new URLSearchParams();

    params.set('username', data.username);
    params.set('token', data.token);
    params.set('password', data.password);

    return this.methodGet('auth/reset', params);
  }

  forgotPassword(username: string): Promise<WPSResponse> {
    let params = new URLSearchParams();

    params.set('username', username);

    return this.methodGet('auth/forgot/password', params);
  }

  forgotUsername(email: string): Promise<WPSResponse> {
    let params = new URLSearchParams();

    params.set('email', email);

    return this.methodGet('auth/forgot/username', params);
  }

  create(user: User): Promise<WPSResponse> {
    return this.methodPost('auth/create/', this.userToUrlEncoded(user));
  }

  update(user: User): Promise<WPSResponse> {
    return this.methodPost('auth/update/', this.userToUrlEncoded(user));
  }

  login(user: User): Promise<WPSResponse> {
    return this.methodPost('auth/login/', this.userToUrlEncoded(user))
      .then(response => this.handleLoginResponse(response));
  }

  loginOpenID(openidURL: string): Promise<WPSResponse> {
    return this.methodPost('auth/login/openid/', `openid_url=${openidURL}`);
  }

  logout(): Promise<WPSResponse> {
    return this.methodGet('auth/logout/')
      .then(response => this.handleLogoutResponse(response));
  }

  userDetails(): Promise<any> {
    return this.methodGet('auth/user/');
  }

  regenerateKey(user: User): Promise<WPSResponse> {
    return this.methodGet('auth/user/regenerate/');
  }

  oauth2(openid: string): Promise<WPSResponse> {
    return this.methodPost('auth/login/oauth2/', `openid=${openid}`);
  }

  myproxyclient(user: User): Promise<WPSResponse> {
    return this.methodPost('auth/login/mpc/',
      `username=${user.username}&password=${user.password}`);
  }

  private handleLoginResponse(response: WPSResponse): WPSResponse {
    if (response.status === 'success') {
      this.user = response.data as User;

      localStorage.setItem('expires', this.user.expires.toString());

      this.logged$.next(this.user);
    } else {
      this.handleLogoutResponse(response);
    }

    return response
  }

  private handleLogoutResponse(response: WPSResponse): WPSResponse {
    localStorage.removeItem('expires');

    this.logged$.next(null);

    return response;
  }

  private handleError(error: any): Promise<any> {
    return Promise.reject(error.message || error);
  }
}
