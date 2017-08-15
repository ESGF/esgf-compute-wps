import { Inject, Injectable } from '@angular/core';
import { Http, Headers } from '@angular/http';
import { DOCUMENT } from '@angular/platform-browser';
import { BehaviorSubject } from 'rxjs/BehaviorSubject';

import { User } from './user';
import { NotificationService } from './notification.service';

@Injectable()
export class AuthService {
  logged = this.isLogged();

  logged$ = new BehaviorSubject(this.logged);

  constructor(
    private notificationService: NotificationService,
    @Inject(DOCUMENT) private doc: any,
    private http: Http
  ) { }

  isLogged(): boolean {
    let expires = localStorage.getItem('wps_expires');

    if (expires != null) {
      let expiresDate = new Date(expires);

      if (expiresDate.getTime() > Date.now()) {
          return true
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

  create(user: User): Promise<string> {
    return this.http.post('auth/create/', this.userToUrlEncoded(user), {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => response.json())
      .catch(response => {
        this.notificationService.error(`Failed to create account`);

        return this.handleError(response);
      });
  }

  update(user: User): Promise<string> {
    return this.http.post('auth/update/', this.userToUrlEncoded(user), {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => response.json())
      .catch(response => {
        this.notificationService.error(`Failed to update account`);

        return this.handleError(response);
      });
  }

  login(user: User): Promise<string> {
    return this.http.post('auth/login/', this.userToUrlEncoded(user), {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => this.handleLoginResponse(response.json()))
      .catch(response => {
        this.notificationService.error(`Failed to login`);

        return this.handleError(response);
      });
  }

  logout(): Promise<string> {
    return this.http.get('auth/logout/', {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => this.handleLogoutResponse(response.json()))
      .catch(response => {
        this.notificationService.error(`Failed to logout`);

        return this.handleError(response);
      });
  }

  user(): Promise<User> {
    return this.http.get('auth/user/', {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => response.json() as User)
      .catch(response => {
        this.notificationService.error(`Failed to retrieve account details`);

        return this.handleError(response);
      });
  }

  regenerateKey(user: User): Promise<string> {
    return this.http.get(`auth/user/${user.id}/regenerate/`, {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => response.json().api_key)
      .catch(response => {
        this.notificationService.error(`Failed to regenerate key`);

        return this.handleError(response);
      });
  }

  oauth2(openid: string): Promise<string> {
    return this.http.post('auth/login/oauth2/', `openid=${openid}`, {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => response.json())
      .catch(response => {
        this.notificationService.error(`Failed to authenticate through OAuth2`);

        return this.handleError(response);
      });
  }

  myproxyclient(user: User): Promise<string> {
    return this.http.post('auth/login/mpc/', `username=${user.username}&password=${user.password}`, {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => response.json())
      .catch(response => {
        this.notificationService.error(`Failed to authenticate through MyProxyClient`);

        return this.handleError(response);
      });
  }

  private handleLoginResponse(response: any): any {
    if (response.status && response.status === 'success') {
      localStorage.setItem('wps_expires', response.expires);

      this.logged$.next(true);
    } else {
      this.handleLogoutResponse(response);
    }
  }

  private handleLogoutResponse(response: any): any {
    localStorage.removeItem('wps_expires');

    this.logged$.next(false);
  }

  private handleError(error: any): Promise<any> {
    return Promise.reject(error.message || error);
  }
}
