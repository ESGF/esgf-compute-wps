import { Inject, Injectable } from '@angular/core';
import { Http, Headers } from '@angular/http';
import { DOCUMENT } from '@angular/platform-browser';

import 'rxjs/add/operator/toPromise';

import { User } from './user';

@Injectable()
export class AuthService {
  constructor(@Inject(DOCUMENT) private doc: any, private http: Http) { }

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

  create(user: User): Promise<string> {
    let params: string = '';

    for (let k in user) {
      params += `${k.toLowerCase()}=${user[k]}&`;
    }

    return this.http.post('auth/create/', params, {
      headers: new Headers({
        'X-CSRFToken': this.getCookie('csrftoken'),
        'Content-Type': 'application/x-www-form-urlencoded'
      })
    })
      .toPromise()
      .then(response => response.json())
      .catch(this.handleError);
  }

  private handleError(error: any): Promise<any> {
    return Promise.reject(error.message || error);
  }
}
