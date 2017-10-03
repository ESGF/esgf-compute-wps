import { ReflectiveInjector } from '@angular/core';
import { async, fakeAsync, tick } from '@angular/core/testing';
import { BaseRequestOptions, ConnectionBackend, Http, RequestOptions } from '@angular/http';
import { Response, ResponseOptions } from '@angular/http';
import { MockBackend, MockConnection } from '@angular/http/testing';
import { DOCUMENT } from '@angular/platform-browser';

import { User, AuthService } from './auth.service';
import { WPSResponse } from './wps.service';

describe('AuthenticationService', () => {
  beforeEach(() => {
    this.injector = ReflectiveInjector.resolveAndCreate([
      {provide: ConnectionBackend, useClass: MockBackend},
      {provide: RequestOptions, useClass: BaseRequestOptions},
      {provide: DOCUMENT, useValue: {}},
      Http,
      AuthService
    ]);
    this.authService = this.injector.get(AuthService);
    this.backend = this.injector.get(ConnectionBackend) as MockBackend;
    this.backend.connections.subscribe((connection: any) => this.lastConnection = connection);
  });

  it('resetPassword() should return a success with redirect', fakeAsync(() => {
    let result: WPSResponse; 

    this.authService.resetPassword({
      username: 'testname',
      token: 'token',
      password: 'abcd'
    }).then((response: WPSResponse) => result = response);

    this.lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({
        status: 'success',
        data: { 
          redirect: 'https://test/login'
        }
      })
    })));
    tick();

    expect(result.status).toBe('success');
    expect(result.data.redirect).toBe('https://test/login');
  }));
});
