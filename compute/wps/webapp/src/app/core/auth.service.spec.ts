import {Injectable, ReflectiveInjector} from '@angular/core';
import {async, fakeAsync, tick} from '@angular/core/testing';
import {BaseRequestOptions, ConnectionBackend, Http, RequestOptions} from '@angular/http';
import {Response, ResponseOptions} from '@angular/http';
import {MockBackend, MockConnection} from '@angular/http/testing';

import { AuthService } from './auth.service';
import { User } from '../user/user.service';

describe('Authentication Service', () => {

  beforeEach(() => {
    this.userMock = new User();
    this.userMock.username = 'test';

    this.injector = ReflectiveInjector.resolveAndCreate([
      {provide: ConnectionBackend, useClass: MockBackend},
      {provide: RequestOptions, useClass: BaseRequestOptions},
      Http,
      AuthService
    ]);

    this.service = this.injector.get(AuthService);
    this.backend = this.injector.get(ConnectionBackend) as MockBackend;
    this.backend.connections.subscribe((connection: any) => this.lastConnection = connection);
  });

  it('should pass username and password for MyProxyClient', () => {
    this.service.myproxyclient('test', 'password');

    let data = this.lastConnection.request._body;

    expect(data).toContain('username=test');
    expect(data).toContain('password=password');
  });

  it('should pass openid', () => {
    let openid = 'http://test.com/openid/test';
    
    this.service.oauth2(openid);

    let data = this.lastConnection.request._body;

    expect(data).toContain(`openid=${openid}`);
  });

  it('should set isLoggedIn false and remove localStorage expires', fakeAsync(() => {
    let logged: boolean;

    localStorage.setItem('expires', Date.now().toString());

    this.service.isLoggedIn$.subscribe((value: boolean) => logged = value);

    this.service.logout();

    this.lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({
        status: 'success'
      })
    })));

    tick();

    expect(logged).toBe(false);
    expect(localStorage.getItem('expires')).toEqual(null);
  }));

  it('should pass openid_url', () => {
    let openid = 'https://test.com/openid/test';

    this.service.loginOpenID(openid);

    let body = this.lastConnection.request._body;

    expect(body).toContain(`openid_url=${openid}`);
  });

  it('should set isLoggedIn false and remove localStorage expires', fakeAsync(() => {
    let errorText: string;
    let logged: boolean;

    this.service.isLoggedIn$.subscribe((value: boolean) => logged = value);

    this.service.login('test', 'password')
      .catch((error: any) => errorText = error);

    this.lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({
        status: 'failed',
        error: 'failed to login'
      })
    })));

    tick();

    expect(logged).toBe(false);
    expect(localStorage.getItem('expires')).toEqual(null);
  }));

  it('should set isLoggedIn and set localStorage expires', fakeAsync(() => {
    let logged: boolean;
    let expires = Date.now();

    this.service.isLoggedIn$.subscribe((value: boolean) => logged = value);

    this.service.login('test', 'password');

    this.lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({
        status: 'success',
        data: {
          username: 'test',
          expires: expires
        }
      })
    })));

    tick();

    expect(logged).toBe(true);
    expect(localStorage.getItem('expires')).toEqual(expires.toString());
  }));

  it('should pass user details', fakeAsync(() => {
    this.service.create(this.userMock);

    tick();

    let data = this.lastConnection.request._body;

    expect(data).toContain('username=test');
  }));

  it('should pass email', fakeAsync(() => {
    this.service.forgotUsername('test_user@gmail.com');

    tick();

    let url = this.lastConnection.request.url;

    expect(url).toContain('email=test_user@gmail.com');
  }));

  it('should pass username', fakeAsync(() => {
    this.service.forgotPassword('test_user');

    tick();

    let url = this.lastConnection.request.url;

    expect(url).toContain('username=test_user');
  }));

  it('should pass reset password params', fakeAsync(() => {
    this.service.resetPassword({
      username: 'test',
      token: 'token',
      password: 'password',
    });

    tick();

    let url = this.lastConnection.request.url;

    expect(url).toContain('username=test');
    expect(url).toContain('token=token');
    expect(url).toContain('password=password');
  }));

  it('should not emit user from userDetails', fakeAsync(() => {
    let result: User;
    let errorText: string;

    this.service.user$.subscribe((value: User) => result = value);

    this.service.userDetails()
      .catch((error: any) => errorText = error);

    this.lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({
        status: 'failed',
        error: 'some error'
      })
    })));

    tick();

    expect(result).toBeNull();
    expect(errorText).toBe('some error');
  }));

  it('should emit user from userDetails', fakeAsync(() => {
    let result: User;

    this.service.user$.subscribe((value: User) => result = value);

    this.service.userDetails();

    this.lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({
        status: 'success',
        data: {
          username: 'test'
        }
      })
    })));

    tick();

    expect(result).toBeDefined();
    expect(result.username).toBe('test');
  }));

  it('should emit user', () => {
    let result: User;

    this.service.user$.subscribe((value: User) => result = value);

    this.service.setUser(this.userMock);

    expect(result).toBe(this.userMock);
  });

  it('should emit isLoggedIn', () => {
    let result: boolean;

    this.service.isLoggedIn$.subscribe((value: boolean) => result = value);

    this.service.setLoggedIn(true);

    expect(result).toBe(true);
  });
  
  it('should be authenticated', () => {
    let now = new Date();

    now.setDate(now.getDate() + 5);

    spyOn(localStorage, 'getItem').and.returnValue(now.toString());

    expect(this.service.authenticated).toBe(true);
  });

  it('should not be authenticated', () => {
    spyOn(localStorage, 'getItem').and.returnValue(null);

    expect(this.service.authenticated).toBe(false);
  });
});
