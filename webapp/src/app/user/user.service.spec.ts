import {Injectable, ReflectiveInjector} from '@angular/core';
import {async, fakeAsync, tick} from '@angular/core/testing';
import {BaseRequestOptions, ConnectionBackend, Http, RequestOptions} from '@angular/http';
import {Response, ResponseOptions} from '@angular/http';
import {MockBackend, MockConnection} from '@angular/http/testing';

import { WPSResponse } from '../core/wps.service';

import { User, UserService } from './user.service';

describe('WPS Service', () => {
  let injector: ReflectiveInjector;
  let lastConnection: any;
  let backend: MockBackend;

  let service: UserService;

  beforeEach(() => {
    injector = ReflectiveInjector.resolveAndCreate([
      {provide: ConnectionBackend, useClass: MockBackend},
      {provide: RequestOptions, useClass: BaseRequestOptions},
      Http,
      UserService
    ]);

    service = injector.get(UserService);
    backend = injector.get(ConnectionBackend) as MockBackend;
    backend.connections.subscribe((connection: any) => lastConnection = connection);
  });

  it('should update a user', fakeAsync(() => {
    let user = new User();

    user.username = 'test';
    user.openID = 'http://test.com/openid/test';
    user.email = 'test@gmail.com';

    spyOn(service, 'postCSRF');

    service.update(user);

    expect(service.postCSRF).toHaveBeenCalledWith('auth/update', 'username=test&openid=http://test.com/openid/test&email=test@gmail.com&');
  }));

  it('should regenerate api key', fakeAsync(() => {
    spyOn(service, 'getCSRF');

    service.regenerateKey();

    expect(service.getCSRF).toHaveBeenCalledWith('auth/user/regenerate');
  }));
});
