import {Injectable, ReflectiveInjector} from '@angular/core';
import {async, fakeAsync, tick} from '@angular/core/testing';
import {BaseRequestOptions, ConnectionBackend, Http, RequestOptions} from '@angular/http';
import {Response, ResponseOptions} from '@angular/http';
import {MockBackend, MockConnection} from '@angular/http/testing';

import { WPSService, WPSResponse } from '../core/wps.service';

describe('WPS Service', () => {
  beforeEach(() => {
    this.injector = ReflectiveInjector.resolveAndCreate([
      {provide: ConnectionBackend, useClass: MockBackend},
      {provide: RequestOptions, useClass: BaseRequestOptions},
      Http,
      WPSService
    ]);

    this.service = this.injector.get(WPSService);
    this.backend = this.injector.get(ConnectionBackend) as MockBackend;
    this.backend.connections.subscribe((connection: any) => this.lastConnection = connection);
  });

  it('should execute a POST request with CSRF token', fakeAsync(() => {
    let response: WPSResponse;

    this.service.postCSRF('user/details').then((data: any) => response = data);

    this.lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({status: 'success', data: 'data'})
    })));

    document.cookie = 'csrftoken=test';

    tick();

    expect(response).toBeDefined();
    expect(response.status).toBe('success');
    expect(response.data).toBe('data');

    let headers = this.lastConnection.request.headers;

    expect(headers.keys()).toContain('Content-Type');
    expect(headers.get('Content-Type')).toContain('application/x-www-form-urlencoded');

    expect(headers.keys()).toContain('X-CSRFToken');
    expect(headers.get('X-CSRFToken')).toContain('test');
  }));

  it('should execute a POST request', fakeAsync(() => {
    let response: WPSResponse;

    this.service.post('user/details').then((data: any) => response = data);

    this.lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({status: 'success', data: 'data'})
    })));

    tick();

    expect(response).toBeDefined();
    expect(response.status).toBe('success');
    expect(response.data).toBe('data');

    let headers = this.lastConnection.request.headers;

    expect(headers.keys()).toContain('Content-Type');
    expect(headers.get('Content-Type')).toContain('application/x-www-form-urlencoded');
  }));

  it('should execute a GET request with CSRF token', fakeAsync(() => {
    let response: WPSResponse;

    this.service.getCSRF('user/details').then((data: any) => response = data);

    this.lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({status: 'success', data: 'data'})
    })));

    document.cookie = 'csrftoken=test';

    tick();

    expect(response).toBeDefined();
    expect(response.status).toBe('success');
    expect(response.data).toBe('data');

    let headers = this.lastConnection.request.headers;

    expect(headers.keys()).toContain('X-CSRFToken');
    expect(headers.get('X-CSRFToken')).toContain('test');
  }));

  it('should execute a GET request', fakeAsync(() => {
    let response: WPSResponse;

    this.service.get('user/details').then((data: any) => response = data);

    this.lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({status: 'success', data: 'data'})
    })));

    tick();

    expect(response).toBeDefined();
    expect(response.status).toBe('success');
    expect(response.data).toBe('data');
  }));
});
