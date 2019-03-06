import { Injectable, ReflectiveInjector } from '@angular/core';
import { async, fakeAsync, tick } from '@angular/core/testing';
import { BaseRequestOptions, ConnectionBackend, Http, RequestOptions } from '@angular/http';
import { Response, ResponseOptions } from '@angular/http';
import { MockBackend, MockConnection } from '@angular/http/testing';

import { ConfigureService } from './configure.service';

describe('Configuration Service', () => {
  let injector: ReflectiveInjector;
  let backend: MockBackend;
  let lastConnection: MockConnection;

  let service: ConfigureService; 

  beforeEach(() => {
    injector = ReflectiveInjector.resolveAndCreate([
      {provide: ConnectionBackend, useClass: MockBackend},
      {provide: RequestOptions, useClass: BaseRequestOptions},
      Http,
      ConfigureService,
    ]);

    service = injector.get(ConfigureService);
    backend = injector.get(ConnectionBackend) as MockBackend;
    backend.connections.subscribe((connection: any) => lastConnection = connection);

    //testConfig.process = 'testProcess';
    //testConfig.variable = 'tas';    
    //testConfig.dataset = {
    //  files: ['file1', 'file2'],
    //  axes: [
    //    {
    //      id: 'time',
    //      id_alt: 't',
    //      start: 0,
    //      stop: 2000,
    //      step: 2,
    //      units: 'days since 1990-1-1'
    //    },
    //  ]
    //} as Dataset;
  });

  describe('Download Script', () => {
    it('should return a string', fakeAsync(() => {
      let resultText: string;
      let mockResponse = 'test response';

      //service.downloadScript(testConfig)
      //  .then((result: string) => resultText = result);

      lastConnection.mockRespond(new Response(new ResponseOptions({
        body: JSON.stringify({
          status: 'success',
          data: mockResponse
        })
      })));

      tick();

      expect(resultText).toEqual(mockResponse);
    }));
  });

  describe('Execute', () => {
    it('should error', fakeAsync(() => {
      //testConfig.regrid = 'Uniform';

      let errorText: string;

      //service.execute(testConfig)
      //  .catch((error: string) => errorText = error);

      tick();

      expect(errorText).toEqual('Regrid option "Uniform" requires Longitude to be set');
    }));

    it('should return a string response', fakeAsync(() => {
      let resultText: string;
      let mockResponse = 'test response';

      //service.execute(testConfig)
      //  .then((result: string) => resultText = result);

      lastConnection.mockRespond(new Response(new ResponseOptions({
        body: JSON.stringify({
          status: 'success',
          data: mockResponse
        })
      })));

      tick();

      expect(resultText).toEqual(mockResponse);
    }));
  });

  it('should return ESGF search results', fakeAsync(() => {
    let mockResult = {
      tas: {
        files: ['file1', 'file2'],
        axes: [
          {
            id: 'time',
            id_alt: 't',
            start: 0,
            stop: 190,
            step: 2,
            units: 'days since 1990-1-1'
          }
        ]
      }
    };
  }));
});
