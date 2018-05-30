import { Injectable, ReflectiveInjector } from '@angular/core';
import { async, fakeAsync, tick } from '@angular/core/testing';
import { BaseRequestOptions, ConnectionBackend, Http, RequestOptions } from '@angular/http';
import { Response, ResponseOptions } from '@angular/http';
import { MockBackend, MockConnection } from '@angular/http/testing';

import { Axis } from './axis.component';
import { Configuration, ConfigureService, VariableCollection, Dataset } from './configure.service';

describe('Configuration Service', () => {
  let injector: ReflectiveInjector;
  let backend: MockBackend;
  let lastConnection: MockConnection;

  let service: ConfigureService; 

  let testConfig: Configuration;

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

    testConfig = new Configuration();
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

  it('should return a list of axes', fakeAsync(() => {
    let mockResult: Axis[] = [
      {
        id: 'time',
        id_alt: 't',
        start: 0,
        stop: 2000,
        step: 2,
        units: 'days since 1990-1-1',
        crs: 'Values'
      },
      {
        id: 'longitude',
        id_alt: 'x',
        start: 0,
        stop: 190,
        step: 2,
        units: 'degrees west',
        crs: 'Values'
      }
    ];
    let axisResult: Axis[];

    let config = new Configuration();

    //config.variable = 'tas';
    config.datasetID = 'mockDatasetID';
    config.indexNode = 'mockIndexNode';

    service.searchVariable(config)
      .then((result: Axis[]) => axisResult = result);

    lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({
        status: 'success',
        data: mockResult
      })
    })));

    tick();

    expect(axisResult).toEqual(mockResult);
  }));

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
    let searchResult: VariableCollection;

    let config: Configuration = new Configuration();

    config.datasetID = 'mockID';
    config.indexNode = 'mockIndexNode';

    //service.searchESGF(config)
    //  .then((result: VariableCollection) => searchResult = result);

    lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({
        status: 'success',
        data: mockResult,
      })
    })));

    tick();

    //expect(searchResult).toEqual(mockResult);
  }));

  it('should return a list of processes', fakeAsync(() => {
    let mockProcesses = ['test1', 'test2', 'test3'];
    let processes: string[];

    service.processes()
      .then((data: string[]) => processes = data);

    lastConnection.mockRespond(new Response(new ResponseOptions({
      body: JSON.stringify({
        status: 'success',
        data: mockProcesses
      })
    })));

    tick();

    expect(processes).toEqual(mockProcesses);
  }));
});
