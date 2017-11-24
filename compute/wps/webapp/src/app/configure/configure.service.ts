import { Http, Headers, URLSearchParams } from '@angular/http';
import { Injectable } from '@angular/core';
import { Params } from '@angular/router';

import { Axis } from './axis.component';
import { Parameter } from './parameter.component';
import { WPSService, WPSResponse } from '../core/wps.service';

export const LNG_NAMES: string[] = ['longitude', 'lon', 'x'];
export const LAT_NAMES: string[] = ['latitude', 'lat', 'y'];

export class Process {
  constructor(
    public identifier: string = '',
    public inputs: Array<Variable|Process> = [],
    public domain: Axis[] = [],
    public regrid: string = 'None',
    public regridOptions: RegridOptions = {lats: 0, lons: 0},
    public parameters: any[] = [],
  ) { }
}

export interface DatasetCollection { 
  [index: string]: Dataset;
}

export interface Dataset {
  id: string;
  variables: Variable[];
}

export interface Variable {
  id: string;
  axes: Axis[];
  files: string[];
}

export interface VariableCollection {
  [index: string]: Variable;
}

export interface RegridOptions {
  lats: number;
  lons: number;
}

export class Configuration {
  process: Process;

  dataset: Dataset;
  variable: Variable;

  // ESGF search parameters
  datasetID: string;
  indexNode: string;
  query: string;
  shard: string;

  constructor() {
    this.process = new Process();
  }

  validate() { 
    if (this.variable.axes === undefined) {
      throw 'Missing domain axes, wait until the domain has loaded';
    }

    if (this.process.regrid !== 'None') {
      if (this.process.regrid === 'Uniform') {
        if (this.process.regridOptions.lons === undefined) {
          throw `Regrid option "${this.process.regrid}" requires Longitude to be set`;
        }
      }

      if (this.process.regridOptions.lats === undefined) {
        throw `Regrid option "${this.process.regrid}" require Latitude to be set`;
      }
    }

    this.process.parameters.forEach((param: Parameter) => {
      if (param.key === '' || param.value === '') {
        throw 'Parameters are invalid';
      }
    });
  }

  prepareData(): string {
    let data = '';
    let numberPattern = /\d+\.?\d+/;

    this.validate();

    let input = <Variable>this.process.inputs[0];

    data += `process=${this.process.identifier}&`;

    data += `variable=${input.id}&`;

    data += `regrid=${this.process.regrid}&`;

    if (this.process.regrid !== 'None') {
      data += `longitudes=${this.process.regridOptions.lons}&`;

      data += `latitudes=${this.process.regridOptions.lats}&`;
    }

    data += `files=${input.files}&`;

    let dimensions = JSON.stringify(input.axes.map((axis: Axis) => { return axis; }));

    data += `dimensions=${dimensions}&`;

    if (this.process.parameters.length > 0) {
      let parameters = this.process.parameters.map((param: Parameter) => { 
        return `${param.key}=${param.value}`; 
      }).join(',');

      data += `parameters=${parameters}`;
    }

    return data;
  }
}

@Injectable()
export class ConfigureService extends WPSService {
  constructor(
    http: Http
  ) { 
    super(http); 
  }

  processes(): Promise<string[]> {
    return this.get('/wps/processes')
      .then(response => {
        return response.data;
      });
  }

  searchESGF(config: Configuration): Promise<VariableCollection> { 
    let params = new URLSearchParams();

    params.append('dataset_id', config.dataset.id);
    params.append('index_node', config.indexNode);
    params.append('query', config.query);
    params.append('shard', config.shard);

    return this.get('/wps/search', params)
      .then(response => {
        return response.data as VariableCollection;
      });
  }

  searchVariable(config: Configuration): Promise<Axis[]> {
    let params = new URLSearchParams();

    params.append('dataset_id', config.dataset.id);
    params.append('index_node', config.indexNode);
    params.append('query', config.query);
    params.append('shard', config.shard);
    params.append('variable', config.variable.id);

    return this.get('/wps/search/variable', params)
      .then(response => {
        return response.data as Axis[];
      });
  }

  execute(config: Configuration): Promise<string> {
    let preparedData: string;

    config.process.inputs.push(config.variable);
    
    try {
      preparedData = config.prepareData();
    } catch (e) {
      return Promise.reject(e);
    }

    return this.postCSRF('/wps/execute/', preparedData).
      then(response => {
        return response.data;
      });
  }

  downloadScript(config: Configuration): Promise<any> {
    let preparedData: string;

    config.process.inputs.push(config.variable);
    
    try {
      preparedData = config.prepareData();
    } catch (e) {
      return Promise.reject(e);
    }

    return this.postCSRF('/wps/generate/', preparedData)
      .then(response => {
        return response.data;
      });
  }
}
