import { Http, Headers, URLSearchParams } from '@angular/http';
import { Injectable } from '@angular/core';
import { Params } from '@angular/router';

import { Axis } from './axis.component';
import { Parameter } from './parameter.component';
import { WPSService, WPSResponse } from '../core/wps.service';

export const LNG_NAMES: string[] = ['longitude', 'lon', 'x'];
export const LAT_NAMES: string[] = ['latitude', 'lat', 'y'];

export interface DatasetCollection { 
  [index: string]: Dataset;
}

export interface Dataset {
  id: string;
  variables: VariableCollection;
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
  process: string;
  dataset: Dataset;
  variable: Variable;
  variableID: string;
  regrid: string;
  regridOptions: RegridOptions;
  params: Parameter[];

  // ESGF search parameters
  datasetID: string;
  indexNode: string;
  query: string;
  shard: string;

  constructor() {
    this.regrid = 'None';

    this.regridOptions = { lats: 0, lons: 0 } as RegridOptions;

    this.dataset = { id: '', variables: {}} as Dataset;

    this.params = [];
  }

  validate() { 
    if (this.variable.axes === undefined) {
      throw 'Missing domain axes, wait until the domain has loaded';
    }

    if (this.regrid !== 'None') {
      if (this.regrid === 'Uniform') {
        if (this.regridOptions.lons === undefined) {
          throw `Regrid option "${this.regrid}" requires Longitude to be set`;
        }
      }

      if (this.regridOptions.lats === undefined) {
        throw `Regrid option "${this.regrid}" require Latitude to be set`;
      }
    }

    this.params.forEach((param: Parameter) => {
      if (param.key === '' || param.value === '') {
        throw 'Parameters are invalid';
      }
    });
  }

  prepareData(): string {
    let data = '';
    let numberPattern = /\d+\.?\d+/;

    this.validate();

    data += `process=${this.process}&`;

    data += `variable=${this.variableID}&`;

    data += `regrid=${this.regrid}&`;

    if (this.regrid !== 'None') {
      data += `longitudes=${this.regridOptions.lons}&`;

      data += `latitudes=${this.regridOptions.lats}&`;
    }

    data += `files=${this.variable.files}&`;

    let dimensions = JSON.stringify(this.variable.axes.map((axis: Axis) => { return axis; }));

    data += `dimensions=${dimensions}&`;

    if (this.params.length > 0) {
      let parameters = this.params.map((param: Parameter) => { return `${param.key}=${param.value}`; }).join(',');

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

    params.append('dataset_id', config.datasetID);
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

    params.append('dataset_id', config.datasetID);
    params.append('index_node', config.indexNode);
    params.append('query', config.query);
    params.append('shard', config.shard);
    params.append('variable', config.variableID);

    return this.get('/wps/search/variable', params)
      .then(response => {
        return response.data as Axis[];
      });
  }

  execute(config: Configuration): Promise<string> {
    let preparedData: string;
    
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
