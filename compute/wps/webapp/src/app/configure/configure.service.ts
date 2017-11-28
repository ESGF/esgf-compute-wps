import { Http, Headers, URLSearchParams, QueryEncoder } from '@angular/http';
import { Injectable } from '@angular/core';
import { Params } from '@angular/router';

import { Axis } from './axis.component';
import { Parameter } from './parameter.component';
import { RegridModel } from './regrid.component';
import { WPSService, WPSResponse } from '../core/wps.service';
import { AuthService } from '../core/auth.service';

export const LNG_NAMES: string[] = ['longitude', 'lon', 'x'];
export const LAT_NAMES: string[] = ['latitude', 'lat', 'y'];

export class Process {
  uid: string;

  constructor(
    public identifier: string = '',
    public inputs: (Variable | Process)[] = [],
    public domain: Axis[] = [],
    public regrid: RegridModel = new RegridModel(),
    public parameters: any[] = [],
  ) { 
    this.uid = Math.random().toString(16).slice(2); 
  }
}

export interface DatasetCollection { 
  [index: string]: Dataset;
}

export class Dataset {
  constructor(
    public id: string,
    public variables: Variable[] = []
  ) { }
}

export class Variable {
  constructor(
    public id: string,
    public axes: Axis[] = [],
    public files: string[] = [],
    public dataset: string = '',
  ) { }
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

    if (this.process.regrid.regridType !== 'None') {
      if (this.process.regrid.regridType === 'Uniform') {
        if (this.process.regrid.lons === undefined) {
          throw `Regrid option "${this.process.regrid.regridType}" requires Longitude to be set`;
        }
      }

      if (this.process.regrid.lats === undefined) {
        throw `Regrid option "${this.process.regrid.regridType}" require Latitude to be set`;
      }
    }

    this.process.parameters.forEach((param: Parameter) => {
      if (param.key === '' || param.value === '') {
        throw 'Parameters are invalid';
      }
    });
  }

  get uuid() {
    return Math.random().toString(16).slice(2);
  }
  
  prepareDataInputs(): string {
    let inputs = this.process.inputs.map((value: Variable | Process) => {
      if (value instanceof Variable) {
        return value.files.map((file: string) => {
          return { 
            id: `${value.id}|${this.uuid}`,
            uri: file,
          };
        });
      }

      return [];
    });

    inputs = [].concat(...inputs);

    let domain = {
      id: this.uuid,
    };

    this.process.domain.forEach((axis: Axis) => {
      domain[axis.id] = {
        start: axis.start,
        end: axis.stop,
        step: axis.step,
        crs: 'values'
      };
    });

    let process = {
      name: this.process.identifier,
      input: inputs.map((value: any) => { return value.id.split('|')[1]; }),
      result: this.uuid,
      domain: domain.id,
    };

    this.process.parameters.forEach((value: any) => {
      process[value.key] = value.value;
    });

    if (this.process.regrid.regridType !== 'None') {
      let grid = '';

      if (this.process.regrid.regridType === 'Gaussian') {
        grid = `gaussian~${this.process.regrid.lats}`;
      } else {
        grid = `uniform~${this.process.regrid.lats}x${this.process.regrid.lons}`;
      }

      process['gridder'] = {
        tool: 'esmf',
        method: 'linear',
        grid: grid,
      };
    }

    let processes = JSON.stringify([process]);
    let variables = JSON.stringify(inputs);
    let domains = JSON.stringify([domain]);

    return `[domain=${domains}|variable=${variables}|operation=${processes}]`;
  }

  prepareData(): string {
    let data = '';
    let numberPattern = /\d+\.?\d+/;

    this.validate();

    let input = <Variable>this.process.inputs[0];

    data += `process=${this.process.identifier}&`;

    data += `variable=${input.id}&`;

    data += `regrid=${this.process.regrid}&`;

    if (this.process.regrid.regridType !== 'None') {
      data += `longitudes=${this.process.regrid.lons}&`;

      data += `latitudes=${this.process.regrid.lats}&`;
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
    http: Http,
    private authService: AuthService,
  ) { 
    super(http); 
  }

  processes(): Promise<string[]> {
    return this.get('/wps/processes')
      .then(response => {
        return response.data;
      });
  }

  searchESGF(config: Configuration): Promise<Variable[]> { 
    let params = new URLSearchParams();

    params.append('dataset_id', config.dataset.id);
    params.append('index_node', config.indexNode);
    params.append('query', config.query);
    params.append('shard', config.shard);

    return this.get('/wps/search', params)
      .then(response => {
        return Object.keys(response.data).map((key: string) => {
          let variable = response.data[key];

          return new Variable(variable.id, variable.axes, variable.files);
        });
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

    config.process.inputs = [];

    config.process.inputs.push(config.variable);

    try {
      preparedData = config.prepareDataInputs();
    } catch (e) {
      return Promise.reject(e);
    }

    let params = new URLSearchParams();

    params.append('service', 'WPS');
    params.append('request', 'execute');
    params.append('api_key', this.authService.user.api_key);
    params.append('identifier', config.process.identifier);
    params.append('datainputs', preparedData);

    return this.getUnmodified('/wps', params)
      .then(response => {
        return response.text(); 
      });
  }

  downloadScript(config: Configuration): Promise<any> {
    let preparedData: string;

    config.process.inputs = [];

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
