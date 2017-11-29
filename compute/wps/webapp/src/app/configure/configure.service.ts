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

  get uuid() {
    return Math.random().toString(16).slice(2);
  }

  newUID() {
    return Math.random().toString(16).slice(2);
  }

  setInputs(inputs: (Variable | Process)[]) {
    this.inputs = inputs;
  }

  validate() { 
    if (this.inputs.length === 0) {
      throw `Process "${this.identifier} - ${this.uid}" must have atleast one input`;
    }

    if (this.regrid.regridType !== 'None') {
      if (this.regrid.regridType === 'Uniform') {
        if (this.regrid.lons === 0) {
          throw `Regrid option "${this.regrid.regridType}" requires Longitude to be set`;
        }
      }

      if (this.regrid.lats === 0) {
        throw `Regrid option "${this.regrid.regridType}" require Latitude to be set`;
      }
    }

    this.parameters.forEach((param: Parameter) => {
      if (param.key === '') {
        throw 'Parameter has an empty key';
      }

      if (param.value === '') {
        throw `Parameter "${param.key}" has an empty value`;
      }
    });
  }

  prepareDataInputs() {
    let operation = {};
    let domain = {};
    let variable = {};

    let disc = {};
    let stack: Process[] = [this];

    while (stack.length > 0) {
      let curr = stack.pop();

      curr.validate();

      // Override with global regrid
      curr.regrid = this.regrid;

      // Merge global parameters
      Array.prototype.push.apply(curr.parameters, this.parameters);

      let vars = curr.inputs.filter((item: any) => { return !(item instanceof Process); });

      console.log(vars);

      if (disc[curr.uid] === undefined) {
        disc[curr.uid] = true;

        let processes = curr.inputs.filter((item: any) => { return item instanceof Process; });

        processes.forEach((proc: Process) => {
          stack.push(proc);
        });
      }
    }

    //let inputs = this.inputs.map((value: Variable | Process) => {
    //  if (value instanceof Variable) {
    //    return value.files.map((file: string) => {
    //      return { 
    //        id: `${value.id}|${this.uuid}`,
    //        uri: file,
    //      };
    //    });
    //  }

    //  return [];
    //});

    //inputs = [].concat(...inputs);

    //let domain = {
    //  id: this.uuid,
    //};

    //this.domain.forEach((axis: Axis) => {
    //  domain[axis.id] = {
    //    start: axis.start,
    //    end: axis.stop,
    //    step: axis.step,
    //    crs: 'values'
    //  };
    //});

    //let process = {
    //  name: this.identifier,
    //  input: inputs.map((value: any) => { return value.id.split('|')[1]; }),
    //  result: this.uuid,
    //  domain: domain.id,
    //};

    //this.parameters.forEach((value: any) => {
    //  process[value.key] = value.value;
    //});

    //if (this.regrid.regridType !== 'None') {
    //  let grid = '';

    //  if (this.regrid.regridType === 'Gaussian') {
    //    grid = `gaussian~${this.regrid.lats}`;
    //  } else {
    //    grid = `uniform~${this.regrid.lats}x${this.regrid.lons}`;
    //  }

    //  process['gridder'] = {
    //    tool: 'esmf',
    //    method: 'linear',
    //    grid: grid,
    //  };
    //}

    //let processes = JSON.stringify([process]);
    //let variables = JSON.stringify(inputs);
    //let domains = JSON.stringify([domain]);

    return `[domain=${domain}|variable=${variable}|operation=${operation}]`;
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

  
  prepareDataInputs(): string {
    return '';
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

  execute(process: Process): Promise<string> {
    let preparedData: string;

    try {
      preparedData = process.prepareDataInputs();
    } catch (e) {
      return Promise.reject(e);
    }

    let params = new URLSearchParams();

    params.append('service', 'WPS');
    params.append('request', 'execute');
    params.append('api_key', this.authService.user.api_key);
    params.append('identifier', process.identifier);
    params.append('datainputs', preparedData);

    return this.getUnmodified('/wps', params)
      .then(response => {
        return response.text(); 
      });
  }

  downloadScript(process: Process): Promise<any> {
    let preparedData: string;
    
    try {
      preparedData = process.prepareDataInputs();
    } catch (e) {
      return Promise.reject(e);
    }

    let data = `datainputs=${preparedData}`;

    return this.postCSRF('/wps/generate/', data)
      .then(response => {
        return response.data;
      });
  }
}
