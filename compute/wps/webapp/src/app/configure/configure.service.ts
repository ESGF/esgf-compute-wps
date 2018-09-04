import { Http, Headers, URLSearchParams, QueryEncoder } from '@angular/http';
import { Injectable } from '@angular/core';
import { Params } from '@angular/router';

import { Axis, AxisCollection } from './axis.component';
import { Parameter } from './parameter.component';
import { RegridModel } from './regrid.component';
import { WPSService, WPSResponse } from '../core/wps.service';
import { ConfigService } from '../core/config.service';
import { Process } from './process';
import { AuthService } from '../core/auth.service';

export const LNG_NAMES: string[] = ['longitude', 'lon', 'x'];
export const LAT_NAMES: string[] = ['latitude', 'lat', 'y'];

export interface DatasetCollection { 
  [index: string]: Dataset;
}

export class Dataset {
  constructor(
    public id: string,
    public variables: Variable[] = []
  ) { }
}

export class File {
  constructor(
    public url: string,
    public index: number,
    public included: boolean = true,
    public temporal: any = null,
    public spatial: any = null,
    public uid: string = '',
  ) { 
    this.uid = Math.random().toString(16).slice(2);  
  }
}

export class Variable {
  constructor(
    public id: string,
    public files: number[],
    public axes: Axis[] = [],
    public dataset: string = '',
    public include: boolean = true,
    public uid: string = '',
  ) { 
    this.uid = Math.random().toString(16).slice(2); 
  }
}

export interface VariableCollection {
  variables: Variable[];
  files: number[];
}

export interface RegridOptions {
  lats: number;
  lons: number;
}

@Injectable()
export class ConfigureService extends WPSService {
  constructor(
    http: Http,
    private configService: ConfigService,
  ) { 
    super(http); 
  }

  searchESGF(dataset_id: string, params: any): Promise<VariableCollection> { 
    let newParams = {dataset_id: dataset_id, ...params};

    return this.get(this.configService.searchPath, newParams)
      .then(response => {
        let variables = Object.keys(response.data.variables).map((name: string) => {
          let files = response.data.variables[name];

          return new Variable(name, files);
        });

        return { variables: variables, files: response.data.files } as VariableCollection;
      });
  }

  searchVariable(variable: string, dataset_id: string, files: number[], params: any): Promise<any[]> {
    let newParams = {
      dataset_id: dataset_id,
      variable: variable,
      files: JSON.stringify(files),
      ...params
    };

    return this.get(this.configService.searchVariablePath, newParams)
      .then(response => {
        return response.data;
      });
  }

  downloadScript(process: Process): Promise<any> {
    let preparedData: string;
    
    try {
      preparedData = this.prepareDataInputsString(process);
    } catch (e) {
      return Promise.reject(e);
    }

    let data = `datainputs=${preparedData}`;

    return this.postCSRF(this.configService.generatePath, data)
      .then(response => {
        return response.data;
      });
  }
}
