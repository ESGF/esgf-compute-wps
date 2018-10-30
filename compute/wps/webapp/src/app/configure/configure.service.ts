import { Http, Headers, URLSearchParams, QueryEncoder } from '@angular/http';
import { Injectable } from '@angular/core';
import { Params } from '@angular/router';

import { Axis, AxisCollection } from './axis.component';
import { Parameter } from './parameter';
import { RegridModel } from './regrid.component';
import { WPSService, WPSResponse } from '../core/wps.service';
import { ConfigService } from '../core/config.service';
import { AuthService } from '../core/auth.service';
import { Process } from './process';
import { File } from './file';
import { Variable } from './variable';
import { Dataset } from './dataset';

export const LNG_NAMES: string[] = ['longitude', 'lon', 'x'];
export const LAT_NAMES: string[] = ['latitude', 'lat', 'y'];

@Injectable()
export class ConfigureService extends WPSService {
  constructor(
    http: Http,
    private configService: ConfigService,
  ) { 
    super(http); 
  }

  searchESGF(dataset_id: string, params: any): Promise<Dataset> { 
    let newParams = {dataset_id: dataset_id, ...params};

    return this.get(this.configService.searchPath, newParams)
      .then(response => {
        let variables = Object.keys(response.data.variables).map((name: string) => {
          let files = response.data.variables[name].map((index: number) => {
            return response.data.files[index]; 
          });

          return new Variable(name, files);
        }).sort((x: Variable, y: Variable) => {
          if (x.name < y.name) {
            return -1;
          } else if (x.name > y.name) {
            return 1;
          } else {
            return 0;
          }
        });

        return new Dataset(dataset_id, variables);
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
