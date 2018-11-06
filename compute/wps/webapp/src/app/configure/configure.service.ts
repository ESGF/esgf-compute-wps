import { Http, Headers, URLSearchParams, QueryEncoder } from '@angular/http';
import { Injectable } from '@angular/core';
import { Params } from '@angular/router';

import { Parameter } from './parameter';
import { RegridModel } from './regrid.component';
import { WPSService, WPSResponse } from '../core/wps.service';
import { ConfigService } from '../core/config.service';
import { AuthService } from '../core/auth.service';
import { Process } from './process';
import { File } from './file';
import { Variable } from './variable';
import { Dataset } from './dataset';
import { Domain } from './domain';

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
          return response.data.variables[name].map((index: number) => {
            return new Variable(name, response.data.files[index], index);
          });
        }).reduce((acc: any, cur: any) => {
          return acc.concat(cur);
        }, []);

        let variableNames = Object.keys(response.data.variables).sort();

        return new Dataset(dataset_id, variables, variableNames);
      });
  }

  searchVariable(variable: string, dataset_id: string, files: number[], params: any): Promise<Domain[]> {
    let newParams = {
      dataset_id: dataset_id,
      variable: variable,
      files: JSON.stringify(files),
      ...params
    };

    return this.get(this.configService.searchVariablePath, newParams)
      .then(response => {
        return response.data.map((item: any) => Domain.fromJSON(item));
      });
  }

  downloadScript(processes: Process[]): Promise<any> {
    let preparedData: string;
    
    try {
      preparedData = this.prepareDataInputsString(processes);
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
