import { Axis, Domain } from './domain.component';
import { File } from './configure.service';
import { Parameter } from './parameter.component';
import { RegridModel } from './regrid.component';

export class Process {
  constructor(
    public identifier: string,
    public description: any = null,
    public inputs: File[] = null,
    public variable: string = '',
    public domain: Domain = null,
    public regrid: RegridModel = null,
    public parameters: Parameter[] = null,
    public uid: string = '',
  ) { 
    this.uid = this.newUID();
  }

  newUID(): string {
    return Math.random().toString(16).slice(2);
  }

  getVariable(): any {
    return this.inputs.map((item: File) => {
      return {uri: item.url, id: `${this.variable}|${item.uid}`};
    });
  }

  getDomain(): any {
    let domain = {};

    this.domain.axes.map((item: Axis) => {
      domain[item.id] = {
        id: item.id,
        start: item.start,
        end: item.stop,
        step: item.step,
        crs: item.crs,
      };
    });

    domain['id'] = this.domain.id = this.newUID();

    return [domain];
  }

  getOperation(): any {
    let operation = {
      name: this.identifier,
      result: this.newUID(),
    };

    operation['input'] = this.inputs.map((item: File) => {
      return item.uid;
    });

    operation['domain'] = this.domain.id;

    for (let x of this.parameters) {
      operation[x.key] = x.value;
    }

    if (this.regrid != null && this.regrid.regridType != 'None') {
      operation['gridder'] = this.buildRegrid(); 
    }

    return [operation];
  }

  buildRegrid() {
    let data = {
      tool: this.regrid.regridTool,
      method: this.regrid.regridMethod,
    };

    if (this.regrid.regridType === 'Gaussian') {
      data['grid'] = `gaussian~${this.regrid.nLats}`;
    } else if (this.regrid.regridType === 'Uniform') {
      let lats = '';
      let lons = '';

      if (this.regrid.startLats != null && this.regrid.deltaLats != null) {
        lats = `${this.regrid.startLats}:${this.regrid.nLats}:${this.regrid.deltaLats}` 
      } else {
        lats = `${this.regrid.deltaLats}`
      }

      if (this.regrid.startLons != null && this.regrid.deltaLons != null) {
        lons = `${this.regrid.startLons}:${this.regrid.nLons}:${this.regrid.deltaLons}`
      } else {
        lons = `${this.regrid.deltaLons}`
      }

      data['grid'] = `uniform~${lats}x${lons}`;
    }

    return data;
  }
}
