import { Axis, Domain } from './domain.component';
import { File } from './file';
import { Parameter } from './parameter.component';
import { RegridModel } from './regrid.component';
import { Input } from './input';
import { UID } from './uid';

export class Process extends UID implements Input {
  constructor(
    public identifier: string,
    public description: any = null,
    public inputs: Input[] = [],
    public variable: string = '',
    public domain: Domain = null,
    public regrid: RegridModel = null,
    public parameters: Parameter[] = [],
  ) { 
    super();
  }

  validate() {
    this.parameters.forEach((param: Parameter) => {
      param.validate();
    });

    if (this.regrid != null && this.regrid.regridType != 'None') {
      this.regrid.validate();
    }

    let metadata = this.description.metadata;

    if (metadata != undefined && metadata.inputs != Infinity) {
      if (metadata.inputs > 0 && metadata.inputs < this.inputs.length) {
        throw `Invalid number of inputs, ${this.inputs.length} are selected, expected ${metadata.inputs}`;
      }
    }
  }

  getVariable(): any {
    if (this.inputs == null) { return []; }

    return this.inputs.map((item: File) => {
      return {uri: item.url, id: `${this.variable}|${item.uid}`};
    });
  }

  getDomain(): any {
    if (this.domain == null) { return []; }

    let domain = {};

    this.domain.axes.map((item: Axis) => {
      domain[item.id] = {
        id: item.id,
        start: item.start,
        end: item.stop,
        step: item.step,
        crs: item.crs.toLowerCase(),
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

    if (this.inputs != null) {
      operation['input'] = this.inputs.map((item: File) => {
        return item.uid;
      });
    }

    if (this.domain != null) {
      operation['domain'] = this.domain.id;
    }

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
