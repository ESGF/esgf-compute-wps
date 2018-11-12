import { Parameter } from './parameter';
import { RegridModel } from './regrid.component';
import { Input } from './input';
import { UID } from './uid';
import { Variable } from './variable';
import { Domain } from './domain';

export class Process extends UID implements Input {
  public description: any;
  public inputs: (Variable|Process)[] = [];
  public domain: Domain = new Domain();
  public regrid: RegridModel = new RegridModel();
  public parameters: Parameter[] = [];

  constructor(
    public identifier: string,
  ) { 
    super();
  }

  display() {
    return this.identifier;
  }

  clearInputs() {
    this.inputs.splice(0, this.inputs.length);
  }

  addParameter(item: Parameter) {
    this.parameters.push(item);
  }

  removeParameter(item: Parameter) {
    this.parameters = this.parameters.filter((x: Parameter) => {
      if (item.uid === x.uid) {
        return false;
      }

      return true;
    });
  }

  removeInput(input: Variable|Process) {
    this.inputs = this.inputs.filter((item: Variable|Process) => {
      if (input.uid == item.uid) {
        return false;
      }

      return true;
    });
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

  toJSON() {
    let data = {
      name: this.identifier,
      input: this.inputs.map((item: Variable|Process) => { return item.uid; }),
      result: this.uid,
    };

    if (this.domain != null && this.domain.isValid()) {
      data['domain'] = this.domain.uid;
    }

    if (this.parameters.length > 0) {
      for (let key in this.parameters) {
        Object.assign(data, this.parameters[key].toJSON());
      }
    }

    return data;
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
