import { Axis, AxisJSON } from './axis';

export interface DomainJSON {
  temporal: AxisJSON;
  spatial: AxisJSON[];
}

export class Domain {
  public temporal: Axis;
  public spatial: Axis[] = [];

  remove(axis: Axis) {
    if (this.temporal && this.temporal.uid === axis.uid) {
      this.temporal = null; 
    }

    this.spatial = this.spatial.filter((item: Axis) => {
      if (axis.uid === item.uid) {
        return false;
      }

      return true;
    });
  }

  static fromJSON(json: DomainJSON|string): Domain {
    if (typeof json === 'string') {
      return JSON.parse(json, Domain.reviver);
    } else {
      let domain = Object.create(Domain.prototype);

      return Object.assign(domain, json, {
        temporal: Axis.fromJSON(json.temporal),
        spatial: json.spatial.map((axis: AxisJSON) => Axis.fromJSON(axis)),
      });
    }
  }

  static reviver(key: string, value: any): any {
    return key === '' ? Domain.fromJSON(value) : value;
  }
}
