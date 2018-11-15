import { Axis, AxisJSON } from './axis';
import { UID } from './uid';

export interface DomainJSON {
  temporal: AxisJSON;
  spatial: AxisJSON[];
}

export class Domain extends UID {
  public temporal: Axis;
  public spatial: Axis[] = [];

  constructor() {
    super();
  }

  clone() {
    let copy = new Domain();

    if (this.temporal != null) {
      let copyAxis = new Axis();

      copy.temporal = Object.assign(copyAxis, this.temporal);
    }

    copy.spatial = this.spatial.map((x: Axis) => {
      let copyAxis = new Axis();

      return Object.assign(copyAxis, x);
    });

    return copy;
  }

  toJSON() {
    let data = {
      id: this.uid,
    };

    if (this.temporal != null) {
      data[this.temporal.id] = this.temporal.toJSON();
    }

    for (let axis of this.spatial) {
      data[axis.id] = axis.toJSON();
    }

    return data;
  }

  isValid() {
    return this.temporal != null || this.spatial.length > 0;
  }

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
